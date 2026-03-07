"""
Arc Radius — Embed + Neo4j Lambda
===================================
Triggered by S3 event when classify Lambda drops a CSV
into processed/to-embed/.

Flow:
  1. Read matched bills CSV from processed/to-embed/
  2. Fetch bill text from LegiScan (getBillText API)
  3. Chunk the text (~500 chars per chunk)
  4. Embed each chunk via Bedrock Titan Embeddings
  5. Write Bill node + Chunk nodes to Neo4j
  6. Move CSV to processed/to-embed/done/

Prerequisites:
  - Neo4j AuraDB instance running
  - Bedrock Titan Embeddings model access enabled
  - LegiScan API key for getBillText

Deploy as a Lambda with:
  - Runtime: Python 3.14
  - Timeout: 15 minutes
  - Memory: 1024 MB
  - Layers: requests (for LegiScan API)
  - Environment variables:
      BUCKET = arc-radius-s3-bucket
      LEGISCAN_API_KEY = your key
      NEO4J_URI = neo4j+s://xxxx.databases.neo4j.io
      NEO4J_USER = neo4j
      NEO4J_PASSWORD = your password
      BEDROCK_MODEL_ID = amazon.titan-embed-text-v1
      USE_BEDROCK = false  (set true when ready)
"""

import json
import os
import csv
import time
import base64
import re
import sys
from pathlib import Path
from datetime import datetime, timezone
from io import StringIO

import boto3
import requests

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from graph.src.chunking import make_chunks
from unstructured.partition.text import partition_text

# ─── Config ────────────────────────────────────────────────
BUCKET = os.environ.get("BUCKET", "arc-radius-s3-bucket")
REGION = os.environ.get("AWS_REGION", "us-east-1")
LEGISCAN_API_KEY = os.environ.get("LEGISCAN_API_KEY", "")
NEO4J_URI = os.environ.get("NEO4J_URI", "")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "")
BEDROCK_MODEL_ID = os.environ.get("BEDROCK_MODEL_ID", "amazon.titan-embed-text-v1")
USE_BEDROCK = os.environ.get("USE_BEDROCK", "false").lower() == "true"
USE_NEO4J = os.environ.get("USE_NEO4J", "false").lower() == "true"

API_URL = "https://api.legiscan.com/"
TO_EMBED_PREFIX = "processed/to-embed/"
DONE_PREFIX = "processed/to-embed/done/"
CHUNK_SIZE = 500  # max chars per chunk
CHUNK_OVERLAP = 50  # overlap between chunks

s3 = boto3.client("s3", region_name=REGION)
bedrock = boto3.client("bedrock-runtime", region_name=REGION) if USE_BEDROCK else None


# ─── LegiScan API ─────────────────────────────────────────

def api_call(op, **params):
    params["key"] = LEGISCAN_API_KEY
    params["op"] = op
    resp = requests.get(API_URL, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if data.get("status") != "OK":
        raise RuntimeError(
            f"LegiScan {op}: {data.get('alert', {}).get('message', 'error')}")
    time.sleep(0.4)
    return data


def fetch_bill_text(doc_id):
    """
    Call getBillText → returns base64 encoded document.
    Decode and extract plain text.
    """
    if not doc_id or str(doc_id) == "0":
        return None

    data = api_call("getBillText", id=int(doc_id))
    text_info = data.get("text", {})

    encoded = text_info.get("doc", "")
    if not encoded:
        return None

    mime = text_info.get("mime", "")
    raw_bytes = base64.b64decode(encoded)

    if "html" in mime:
        # Strip HTML tags to get plain text
        text = raw_bytes.decode("utf-8", errors="ignore")
        text = strip_html(text)
        return text
    elif "pdf" in mime:
        # For PDFs, we'd need a PDF parser (not in Lambda by default)
        # Store raw and parse later, or use a basic extraction
        # For now, return None and fall back to title + description
        print(f"    PDF document (doc_id={doc_id}), skipping text extraction")
        return None
    else:
        # Try decoding as text
        try:
            return raw_bytes.decode("utf-8", errors="ignore")
        except Exception:
            return None


def strip_html(html):
    """Remove HTML tags and clean up whitespace."""
    text = re.sub(r'<style[^>]*>.*?</style>', '', html, flags=re.DOTALL)
    text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'&nbsp;', ' ', text)
    text = re.sub(r'&amp;', '&', text)
    text = re.sub(r'&lt;', '<', text)
    text = re.sub(r'&gt;', '>', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def normalize_state_code(state):
    """Normalize to uppercase two-letter state code when possible."""
    letters = re.sub(r"[^A-Za-z]", "", state or "").upper()
    return letters[:2] if len(letters) >= 2 else ""


def normalize_session_id(session_id):
    """Normalize session id as a stripped string."""
    return str(session_id or "").strip()


# ─── Chunking ─────────────────────────────────────────────

def chunk_text(text, chunk_size=CHUNK_SIZE, overlap=CHUNK_OVERLAP):
    """
    Chunk with graph/src/chunking.py and return
    [{"text": ..., "index": ...}, ...]
    """
    if not text or len(text) < 50:
        return []

    elements = partition_text(text=text)
    graph_chunks = make_chunks(
        elements,
        max_characters=chunk_size,
        new_after_n_chars=max(int(chunk_size * 0.85), 1),
        overlap=overlap,
        combine_under=max(int(chunk_size * 0.5), 1),
    )

    return [
        {
            "text": ch.get("text", "").strip(),
            "index": idx,
        }
        for idx, ch in enumerate(graph_chunks)
        if ch.get("text", "").strip()
    ]


# ─── Bedrock Embeddings ───────────────────────────────────

def embed_text(text):
    """
    Call Bedrock Titan Embeddings for a single text.
    Returns embedding vector (list of floats).
    """
    if USE_BEDROCK:
        try:
            response = bedrock.invoke_model(
                modelId=BEDROCK_MODEL_ID,
                contentType="application/json",
                accept="application/json",
                body=json.dumps({"inputText": text[:8000]}),
            )
            result = json.loads(response["Body"].read())
            return result.get("embedding", [])
        except Exception as e:
            print(f"    Bedrock error: {e}")
            return []
    else:
        # Mock: return empty vector (Neo4j will store without embedding)
        return []


def embed_chunks(chunks):
    """Embed all chunks. Returns chunks with 'embedding' field added."""
    for i, chunk in enumerate(chunks):
        chunk["embedding"] = embed_text(chunk["text"])
        if (i + 1) % 10 == 0:
            print(f"      Embedded {i+1}/{len(chunks)} chunks")
    return chunks


# ─── Neo4j ────────────────────────────────────────────────

def get_neo4j_driver():
    """Create Neo4j driver. Returns None if not configured."""
    if not USE_NEO4J or not NEO4J_URI:
        return None
    try:
        from neo4j import GraphDatabase
        return GraphDatabase.driver(
            NEO4J_URI,
            auth=(NEO4J_USER, NEO4J_PASSWORD),
        )
    except ImportError:
        print("  neo4j package not installed, skipping Neo4j write")
        return None
    except Exception as e:
        print(f"  Neo4j connection error: {e}")
        return None


def write_bill_to_neo4j(driver, bill, chunks):
    """
    Write a Bill node and its Chunk nodes to Neo4j.

    Graph structure:
      (Bill {bill_id, state, bill_number, title, label, ...})
        -[:HAS_CHUNK]->
      (Chunk {bill_id, index, text, embedding})
    """
    with driver.session() as session:
        state_code = normalize_state_code(bill.get("state", ""))
        session_id = normalize_session_id(bill.get("session_id", ""))
        session_pk = f"{state_code}:{session_id}" if state_code and session_id else ""
        # Create/update Bill node
        session.run("""
            MERGE (b:Bill {bill_id: $bill_id})
            SET b.state = $state,
                b.session_id = $session_id,
                b.bill_number = $bill_number,
                b.title = $title,
                b.description = $description,
                b.status = $status,
                b.status_date = $status_date,
                b.label = $label,
                b.label_source = $label_source,
                b.issues = $issues,
                b.url = $url,
                b.sponsor_names = $sponsor_names,
                b.primary_sponsor = $primary_sponsor,
                b.updated_at = datetime()
        """, {
            "bill_id": str(bill.get("bill_id", "")),
            "state": bill.get("state", ""),
            "session_id": bill.get("session_id", ""),
            "bill_number": bill.get("bill_number", ""),
            "title": bill.get("title", ""),
            "description": bill.get("description", ""),
            "status": bill.get("status", ""),
            "status_date": bill.get("status_date", ""),
            "label": bill.get("label", ""),
            "label_source": bill.get("label_source", ""),
            "issues": bill.get("issues", ""),
            "url": bill.get("url", ""),
            "sponsor_names": bill.get("sponsor_names", ""),
            "primary_sponsor": bill.get("primary_sponsor", ""),
        })

        # Keep State node + Bill state relationship aligned with bill.state
        if state_code:
            session.run("""
                MERGE (s:State {code: $state_code})
                WITH s
                MATCH (b:Bill {bill_id: $bill_id})
                MERGE (b)-[:IN_STATE]->(s)
            """, {
                "state_code": state_code,
                "bill_id": str(bill.get("bill_id", "")),
            })

        # Connect bill/state to session when session_id is available
        if session_pk:
            session.run("""
                MERGE (sn:Session {session_pk: $session_pk})
                SET sn.session_id = $session_id,
                    sn.state_code = $state_code
                WITH sn
                MATCH (b:Bill {bill_id: $bill_id})
                MERGE (b)-[:IN_SESSION]->(sn)
                WITH sn
                MATCH (s:State {code: $state_code})
                MERGE (s)-[:HAS_SESSION]->(sn)
            """, {
                "session_pk": session_pk,
                "session_id": session_id,
                "state_code": state_code,
                "bill_id": str(bill.get("bill_id", "")),
            })

        # Delete old chunks (in case bill was updated)
        session.run("""
            MATCH (b:Bill {bill_id: $bill_id})-[r:HAS_CHUNK]->(c:Chunk)
            DELETE r, c
        """, {"bill_id": str(bill.get("bill_id", ""))})

        # Create new chunks
        for chunk in chunks:
            params = {
                "bill_id": str(bill.get("bill_id", "")),
                "index": chunk["index"],
                "text": chunk["text"],
            }

            if chunk.get("embedding"):
                # With embedding vector
                session.run("""
                    MATCH (b:Bill {bill_id: $bill_id})
                    CREATE (c:Chunk {
                        bill_id: $bill_id,
                        chunk_index: $index,
                        text: $text,
                        embedding: $embedding
                    })
                    CREATE (b)-[:HAS_CHUNK]->(c)
                """, {**params, "embedding": chunk["embedding"]})
            else:
                # Without embedding (will add later)
                session.run("""
                    MATCH (b:Bill {bill_id: $bill_id})
                    CREATE (c:Chunk {
                        bill_id: $bill_id,
                        chunk_index: $index,
                        text: $text
                    })
                    CREATE (b)-[:HAS_CHUNK]->(c)
                """, params)


def write_bill_mock(bill, chunks):
    """Print what would be written to Neo4j."""
    print(f"    [MOCK NEO4J] Bill: {bill.get('state')} {bill.get('bill_number')} "
          f"— {bill.get('label')} — {len(chunks)} chunks")
    if chunks:
        print(f"      Chunk 0: {chunks[0]['text'][:80]}...")


# ─── S3 helpers ───────────────────────────────────────────

def list_to_embed_csvs():
    """Find CSVs in processed/to-embed/ (not in done/)."""
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=TO_EMBED_PREFIX)
    keys = []
    for obj in response.get("Contents", []):
        key = obj["Key"]
        if key.endswith(".csv") and "/done/" not in key:
            keys.append(key)
    return keys


def read_csv_from_s3(key):
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    text = obj["Body"].read().decode("utf-8")
    return list(csv.DictReader(StringIO(text)))


def move_to_done(key):
    """Move to done/ subfolder."""
    filename = key.split("/")[-1]
    new_key = f"{DONE_PREFIX}{filename}"
    s3.copy_object(
        Bucket=BUCKET,
        CopySource={"Bucket": BUCKET, "Key": key},
        Key=new_key,
    )
    s3.delete_object(Bucket=BUCKET, Key=key)


# ─── Lambda handler ───────────────────────────────────────

def lambda_handler(event, context):
    start = time.time()
    print(f"Embed+Neo4j started at {datetime.now(timezone.utc).isoformat()}")
    print(f"Bedrock: {'ON' if USE_BEDROCK else 'OFF (mock)'}  "
          f"Neo4j: {'ON' if USE_NEO4J else 'OFF (mock)'}")

    # 1. Find CSVs to process
    csv_keys = list_to_embed_csvs()
    if not csv_keys:
        print("No CSVs in to-embed/. Done.")
        return {"statusCode": 200, "processed": 0}

    print(f"Found {len(csv_keys)} CSV(s)")

    # 2. Read all matched bills
    all_bills = []
    for key in csv_keys:
        bills = read_csv_from_s3(key)
        all_bills.extend(bills)
        print(f"  {key}: {len(bills)} bills")

    # 3. Connect to Neo4j
    driver = get_neo4j_driver()

    # 4. Process each bill: fetch text → chunk → embed → write
    processed = 0
    text_fetched = 0
    total_chunks = 0
    errors = 0

    for i, bill in enumerate(all_bills):
        bid = bill.get("bill_id", "")
        doc_id = bill.get("document_id", "0")

        print(f"\n  [{i+1}/{len(all_bills)}] {bill.get('state')} "
              f"{bill.get('bill_number')} (bill_id={bid})")

        # Fetch bill text
        bill_text = None
        if LEGISCAN_API_KEY and doc_id and str(doc_id) != "0":
            try:
                bill_text = fetch_bill_text(doc_id)
                if bill_text:
                    text_fetched += 1
                    print(f"    Text: {len(bill_text)} chars from doc_id={doc_id}")
            except Exception as e:
                print(f"    Text fetch error: {e}")

        # Fallback: use title + description
        if not bill_text:
            bill_text = f"{bill.get('title', '')}. {bill.get('description', '')}"
            print(f"    Using title+description as fallback ({len(bill_text)} chars)")

        # Chunk
        chunks = chunk_text(bill_text)
        total_chunks += len(chunks)
        print(f"    Chunks: {len(chunks)}")

        # Embed
        if chunks:
            chunks = embed_chunks(chunks)

        # Write to Neo4j
        try:
            if driver:
                write_bill_to_neo4j(driver, bill, chunks)
                print(f"    Written to Neo4j")
            else:
                write_bill_mock(bill, chunks)
            processed += 1
        except Exception as e:
            print(f"    Neo4j error: {e}")
            errors += 1

    # 5. Close Neo4j
    if driver:
        driver.close()

    # 6. Move CSVs to done
    for key in csv_keys:
        move_to_done(key)
        print(f"  Moved to done: {key}")

    duration = round(time.time() - start, 1)
    print(f"\nDone in {duration}s")
    print(f"  Processed: {processed}/{len(all_bills)}")
    print(f"  Bill text fetched: {text_fetched}")
    print(f"  Total chunks: {total_chunks}")
    print(f"  Errors: {errors}")

    return {
        "statusCode": 200,
        "processed": processed,
        "text_fetched": text_fetched,
        "total_chunks": total_chunks,
        "errors": errors,
        "duration_seconds": duration,
    }


# ─── Local testing ────────────────────────────────────────

if __name__ == "__main__":
    result = lambda_handler({}, None)
    print(json.dumps(result, indent=2))
