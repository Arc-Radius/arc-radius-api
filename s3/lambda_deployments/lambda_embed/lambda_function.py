"""
Arc Radius — Embed + Neo4j Lambda
===================================
Triggered by S3 event when classify Lambda drops a CSV
into processed/classified/.

Flow:
  1. Load embedded_bills.json tracker
  2. Read matched bills CSV from processed/classified/
  3. Skip bills already embedded (hash match)
  4. For each new/changed bill:
     a. Create/update Bill node (with bill_pk = state:session_id:bill_id)
     b. Create State, Session, Topic nodes + relationships
     c. Create Document node + HAS_DOCUMENT relationship
     d. Fetch bill text from LegiScan (getBillText API)
     e. Chunk the text (sentence-boundary splitting)
     f. Embed each chunk via Bedrock Titan Embeddings v2 (1024-dim)
     g. Create Chunk nodes with embeddings + HAS_CHUNK relationships
  5. Update embedded_bills.json
  6. Move CSV to processed/embedded/

Graph schema (matches teammate's 1_ingest_metadata.py):
  (:Bill {bill_pk})-[:IN_STATE]->(:State {code})
  (:Bill)-[:IN_SESSION]->(:Session {session_pk})
  (:State)-[:HAS_SESSION]->(:Session)
  (:Bill)-[:HAS_TOPIC]->(:Topic {name})
  (:Bill)-[:HAS_DOCUMENT]->(:Document {document_id})
  (:Document)-[:HAS_CHUNK]->(:Chunk {chunk_id, embedding})

Deploy as a Lambda with:
  - Runtime: Python 3.12
  - Timeout: 15 minutes
  - Memory: 1024 MB
  - Layers: requests, neo4j
  - Environment variables:
      BUCKET = arc-radius-s3-bucket
      LEGISCAN_API_KEY = your key
      NEO4J_URI = neo4j+s://dc37b1dc.databases.neo4j.io
      NEO4J_USER = neo4j
      NEO4J_PASSWORD = your password
      BEDROCK_MODEL_ID = amazon.titan-embed-text-v2:0
"""

import ast
import json
import os
import csv
import time
import base64
import re
from datetime import datetime, timezone
from io import StringIO

import boto3
import requests

# ─── Config ────────────────────────────────────────────────
BUCKET = os.environ.get("BUCKET", "arc-radius-s3-bucket")
REGION = os.environ.get("AWS_REGION", "us-east-1")
LEGISCAN_API_KEY = os.environ.get("LEGISCAN_API_KEY", "")
NEO4J_URI = os.environ.get("NEO4J_URI", "")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "")
BEDROCK_MODEL_ID = os.environ.get(
    "BEDROCK_MODEL_ID", "amazon.titan-embed-text-v2:0")

API_URL = "https://api.legiscan.com/"
CLASSIFIED_PREFIX = "processed/classified/"
EMBEDDED_PREFIX = "processed/embedded/"
EMBEDDED_TRACKER_KEY = "pipeline/metadata/embedded_bills.json"
CHUNK_SIZE = 500
CHUNK_OVERLAP = 50

s3 = boto3.client("s3", region_name=REGION)
bedrock = boto3.client("bedrock-runtime", region_name=REGION)


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
        text = raw_bytes.decode("utf-8", errors="ignore")
        text = strip_html(text)
        return text
    elif "pdf" in mime:
        print(f"    PDF document (doc_id={doc_id}), skipping text extraction")
        return None
    else:
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


# ─── Helpers ──────────────────────────────────────────────

def normalize_state_code(state):
    letters = re.sub(r"[^A-Za-z]", "", state or "").upper()
    return letters[:2] if len(letters) >= 2 else ""


def make_bill_pk(state, session_id, bill_id):
    """Composite primary key matching teammate's schema: state:session_id:bill_id"""
    code = normalize_state_code(state)
    sid = str(session_id or "").strip()
    bid = str(bill_id or "").strip()
    return f"{code}:{sid}:{bid}" if code and sid and bid else ""


# ─── Chunking ─────────────────────────────────────────────

def chunk_text(text, chunk_size=CHUNK_SIZE, overlap=CHUNK_OVERLAP):
    """Chunk text using sentence-boundary splitting."""
    if not text or len(text) < 50:
        return []

    chunks = []
    start = 0
    index = 0
    while start < len(text):
        end = start + chunk_size
        if end < len(text):
            for sep in [". ", ".\n", ";\n", "\n\n"]:
                last_sep = text[start:end].rfind(sep)
                if last_sep > chunk_size * 0.5:
                    end = start + last_sep + len(sep)
                    break
        chunk = text[start:end].strip()
        if chunk:
            chunks.append({"text": chunk, "heading": "", "index": index})
            index += 1
        start = end - overlap
    return chunks


# ─── Bedrock Embeddings ───────────────────────────────────

def embed_text(text):
    """Call Bedrock Titan Embeddings v2. Returns 1024-dim vector."""
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


def embed_chunks(chunks):
    """Embed all chunks. Returns chunks with 'embedding' field added."""
    for i, chunk in enumerate(chunks):
        chunk["embedding"] = embed_text(chunk["text"])
        if (i + 1) % 10 == 0:
            print(f"      Embedded {i+1}/{len(chunks)} chunks")
    return chunks


# ─── Neo4j ────────────────────────────────────────────────

def get_neo4j_driver():
    if not NEO4J_URI:
        print("  ERROR: NEO4J_URI not set")
        return None
    try:
        from neo4j import GraphDatabase
        return GraphDatabase.driver(
            NEO4J_URI,
            auth=(NEO4J_USER, NEO4J_PASSWORD),
        )
    except ImportError:
        print("  ERROR: neo4j package not installed")
        return None
    except Exception as e:
        print(f"  Neo4j connection error: {e}")
        return None


def write_bill_to_neo4j(driver, bill, doc_id, chunks):
    """
    Write Bill, State, Session, Topic, Document, Chunk nodes to Neo4j.
    Matches teammate's schema from 1_ingest_metadata.py.
    """
    state_code = normalize_state_code(bill.get("state", ""))
    session_id = str(bill.get("session_id", "")).strip()
    bill_id = str(bill.get("bill_id", "")).strip()
    bill_pk = make_bill_pk(bill.get("state", ""), session_id, bill_id)
    doc_id_str = str(doc_id or "")

    if not bill_pk:
        print(f"    Cannot build bill_pk, skipping")
        return

    with driver.session() as session:
        # Create/update Bill node
        session.run("""
            MERGE (b:Bill {bill_pk: $bill_pk})
            SET b.state          = $state,
                b.session_id     = $session_id,
                b.bill_id        = $bill_id,
                b.bill_number    = $bill_number,
                b.status         = $status,
                b.status_desc    = $status_desc,
                b.status_date    = $status_date,
                b.title          = $title,
                b.description    = $description,
                b.last_action_date = $last_action_date,
                b.last_action    = $last_action,
                b.url            = $url,
                b.state_link     = $state_link,
                b.label          = $label,
                b.label_source   = $label_source,
                b.confidence     = $confidence,
                b.relevance_score = $relevance_score,
                b.issues         = $issues,
                b.issue_categories = $issue_categories,
                b.sponsor_names  = $sponsor_names,
                b.primary_sponsor = $primary_sponsor,
                b.year           = $year,
                b.session_year   = $session_year,
                b.updated_at     = datetime()
        """, {
            "bill_pk": bill_pk,
            "state": bill.get("state", ""),
            "session_id": session_id,
            "bill_id": bill_id,
            "bill_number": bill.get("bill_number", ""),
            "status": bill.get("status", ""),
            "status_desc": bill.get("status_desc", ""),
            "status_date": bill.get("status_date", ""),
            "title": bill.get("title", ""),
            "description": bill.get("description", ""),
            "last_action_date": bill.get("last_action_date", ""),
            "last_action": bill.get("last_action", ""),
            "url": bill.get("url", ""),
            "state_link": bill.get("state_link", ""),
            "label": bill.get("label", ""),
            "label_source": bill.get("label_source", ""),
            "confidence": bill.get("confidence", ""),
            "relevance_score": bill.get("relevance_score", ""),
            "issues": bill.get("issues", ""),
            "issue_categories": bill.get("issue_categories", ""),
            "sponsor_names": bill.get("sponsor_names", ""),
            "primary_sponsor": bill.get("primary_sponsor", ""),
            "year": bill.get("year", ""),
            "session_year": bill.get("session_year", ""),
        })

        # State node + IN_STATE relationship
        if state_code:
            session.run("""
                MERGE (s:State {code: $state_code})
                WITH s
                MATCH (b:Bill {bill_pk: $bill_pk})
                MERGE (b)-[:IN_STATE]->(s)
            """, {"state_code": state_code, "bill_pk": bill_pk})

        # Session node + relationships
        session_pk = f"{state_code}:{session_id}" if state_code and session_id else ""
        if session_pk:
            session.run("""
                MERGE (sn:Session {session_pk: $session_pk})
                SET sn.session_id = $session_id,
                    sn.state_code = $state_code
                WITH sn
                MATCH (b:Bill {bill_pk: $bill_pk})
                MERGE (b)-[:IN_SESSION]->(sn)
                WITH sn
                MATCH (s:State {code: $state_code})
                MERGE (s)-[:HAS_SESSION]->(sn)
            """, {
                "session_pk": session_pk,
                "session_id": session_id,
                "state_code": state_code,
                "bill_pk": bill_pk,
            })

        # Topic nodes + HAS_TOPIC relationships (from Lambda 2's issue_categories)
        issue_categories_str = bill.get("issue_categories", "")
        if issue_categories_str:
            try:
                categories = ast.literal_eval(issue_categories_str)
            except (ValueError, SyntaxError):
                categories = []
            for topic_name in categories:
                if topic_name and topic_name != "other":
                    session.run("""
                        MERGE (t:Topic {name: $topic_name})
                        WITH t
                        MATCH (b:Bill {bill_pk: $bill_pk})
                        MERGE (b)-[:HAS_TOPIC]->(t)
                    """, {"topic_name": topic_name, "bill_pk": bill_pk})

        # Document node + HAS_DOCUMENT relationship
        if doc_id_str and doc_id_str != "0":
            session.run("""
                MERGE (d:Document {document_id: $document_id})
                SET d.document_type = $document_type,
                    d.url = $document_url
                WITH d
                MATCH (b:Bill {bill_pk: $bill_pk})
                MERGE (b)-[:HAS_DOCUMENT]->(d)
            """, {
                "document_id": doc_id_str,
                "document_type": bill.get("document_type", ""),
                "document_url": bill.get("document_url", ""),
                "bill_pk": bill_pk,
            })

            # Delete old chunks for this document (in case bill was updated)
            session.run("""
                MATCH (d:Document {document_id: $document_id})-[r:HAS_CHUNK]->(c:Chunk)
                DELETE r, c
            """, {"document_id": doc_id_str})

            # Create new chunks: (:Document)-[:HAS_CHUNK]->(:Chunk)
            for chunk in chunks:
                chunk_id = f"{doc_id_str}:{chunk['index']}"
                params = {
                    "document_id": doc_id_str,
                    "chunk_id": chunk_id,
                    "chunk_index": chunk["index"],
                    "section_path": chunk.get("heading", ""),
                    "text": chunk["text"],
                }

                if chunk.get("embedding"):
                    session.run("""
                        MATCH (d:Document {document_id: $document_id})
                        MERGE (c:Chunk {chunk_id: $chunk_id})
                        SET c.chunk_index = $chunk_index,
                            c.section_path = $section_path,
                            c.text = $text,
                            c.embedding = $embedding
                        MERGE (d)-[:HAS_CHUNK]->(c)
                    """, {**params, "embedding": chunk["embedding"]})
                else:
                    session.run("""
                        MATCH (d:Document {document_id: $document_id})
                        MERGE (c:Chunk {chunk_id: $chunk_id})
                        SET c.chunk_index = $chunk_index,
                            c.section_path = $section_path,
                            c.text = $text
                        MERGE (d)-[:HAS_CHUNK]->(c)
                    """, params)

            # Update document metadata
            session.run("""
                MATCH (d:Document {document_id: $document_id})
                SET d.extracted_at = datetime(),
                    d.extracted_len = $text_len
            """, {
                "document_id": doc_id_str,
                "text_len": sum(len(c["text"]) for c in chunks),
            })


# ─── S3 helpers ───────────────────────────────────────────

def load_json_from_s3(key):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception:
        return None


def save_json_to_s3(key, data):
    s3.put_object(
        Bucket=BUCKET, Key=key,
        Body=json.dumps(data),
        ContentType="application/json",
    )


def list_classified_csvs():
    """Find CSVs in processed/classified/."""
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=CLASSIFIED_PREFIX)
    keys = []
    for obj in response.get("Contents", []):
        key = obj["Key"]
        if key.endswith(".csv"):
            keys.append(key)
    return keys


def read_csv_from_s3(key):
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    text = obj["Body"].read().decode("utf-8")
    return list(csv.DictReader(StringIO(text)))


def move_to_embedded(key):
    """Move to processed/embedded/ (different prefix, no re-trigger)."""
    filename = key.split("/")[-1]
    new_key = f"{EMBEDDED_PREFIX}{filename}"
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
    print(f"Bedrock model: {BEDROCK_MODEL_ID}")
    print(f"Neo4j: {NEO4J_URI[:30]}..." if NEO4J_URI else "Neo4j: NOT SET")

    # 1. Load embedded tracker
    embedded = load_json_from_s3(EMBEDDED_TRACKER_KEY) or {}
    print(f"Previously embedded: {len(embedded)} bills")

    # 2. Find CSVs to process
    csv_keys = list_classified_csvs()
    if not csv_keys:
        print("No CSVs in processed/classified/. Done.")
        return {"statusCode": 200, "processed": 0}

    print(f"Found {len(csv_keys)} CSV(s)")

    # 3. Read all matched bills
    all_bills = []
    for key in csv_keys:
        bills = read_csv_from_s3(key)
        all_bills.extend(bills)
        print(f"  {key}: {len(bills)} bills")

    # 4. Filter to bills that need embedding
    to_process = []
    skipped = 0
    for bill in all_bills:
        bid = str(bill.get("bill_id", ""))
        current_hash = bill.get("change_hash", "")

        if bid in embedded and embedded[bid] is not None and embedded[bid] == current_hash:
            skipped += 1
            continue

        to_process.append(bill)

    print(f"To embed: {len(to_process)} (skipped {skipped} already done)")

    if not to_process:
        for key in csv_keys:
            move_to_embedded(key)
        print("All bills already embedded. CSVs moved. Done.")
        return {"statusCode": 200, "processed": 0}

    # 5. Connect to Neo4j
    driver = get_neo4j_driver()
    if not driver:
        print("ERROR: Could not connect to Neo4j. Aborting.")
        return {"statusCode": 500, "error": "Neo4j connection failed"}

    # 6. Process each bill: fetch text → chunk → embed → write
    processed = 0
    text_fetched = 0
    total_chunks = 0
    errors = 0

    for i, bill in enumerate(to_process):
        bid = str(bill.get("bill_id", ""))
        doc_id = bill.get("document_id", "0")

        print(f"\n  [{i+1}/{len(to_process)}] {bill.get('state')} "
              f"{bill.get('bill_number')} (bill_id={bid})")

        # Fetch bill text
        bill_text = None
        if LEGISCAN_API_KEY and doc_id and str(doc_id) != "0":
            try:
                bill_text = fetch_bill_text(doc_id)
                if bill_text:
                    text_fetched += 1
                    print(
                        f"    Text: {len(bill_text)} chars from doc_id={doc_id}")
            except Exception as e:
                print(f"    Text fetch error: {e}")

        # Fallback: use title + description
        if not bill_text:
            bill_text = f"{bill.get('title', '')}. {bill.get('description', '')}"
            print(
                f"    Using title+description as fallback ({len(bill_text)} chars)")

        # Chunk
        chunks = chunk_text(bill_text)
        total_chunks += len(chunks)
        print(f"    Chunks: {len(chunks)}")

        # Embed
        if chunks:
            chunks = embed_chunks(chunks)

        # Write to Neo4j
        try:
            write_bill_to_neo4j(driver, bill, doc_id, chunks)
            print(f"    Written to Neo4j")
            processed += 1

            # Update tracker only after successful write
            embedded[bid] = bill.get("change_hash", "")

        except Exception as e:
            print(f"    Neo4j error: {e}")
            errors += 1

    # 7. Close Neo4j
    driver.close()

    # 8. Save embedded tracker
    save_json_to_s3(EMBEDDED_TRACKER_KEY, embedded)
    print(f"\n  embedded_bills.json: {len(embedded)} total")

    # 9. Move CSVs to archive
    for key in csv_keys:
        move_to_embedded(key)
        print(f"  Moved: {key} → {EMBEDDED_PREFIX}")

    duration = round(time.time() - start, 1)
    print(f"\nDone in {duration}s")
    print(f"  Processed: {processed}/{len(to_process)}")
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
    from dotenv import load_dotenv
    load_dotenv()

    LEGISCAN_API_KEY = os.environ.get("LEGISCAN_API_KEY", "")
    NEO4J_URI = os.environ.get("NEO4J_URI", "")
    NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
    NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "")

    result = lambda_handler({}, None)
    print(json.dumps(result, indent=2))
