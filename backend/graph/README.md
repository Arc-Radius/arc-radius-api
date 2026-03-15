# Graph DB: Local Build + Eval Loop

This is the shortest path to build the local graph DB once and run the eval loop end-to-end.

## Prereqs

- Python/Poetry env ready in `backend`
- Docker running locally
- AWS credentials configured locally (for Bedrock embedding/inference), e.g. `AWS_PROFILE` or standard `~/.aws/credentials`
- Data files present under:
  - `datasources/final-outputs/matched_lgbtq_bills.csv`
  - `datasources/legiscan-bulk-csv/`
    - the folder for each state year should contain the following files:
      - `bills.csv`
      - `people.csv`
      - `sponsors.csv`
      - `history.csv`
      - `documents.csv`
      - `rollcalls.csv`
  - `datasources/legiscan-bill-text/bill-text/`
    - Contains the pdf and html file for each bill

## 1) Create `.env` (required)

From `backend/.env.example`, create `backend/.env` and set:

```env
NEO4J_PASSWORD=
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j

BEDROCK_EMBED_MODEL_ID=amazon.titan-embed-text-v2:0
BEDROCK_EMBED_DIMS=1024
BEDROCK_EMBED_NORMALIZE=true
RAG_RETRIEVER_MODE=vector
```

Notes:
- `NEO4J_PASSWORD` must match what Docker Neo4j boots with.

## 2) Install deps

From `backend/`:

```bash
poetry install
```

## 3) Build graph DB (single local build)

From `backend/graph/`:

```bash
make setup
make pipeline
```
What each command does:
- `make setup` -> starts Neo4j + waits + applies schema
- `make pipeline` -> metadata ingest -> text extract/chunk -> embeddings
  - This is time-intensive and can take a couple hours to complete to run extraction and embedding steps.

## Stepwise command if you don't want to run the full pipeline

- `make ingest`: can be several minutes (large CSV scan + graph writes)
- `make extract`: usually the slowest CPU/IO step (document extraction/chunking)
- `make embed`: often the slowest wall-clock step (many Bedrock API calls)

If you want a quick smoke run first:

```bash
make extract MAX_DOCS=10
make embed
```

(Then run full extract later.)

## 4) Start API (required for eval loop)

From `backend/`:

```bash
poetry run uvicorn src.main:app --host 0.0.0.0 --port 8000
```

Keep this running in a terminal.

## 5) Run eval loop

In another terminal, from `backend/`:

```bash
poetry run python graph/scripts/7_eval_loop.py
```

Optional small eval:

```bash
poetry run python graph/scripts/7_eval_loop.py --input-csv graph/two_bills.csv --max-bills 2
```

Outputs are written to:
- `backend/graph/output/eval_bills_generate_outputs.csv`
- `backend/graph/output/eval_bills_generate_outputs.json`

## Other useful ops

From `backend/graph/`:

```bash
make query-example   # quick retrieval sanity check
make down            # stop Neo4j
make down-v          # stop + wipe Neo4j volumes
make reset           # rebuild Neo4j from empty + schema
```

## Helpful tests to see extraction and chunking steps

From `backend/graph/`:

```bash
poetry run python tests/test_extract.py
poetry run python tests/test_chunk.py
```