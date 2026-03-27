# Graph DB: Local Build + Eval Loop

Build the local Neo4j knowledge graph and run the RAG eval loop end-to-end.

## Prerequisites

- Docker running locally
- Python 3.11+ installed
- AWS credentials configured (for Bedrock embedding/inference) via an AWS profile (e.g. `~/.aws/credentials`)
- Data files present at the following paths relative to the repo root:
  - `datasources/final-outputs/matched_lgbtq_bills.csv`
  - `datasources/legiscan-bulk-csv/` — one folder per state-year, each containing:
    - `bills.csv`, `people.csv`, `sponsors.csv`, `history.csv`, `documents.csv`, `rollcalls.csv`
  - `datasources/legiscan-bill-text/bill-text/` — PDF and HTML files for each bill

## 1) Create `backend/.env`

Copy `backend/.env.example` to `backend/.env` and fill in the required values:

```env
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=myneo4jpassword

AWS_PROFILE=<your-aws-profile>
```

Both the backend API and graph pipeline read from `backend/.env`.

## 2) Set up the graph virtual environment

From `graph/`:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 3) Build the graph DB

From `graph/` (with `.venv` activated):

```bash
make setup       # docker compose up + wait for Neo4j + apply schema
make pipeline    # ingest metadata -> extract/chunk text -> embed vectors
```

`make setup` runs `docker compose up -d` under the hood, so make sure Docker Desktop is running first.

The pipeline can take a couple of hours due to document extraction and Bedrock API calls.

### Run individual steps

```bash
make ingest      # load bill metadata from CSVs into Neo4j
make extract     # extract text from bill documents and create chunks
make embed       # generate embeddings via Bedrock and store in Neo4j
```

For a quick smoke test before running the full pipeline:

```bash
make extract MAX_DOCS=10
make embed
```

## 4) Start the backend API

From `backend/`:

```bash
poetry install
poetry run uvicorn src.main:app --host 0.0.0.0 --port 8000
```

Keep this running — the eval loop calls the API.

## 5) Run the eval loop

In another terminal, from `graph/`:

```bash
make query-example                                              # quick retrieval sanity check
python scripts/7_eval_loop.py                                   # full eval loop
```

Optional small eval:

```bash
python scripts/7_eval_loop.py --input-csv two_bills.csv --max-bills 2
```

Outputs are written to `graph/output/`.

## Other useful commands

From `graph/`:

```bash
make down        # stop Neo4j container
make down-v      # stop Neo4j and wipe volumes
make reset       # wipe volumes, restart Neo4j, re-apply schema
```

## Running experiments

The Makefile supports running parallel Neo4j instances with different configurations for A/B experiments:

```bash
make exp-a-setup      # start experiment A Neo4j instance
make exp-a-pipeline   # run full pipeline for experiment A
make exp-a-reset      # reset experiment A

make exp-b-setup      # start experiment B Neo4j instance
make exp-b-pipeline   # run full pipeline for experiment B
make exp-b-reset      # reset experiment B
```

Experiment env files are located in `graph/env/` (e.g. `exp-a.env`, `exp-b.env`).

## Tests

From `graph/`:

```bash
python tests/test_extract.py
python tests/test_chunk.py
```
