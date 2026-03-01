# Arc Dagster Pipeline

Standalone Dagster+ Serverless orchestration for Arc LegiScan ingestion.

## Local setup

```bash
cd inference_pipeline
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## Required environment variables

- `LEGISCAN_API_KEY`
- `PIPELINE_ARTIFACT_BUCKET`
- `PIPELINE_SESSION_STATE_TABLE` (default: `session_state`)
- `PIPELINE_WATERMARK_TABLE` (default: `pipeline_watermark`)
- `AWS_DEFAULT_REGION`
- AWS auth variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional `AWS_SESSION_TOKEN`)

## Validate definitions and run locally

```bash
dagster definitions validate -m arc_pipeline.definitions
dagster dev -m arc_pipeline.definitions
```

## Dagster+ Serverless code location

- Module: `arc_pipeline.definitions`
- Attribute: `defs`

## S3 layout

All outputs are rooted under `inference/`:

- `inference/raw/legiscan/date=YYYY-MM-DD/run_id=<run_id>/...`
- `inference/normalized/legiscan/date=YYYY-MM-DD/run_id=<run_id>/...`
- `inference/graph/rows/date=YYYY-MM-DD/run_id=<run_id>/...`
- `inference/classification/date=YYYY-MM-DD/run_id=<run_id>/pending_bill_ids.jsonl`

