up:
	docker compose up -d

down:
	docker compose down

export PYTHONPATH := $(CURDIR)

schema:
	python scripts/run_schema.py

ingest:
	python scripts/1_ingest_metadata.py

extract:
	python scripts/2_extract_and_chunk.py $(if $(MAX_DOCS),--max-docs $(MAX_DOCS))

embed:
	python scripts/3_embed_and_cite.py

wait:
	@echo "Waiting for Neo4j..."
	@sleep 15

setup: up wait schema
pipeline: ingest extract embed

reset:
	docker compose down -v
	docker compose up -d
	@echo "Waiting for Neo4j..."
	@sleep 15
	python scripts/run_schema.py
