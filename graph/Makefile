export PYTHONPATH := $(CURDIR)/..
PYTHON := .venv/bin/python

BASE_ENV := ../backend/.env
WAIT_SECONDS ?= 15

EXP_A_ENV := env/exp-a.env
EXP_B_ENV := env/exp-b.env
EXP_A_PROJECT := graph-exp-a
EXP_B_PROJECT := graph-exp-b

COMPOSE_BASE := docker compose --env-file $(BASE_ENV)
COMPOSE_EXP_A := docker compose -p $(EXP_A_PROJECT) --env-file $(BASE_ENV) --env-file $(EXP_A_ENV)
COMPOSE_EXP_B := docker compose -p $(EXP_B_PROJECT) --env-file $(BASE_ENV) --env-file $(EXP_B_ENV)

up:
	$(COMPOSE_BASE) up -d

down:
	$(COMPOSE_BASE) down

down-v:
	$(COMPOSE_BASE) down -v

schema:
	$(PYTHON) scripts/run_schema.py

ingest:
	$(PYTHON) scripts/1_ingest_metadata.py

extract:
	$(PYTHON) scripts/2_extract_and_chunk.py $(if $(MAX_DOCS),--max-docs $(MAX_DOCS))

embed:
	$(PYTHON) scripts/3_embed.py

query-example:
	$(PYTHON) scripts/4_example_rag_query.py

wait:
	@echo "Waiting for Neo4j..."
	@sleep $(WAIT_SECONDS)

setup: up wait schema
pipeline: ingest extract embed

reset:
	$(COMPOSE_BASE) down -v
	$(COMPOSE_BASE) up -d
	$(MAKE) wait
	$(MAKE) schema

exp-a-up:
	$(COMPOSE_EXP_A) up -d

exp-a-down:
	$(COMPOSE_EXP_A) down

exp-a-down-v:
	$(COMPOSE_EXP_A) down -v

exp-a-schema:
	GRAPH_ENV_FILE=$(EXP_A_ENV) $(PYTHON) scripts/run_schema.py

exp-a-ingest:
	GRAPH_ENV_FILE=$(EXP_A_ENV) $(PYTHON) scripts/1_ingest_metadata.py

exp-a-extract:
	GRAPH_ENV_FILE=$(EXP_A_ENV) $(PYTHON) scripts/2_extract_and_chunk.py $(if $(MAX_DOCS),--max-docs $(MAX_DOCS))

exp-a-embed:
	GRAPH_ENV_FILE=$(EXP_A_ENV) $(PYTHON) scripts/3_embed.py

exp-a-query-example:
	GRAPH_ENV_FILE=$(EXP_A_ENV) $(PYTHON) scripts/4_example_rag_query.py

exp-a-setup: exp-a-up wait exp-a-schema
exp-a-pipeline: exp-a-ingest exp-a-extract exp-a-embed

exp-a-reset:
	$(COMPOSE_EXP_A) down -v
	$(COMPOSE_EXP_A) up -d
	$(MAKE) wait
	$(MAKE) exp-a-schema

exp-b-up:
	$(COMPOSE_EXP_B) up -d

exp-b-down:
	$(COMPOSE_EXP_B) down

exp-b-down-v:
	$(COMPOSE_EXP_B) down -v

exp-b-schema:
	GRAPH_ENV_FILE=$(EXP_B_ENV) $(PYTHON) scripts/run_schema.py

exp-b-ingest:
	GRAPH_ENV_FILE=$(EXP_B_ENV) $(PYTHON) scripts/1_ingest_metadata.py

exp-b-extract:
	GRAPH_ENV_FILE=$(EXP_B_ENV) $(PYTHON) scripts/2_extract_and_chunk.py $(if $(MAX_DOCS),--max-docs $(MAX_DOCS))

exp-b-embed:
	GRAPH_ENV_FILE=$(EXP_B_ENV) $(PYTHON) scripts/3_embed.py

exp-b-query-example:
	GRAPH_ENV_FILE=$(EXP_B_ENV) $(PYTHON) scripts/4_example_rag_query.py

exp-b-setup: exp-b-up wait exp-b-schema
exp-b-pipeline: exp-b-ingest exp-b-extract exp-b-embed

exp-b-reset:
	$(COMPOSE_EXP_B) down -v
	$(COMPOSE_EXP_B) up -d
	$(MAKE) wait
	$(MAKE) exp-b-schema
