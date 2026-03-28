"""Keep UI contract tests on mock data even if developer .env enables Neo4j."""

import os

os.environ["NEO4J_UI_ENABLED"] = "false"
