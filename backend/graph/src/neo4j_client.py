import os

from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()


class Neo4j:
    def __init__(self):
        uri = os.environ["NEO4J_URI"]
        user = os.environ["NEO4J_USER"]
        pw = os.environ["NEO4J_PASSWORD"]
        self.driver = GraphDatabase.driver(uri, auth=(user, pw))

    def run(self, cypher: str, **params):
        with self.driver.session() as session:
            return session.run(cypher, params).data()

    def run_batch(self, cypher: str, rows: list[dict], key: str = "rows"):
        """Run a single UNWIND-style query with a list of row dicts."""
        with self.driver.session() as session:
            return session.run(cypher, {key: rows}).data()

    def close(self):
        self.driver.close()
