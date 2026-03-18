"""
Neo4j client — module scope driver.

Created ONCE on Lambda cold start, reused across warm invocations.
"""

import os
from neo4j import GraphDatabase

_uri = os.environ.get("NEO4J_URI", "")
_user = os.environ.get("NEO4J_USER", "neo4j")
_pw = os.environ.get("NEO4J_PASSWORD", "")

# Module-scope driver — initialized once on cold start
driver = GraphDatabase.driver(_uri, auth=(_user, _pw)) if _uri else None


class Neo4j:
    """
    Wrapper matching teammate's interface.
    Uses the module-scope driver instead of creating a new one each time.
    """

    def __init__(self):
        if driver is None:
            raise RuntimeError("NEO4J_URI not set — cannot connect")
        self.driver = driver

    def run(self, cypher: str, **params):
        with self.driver.session() as session:
            return session.run(cypher, params).data()

    def run_batch(self, cypher: str, rows: list[dict], key: str = "rows"):
        with self.driver.session() as session:
            return session.run(cypher, {key: rows}).data()

    def close(self):
        # No-op: module-scope driver stays alive across invocations
        pass
