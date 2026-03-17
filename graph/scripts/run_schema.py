from graph.api.neo4j_client import Neo4j

def main():
    db = Neo4j()
    with open("scripts/schema.cypher", "r", encoding="utf-8") as f:
        cypher = f.read()

    for stmt in cypher.split(";"):
        stmt = stmt.strip()
        if stmt:
            db.run(stmt)

    db.close()
    print("[OK] schema applied")


if __name__ == "__main__":
    main()
