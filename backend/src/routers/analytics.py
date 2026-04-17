"""Analytics endpoints — diffusion analysis powered by Neo4j knowledge graph."""

import logging

from fastapi import APIRouter, HTTPException, Response

from src.core.settings import settings
from src.db.neo4j_client import neo4j_read

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/analytics", tags=["analytics"])

CACHE_HEADER = "public, max-age=600"


def _require_neo4j():
    if not settings.neo4j_ui_enabled:
        raise HTTPException(status_code=503, detail="Neo4j not enabled")


@router.get("/issue-wave", summary="Bills by issue category and year")
async def issue_wave(response: Response):
    _require_neo4j()
    response.headers["Cache-Control"] = CACHE_HEADER
    try:
        rows = await neo4j_read("""
            MATCH (b:Bill)-[:HAS_TOPIC]->(t:Topic)
            WHERE b.session_year IS NOT NULL
            WITH t.name AS category,
                 toInteger(b.session_year) AS year,
                 count(b) AS count
            RETURN category, year, count
            ORDER BY year, category
        """)
        return rows
    except Exception:
        logger.exception("issue-wave query failed")
        raise HTTPException(status_code=503, detail="Neo4j unavailable")


@router.get("/spread-timeline", summary="Harmful vs supportive bill counts over time")
async def spread_timeline(response: Response):
    _require_neo4j()
    response.headers["Cache-Control"] = CACHE_HEADER
    try:
        rows = await neo4j_read("""
            MATCH (b:Bill)
            WHERE b.session_year IS NOT NULL AND b.label IS NOT NULL
            WITH toInteger(b.session_year) AS year,
                 b.label AS label,
                 count(b) AS count,
                 count(DISTINCT b.state) AS state_count
            RETURN year, label, count, state_count
            ORDER BY year, label
        """)
        timeline: dict[int, dict] = {}
        for r in rows:
            yr = r["year"]
            if yr not in timeline:
                timeline[yr] = {
                    "year": yr, "harmful": 0, "supportive": 0,
                    "harmful_states": 0, "supportive_states": 0,
                }
            label = r["label"]
            if label in ("harmful", "supportive"):
                timeline[yr][label] = r["count"]
                timeline[yr][f"{label}_states"] = r["state_count"]
        return sorted(timeline.values(), key=lambda x: x["year"])
    except Exception:
        logger.exception("spread-timeline query failed")
        raise HTTPException(status_code=503, detail="Neo4j unavailable")


@router.get("/state-heatmap", summary="Harmful bill counts per state and year")
async def state_heatmap(response: Response):
    _require_neo4j()
    response.headers["Cache-Control"] = CACHE_HEADER
    try:
        rows = await neo4j_read("""
            MATCH (b:Bill)-[:IN_STATE]->(s:State)
            WHERE b.label = 'harmful' AND b.session_year IS NOT NULL
            RETURN s.code AS state,
                   toInteger(b.session_year) AS year,
                   count(b) AS count
            ORDER BY count DESC
        """)
        return rows
    except Exception:
        logger.exception("state-heatmap query failed")
        raise HTTPException(status_code=503, detail="Neo4j unavailable")


@router.get("/state-profiles", summary="Per-state harmful/supportive totals with political lean")
async def state_profiles(response: Response):
    _require_neo4j()
    response.headers["Cache-Control"] = CACHE_HEADER
    try:
        rows = await neo4j_read("""
            MATCH (b:Bill)-[:IN_STATE]->(s:State)
            WHERE b.label IS NOT NULL
            WITH s.code AS state,
                 sum(CASE WHEN b.label = 'harmful' THEN 1 ELSE 0 END) AS harmful,
                 sum(CASE WHEN b.label = 'supportive' THEN 1 ELSE 0 END) AS supportive,
                 collect(DISTINCT b.state_lean)[-1] AS state_lean
            RETURN state, harmful, supportive, state_lean
            ORDER BY harmful DESC
        """)
        return rows
    except Exception:
        logger.exception("state-profiles query failed")
        raise HTTPException(status_code=503, detail="Neo4j unavailable")


@router.get("/similarity-network", summary="Cross-state bill text similarity from embeddings")
async def similarity_network(response: Response, threshold: float = 0.88):
    _require_neo4j()
    response.headers["Cache-Control"] = CACHE_HEADER
    try:
        rows = await neo4j_read("""
            MATCH (b1:Bill)-[:HAS_DOCUMENT]->(:Document)-[:HAS_CHUNK]->(c1:Chunk)
            WHERE c1.embedding IS NOT NULL AND b1.state IS NOT NULL
            WITH b1, collect(c1.embedding)[0] AS emb1
            WHERE emb1 IS NOT NULL
            CALL db.index.vector.queryNodes('chunkEmbeddingIndex', 10, emb1)
            YIELD node AS c2, score
            MATCH (c2)<-[:HAS_CHUNK]-(:Document)<-[:HAS_DOCUMENT]-(b2:Bill)
            WHERE b2.state <> b1.state AND score >= $threshold
            WITH b1.state AS state_a, b2.state AS state_b,
                 count(*) AS pair_count, avg(score) AS avg_similarity
            RETURN state_a, state_b, pair_count, avg_similarity
            ORDER BY pair_count DESC
            LIMIT 200
        """, {"threshold": threshold})
        return rows
    except Exception:
        logger.exception("similarity-network query failed")
        raise HTTPException(status_code=503, detail="Neo4j unavailable")


# ── 1. Sponsor Network ────────────────────────────────────


@router.get("/sponsor-network", summary="Sponsors and their bills for a state")
async def sponsor_network(response: Response, state: str):
    _require_neo4j()
    response.headers["Cache-Control"] = CACHE_HEADER
    try:
        rows = await neo4j_read("""
            MATCH (p:Person)-[sp:SPONSORS]->(b:Bill)-[:IN_STATE]->(s:State {code: $state})
            WHERE b.label IS NOT NULL
            WITH p.people_id AS person_id, p.name AS name, p.party AS party,
                 collect({
                     bill_pk: b.bill_pk, bill_number: b.bill_number,
                     title: b.title, label: b.label
                 }) AS bills
            WHERE size(bills) >= 2
            RETURN person_id, name, party, bills
            ORDER BY size(bills) DESC
            LIMIT 30
        """, {"state": state.upper()})
        return rows
    except Exception:
        logger.exception("sponsor-network query failed")
        raise HTTPException(status_code=503, detail="Neo4j unavailable")


# ── 2. Bill Similarity Explorer ───────────────────────────


@router.get("/bill-similarity", summary="Find similar bills to a given bill via embeddings")
async def bill_similarity(response: Response, bill_pk: str, top: int = 10):
    _require_neo4j()
    response.headers["Cache-Control"] = CACHE_HEADER
    try:
        rows = await neo4j_read("""
            MATCH (b1:Bill {bill_pk: $bill_pk})-[:HAS_DOCUMENT]->(:Document)-[:HAS_CHUNK]->(c1:Chunk)
            WHERE c1.embedding IS NOT NULL
            WITH b1, collect(c1.embedding)[0] AS emb
            WHERE emb IS NOT NULL
            CALL db.index.vector.queryNodes('chunkEmbeddingIndex', $top * 3, emb)
            YIELD node AS c2, score
            MATCH (c2)<-[:HAS_CHUNK]-(:Document)<-[:HAS_DOCUMENT]-(b2:Bill)
            WHERE b2.bill_pk <> $bill_pk
            WITH b2, max(score) AS similarity
            RETURN b2.bill_pk AS bill_pk, b2.state AS state,
                   b2.bill_number AS bill_number, b2.title AS title,
                   b2.label AS label, toInteger(b2.session_year) AS session_year,
                   round(similarity * 1000) / 1000 AS similarity
            ORDER BY similarity DESC
            LIMIT $top
        """, {"bill_pk": bill_pk, "top": top})
        return rows
    except Exception:
        logger.exception("bill-similarity query failed")
        raise HTTPException(status_code=503, detail="Neo4j unavailable")


# ── 3 & 4. State Clusters by category ────────────────────


@router.get("/state-clusters", summary="States grouped by shared issue categories")
async def state_clusters(response: Response, label: str = "harmful"):
    _require_neo4j()
    response.headers["Cache-Control"] = CACHE_HEADER
    try:
        rows = await neo4j_read("""
            MATCH (b:Bill)-[:HAS_TOPIC]->(t:Topic),
                  (b)-[:IN_STATE]->(s:State)
            WHERE b.label = $label
            WITH s.code AS state, t.name AS category, count(b) AS count
            RETURN state, category, count
            ORDER BY state, count DESC
        """, {"label": label})

        state_map: dict[str, dict[str, int]] = {}
        for r in rows:
            state_map.setdefault(r["state"], {})[r["category"]] = r["count"]

        states = [
            {"state": st, "categories": cats, "total": sum(cats.values())}
            for st, cats in state_map.items()
        ]
        states.sort(key=lambda x: -x["total"])
        return states
    except Exception:
        logger.exception("state-clusters query failed")
        raise HTTPException(status_code=503, detail="Neo4j unavailable")


# ── 5. Model Bill Detector ────────────────────────────────


@router.get("/model-bills", summary="Near-identical bill pairs across states")
async def model_bills(response: Response, threshold: float = 0.90, limit: int = 50):
    _require_neo4j()
    response.headers["Cache-Control"] = CACHE_HEADER
    try:
        rows = await neo4j_read("""
            MATCH (b1:Bill)-[:HAS_DOCUMENT]->(:Document)-[:HAS_CHUNK]->(c1:Chunk)
            WHERE c1.embedding IS NOT NULL AND b1.state IS NOT NULL
                  AND b1.label IS NOT NULL
            WITH b1, collect(c1.embedding)[0] AS emb
            WHERE emb IS NOT NULL
            CALL db.index.vector.queryNodes('chunkEmbeddingIndex', 5, emb)
            YIELD node AS c2, score
            WHERE score >= $threshold
            MATCH (c2)<-[:HAS_CHUNK]-(:Document)<-[:HAS_DOCUMENT]-(b2:Bill)
            WHERE b2.bill_pk > b1.bill_pk AND b2.state <> b1.state
            RETURN b1.bill_pk AS bill_pk_a, b1.state AS state_a,
                   b1.bill_number AS bill_number_a, b1.title AS title_a, b1.label AS label_a,
                   b2.bill_pk AS bill_pk_b, b2.state AS state_b,
                   b2.bill_number AS bill_number_b, b2.title AS title_b, b2.label AS label_b,
                   round(score * 1000) / 1000 AS similarity
            ORDER BY similarity DESC
            LIMIT $limit
        """, {"threshold": threshold, "limit": limit})
        return rows
    except Exception:
        logger.exception("model-bills query failed")
        raise HTTPException(status_code=503, detail="Neo4j unavailable")


# ── 6. Bill Passage Funnel ────────────────────────────────


@router.get("/passage-funnel", summary="Bill progression stages by stance")
async def passage_funnel(response: Response):
    _require_neo4j()
    response.headers["Cache-Control"] = CACHE_HEADER
    try:
        rows = await neo4j_read("""
            MATCH (b:Bill)
            WHERE b.label IS NOT NULL AND b.status IS NOT NULL
            WITH b.label AS label,
                 toInteger(b.status) AS status,
                 count(b) AS count
            RETURN label, status, count
            ORDER BY label, status
        """)
        STAGES = {1: "Introduced", 2: "Engrossed", 3: "Enrolled", 4: "Passed", 5: "Vetoed", 6: "Failed"}
        result: dict[str, list] = {"harmful": [], "supportive": []}
        for r in rows:
            label = r["label"]
            if label in result:
                result[label].append({
                    "status": r["status"],
                    "stage": STAGES.get(r["status"], f"Status {r['status']}"),
                    "count": r["count"],
                })
        return result
    except Exception:
        logger.exception("passage-funnel query failed")
        raise HTTPException(status_code=503, detail="Neo4j unavailable")


# ── 7. Party Crossover ───────────────────────────────────


@router.get("/party-crossover", summary="Bills where sponsor party doesn't match state lean")
async def party_crossover(response: Response):
    _require_neo4j()
    response.headers["Cache-Control"] = CACHE_HEADER
    try:
        rows = await neo4j_read("""
            MATCH (b:Bill)-[:IN_STATE]->(s:State)
            WHERE b.label IS NOT NULL
                  AND b.bill_dominant_party IS NOT NULL
                  AND b.state_lean IS NOT NULL
            WITH s.code AS state, b.bill_pk AS bill_pk,
                 b.bill_number AS bill_number, b.title AS title,
                 b.label AS label, b.bill_dominant_party AS sponsor_party,
                 b.state_lean AS state_lean
            WHERE (sponsor_party = 'D' AND state_lean IN ['Strong R', 'Lean R'])
               OR (sponsor_party = 'R' AND state_lean IN ['Strong D', 'Lean D'])
            RETURN state, bill_pk, bill_number, title, label,
                   sponsor_party, state_lean
            ORDER BY state, label
        """)
        return rows
    except Exception:
        logger.exception("party-crossover query failed")
        raise HTTPException(status_code=503, detail="Neo4j unavailable")


# ── 8. Diffusion Chain ───────────────────────────────────


@router.get("/diffusion-chain", summary="Chronological adoption of an issue category across states")
async def diffusion_chain(response: Response, category: str = "sports"):
    _require_neo4j()
    response.headers["Cache-Control"] = CACHE_HEADER
    try:
        rows = await neo4j_read("""
            MATCH (b:Bill)-[:HAS_TOPIC]->(t:Topic {name: $category}),
                  (b)-[:IN_STATE]->(s:State)
            WHERE b.session_year IS NOT NULL
            WITH s.code AS state, toInteger(b.session_year) AS year,
                 count(b) AS bill_count, b.label AS label
            WITH state, year, sum(bill_count) AS total_bills,
                 sum(CASE WHEN label = 'harmful' THEN bill_count ELSE 0 END) AS harmful,
                 sum(CASE WHEN label = 'supportive' THEN bill_count ELSE 0 END) AS supportive
            RETURN state, year, total_bills, harmful, supportive
            ORDER BY year, state
        """, {"category": category})

        state_first: dict[str, int] = {}
        for r in rows:
            if r["state"] not in state_first:
                state_first[r["state"]] = r["year"]

        return {
            "category": category,
            "timeline": rows,
            "first_adopters": sorted(state_first.items(), key=lambda x: x[1]),
        }
    except Exception:
        logger.exception("diffusion-chain query failed")
        raise HTTPException(status_code=503, detail="Neo4j unavailable")
