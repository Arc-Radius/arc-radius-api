# Data Contract — Pipeline → Neo4j → API

This document defines the end-to-end data contract for bill data flowing through ArcRadius. Two paths produce bill records for Neo4j: the **batch pipeline** (notebooks) and the **live pipeline** (Lambda + SageMaker).

---

## Data Flow Overview

### Batch Pipeline (historical bills)
```
LegiScan Bulk CSVs
  → 01_concat_with_join_legiscan.py      → all_bills_2021_2026.csv
ACLU + Plural CSVs
  → 02_aclu_plural_bills_concat.ipynb     → all_lgbtq_bills_merged.csv
Both
  → 03_bills_data_prep_eda.ipynb          → matched_lgbtq_bills_with_duplicates.csv
  → relevance_and_stance_classifiers.ipynb → matched_lgbtq_bills.csv
  → graph/scripts/1_ingest_metadata.py    → Neo4j
```

### Live Pipeline (new/updated bills)
```
EventBridge (daily)
  → Lambda 1: Poll (LegiScan API)        → S3 CSV
  → Lambda 2: Classify (SageMaker)       → S3 CSV (LGBTQ+ matches only)
  → Lambda 3: Embed (Neo4j)              → Neo4j
```

---

## Stage 1: LegiScan Raw Data

**Source:** `all_bills_2021_2026.csv` (batch) or LegiScan `getBill` API (live)

| Field | Type | Description |
|---|---|---|
| `state` | string | 2-letter state abbreviation |
| `bill_id` | int | LegiScan bill ID |
| `session_id` | int | LegiScan session ID |
| `bill_number` | string | e.g., "HB1557", "SB1234" |
| `status` | int | 0=N/A, 1=Introduced, 2=Engrossed, 3=Enrolled, 4=Passed, 5=Vetoed, 6=Failed |
| `status_desc` | string | Human-readable: "Introduced", "Passed", etc. |
| `status_date` | string | Date of last status change |
| `title` | string | Bill title |
| `description` | string | Bill description/summary |
| `committee_id` | int | Committee ID |
| `committee` | string | Committee name |
| `last_action_date` | string | Date of most recent action |
| `last_action` | string | Text of most recent action |
| `url` | string | LegiScan URL |
| `state_link` | string | State legislature URL |
| `sponsor_names` | string | Pipe-separated: "Smith \| Jones" |
| `sponsor_parties` | string | Pipe-separated: "R \| D \| R" |
| `primary_sponsor` | string | Name of primary sponsor |
| `sponsor_count` | int | Total sponsor count |
| `action_count` | int | Total action count |
| `last_history_action` | string | Last history action text |
| `document_count` | int | Number of documents |
| `document_id` | int | Most recent document ID |
| `document_type` | string | e.g., "Introduced", "Enrolled" |
| `document_url` | string | URL to bill document |
| `rollcall_count` | int | Number of roll call votes |
| `total_yea` | int | Total yea votes across roll calls |
| `total_nay` | int | Total nay votes across roll calls |

---

## Stage 2: LGBTQ+ Labels

**Source:** `all_lgbtq_bills_merged.csv` (batch) or SageMaker endpoint (live)

### Batch — from ACLU/Plural source data
| Field | Type | Description |
|---|---|---|
| `label` | string | "harmful" or "supportive" |
| `label_source` | string | "aclu" or "plural" |
| `issues` | string | Raw ACLU issue text, pipe-separated (batch only — not available for live bills) |

NB02 also maps ACLU/Plural text statuses (e.g., "Advancing", "Defeated") to LegiScan numeric `status` codes and preserves the original text as `status_desc`.

### Live — from SageMaker endpoint response
| Field | Type | Description |
|---|---|---|
| `lgbtq_related` | bool | Is the bill LGBTQ+ relevant? |
| `relevance_score` | float | 0.0–1.0, relevance model confidence |
| `label` | string | "harmful", "supportive", or "not_relevant" |
| `confidence` | float | 0.0–1.0, stance model confidence |
| `bill_dominant_party` | string | "R", "D", "Bipartisan", or "Other" |
| `state_r_sponsorship_ratio` | float | 0.0–1.0, R-sponsored / total partisan bills for the state |
| `state_lean` | string | 5-tier: "Strong R", "Lean R", "Competitive", "Lean D", "Strong D" |
| `pass_rate_gap` | float | R pass rate minus D pass rate for the state |
| `overall_pass_rate` | float | Total passed / total bills for the state |

---

## Stage 3: Computed Fields (NB03 / SageMaker)

These fields are derived from raw data. Both pipelines now produce them with aligned logic.

### Per-Bill Fields (from `sponsor_parties`)
| Field | Logic | Notes |
|---|---|---|
| `r_sponsors` | Count of "R" in sponsor_parties | NB03 only (not stored in Neo4j) |
| `d_sponsors` | Count of "D" in sponsor_parties | NB03 only |
| `other_sponsors` | Count of non-R, non-D, non-empty | Empty strings excluded (aligned) |
| `bill_dominant_party` | R > D → "R", D > R → "D", tied → "Bipartisan", neither → "Other" | Stored in Neo4j, output by SageMaker |

### Per-State-Year Fields (aggregated from all bills)
| Field | Logic | Notes |
|---|---|---|
| `state_r_sponsorship_ratio` | R-sponsored bills / (R + D sponsored bills) | >0.5 = R-leaning legislature |
| `pass_rate_gap` | R bill pass rate - D bill pass rate | Positive = R bills pass more. fillna(0) |
| `overall_pass_rate` | Total passed / total bills | General legislative productivity |
| `state_lean` | 5-tier classification from ratio + gap | See classification logic below |

### `state_lean` Classification Logic
```
ratio > 0.7:   Strong R (downgrade to Lean R if gap < -0.1)
ratio > 0.55:  Strong R if gap > 0.1, Competitive if gap < -0.1, else Lean R
ratio < 0.3:   Strong D (downgrade to Lean D if gap > 0.1)
ratio < 0.45:  Strong D if gap < -0.1, Competitive if gap > 0.1, else Lean D
else (0.45-0.55): Lean R if gap > 0.1, Lean D if gap < -0.1, else Competitive
```
When ratio and gap contradict, the classification downgrades one tier toward Competitive.

### SageMaker Internal Only (not stored in Neo4j)
| Field | Logic |
|---|---|
| `percent_yea` | total_yea / (total_yea + total_nay) |
| `percent_nay` | total_nay / (total_yea + total_nay) |

---

## Stage 4: Issue Categorization

**Source:** TOPIC_RULES (identical copies in NB03 and `lambda_classify.py`)

| Category | Keywords |
|---|---|
| `healthcare` | healthcare, medical, heathcare |
| `sports` | sports, athletics |
| `education` | student, educator, school restriction, school or curriculum |
| `curriculum_speech` | curriculum, outing, don't say |
| `facilities` | facilities, facility, bathroom, single-sex, public accommodation |
| `religious_exemption` | religious, rfra |
| `identity_documents` | accurate id, identification document, gender marker, definition of sex, re-definition, barriers to accurate |
| `expression` | drag, free speech, expression ban, expression restriction |
| `civil_rights` | civil rights, nondiscrimination, equality |

- **NB03:** Splits `issues` on `|`, checks each segment. Falls back to `title | description` for bills with no issues.
- **Lambda:** Checks `title + description` as full text (no pipe-splitting — raw text has no pipes).
- Default category: `["other"]` if no keywords match.

---

## Stage 5: Neo4j Bill Node Properties

Written by `1_ingest_metadata.py` (batch) or `lambda_embed_neo4j.py` (live).

### From LegiScan (FALLBACK_FIELDS)
`state`, `session_id`, `bill_number`, `status`, `status_desc`, `status_date`, `title`, `description`, `last_action_date`, `last_action`, `url`, `state_link`, `document_id`, `document_type`, `document_url`

### From EDA / SageMaker (EDA_FIELDS)
`label`, `label_source`, `year`, `state_lean`, `bill_dominant_party`, `state_r_sponsorship_ratio`, `pass_rate_gap`, `overall_pass_rate`, `session_year`, `issues` (batch only), `issue_categories`

### Live-only (from SageMaker via Lambda)
`confidence`, `relevance_score`

---

## Stage 6: API Response

### Bill List (`/bills`) — from `ui_queries.py`
- `billTab`: `"passed"` if `status = 4`, else `"active"`
- `stance`: `label` value or `"mixed"` if null
- `status`: numeric status as string
- `status_desc`: human-readable status
- `issue_categories`: from Topic node relationships

### Bill Detail (`/bills/:id`) — from `ui_queries.py`
- All bill list fields plus sponsors, action history, committee
- `graphRecord`: all Neo4j Bill node properties (via `properties(b)`)

### RAG Context (`/bills/rag`) — from `formatting.py`
Context block sent to LLM includes:
- State, bill_number, title, year
- Status (from `status_desc`, falls back to numeric → STATUS_DESC map)
- Label, state_lean, state_r_sponsorship_ratio
- Section path, chunk text, URLs

---

## SageMaker Endpoint Contract

### Request (Lambda sends raw fields)
```json
{
  "text": "An act prohibiting gender transition...",
  "state": "TX",
  "sponsor_parties": "R | R | R",
  "total_yea": 0,
  "total_nay": 0
}
```

### Response
```json
{
  "lgbtq_related": true,
  "relevance_score": 0.94,
  "label": "harmful",
  "confidence": 0.87,
  "bill_dominant_party": "R",
  "state_r_sponsorship_ratio": 0.72,
  "state_lean": "Strong R",
  "pass_rate_gap": 0.25,
  "overall_pass_rate": 0.30,
  "stance_proba": {"supportive": 0.13, "harmful": 0.87}
}
```

### Stance Classifier Features (7)
`state_r_sponsorship_ratio`, `state_lean` (ordinal 0-4), `bill_dominant_party` (binary R=1), `percent_nay`, `r_sponsors`, `d_sponsors`, `other_sponsors`

### Baked into model.tar.gz
- LegalBERT weights + tokenizer (relevance model)
- stance_model.joblib (LogReg)
- state_profiles.joblib: `{state → {state_r_sponsorship_ratio, pass_rate_gap, overall_pass_rate, state_lean}}`
- code/inference.py

---

## Lambda 2 (Classify) Output CSV Fields

All raw LegiScan fields from Lambda 1, plus:
`status_desc`, `year`, `session_year`, `label`, `label_source`, `confidence`, `relevance_score`, `issue_categories`, `bill_dominant_party`, `state_r_sponsorship_ratio`, `state_lean`, `pass_rate_gap`, `overall_pass_rate`

---

## Known Gaps (Batch vs Live)

| Field | Batch | Live | Impact |
|---|---|---|---|
| `issues` | ACLU/Plural raw text | Not available | Low — `issue_categories` covers the use case |
| `confidence` | Not computed | SageMaker output | Low — only batch ACLU/Plural bills lack it |
| `relevance_score` | Not computed | SageMaker output | Low — batch bills are pre-labeled by ACLU/Plural |
| `r_sponsors`, `d_sponsors`, `other_sponsors` | Computed in NB03 (not stored in Neo4j) | Computed inside SageMaker (not output) | None — `bill_dominant_party` captures the signal |
| State profiles | Recomputed on each NB03 run | Frozen at model deploy time | Profiles drift until model retrain/redeploy |

---

## Resolved Issues

1. **Status type mismatch** — `status_desc` now flows from both LegiScan CSV and NB02 ACLU/Plural mapping. `formatting.py` uses `status_desc` with numeric fallback.
2. **Boolean string round-trip** — Removed `passed`/`failed`/`vetoed` columns entirely. All consumers use numeric `status` (4=Passed, 5=Vetoed, 6=Failed).
3. **ACLU/Plural status dropped** — NB02 now preserves original text as `status_desc` and maps to numeric `status` via `ACLU_STATUS_TO_LEGISCAN`.
4. **Duplicate TOPIC_RULES** — Acknowledged; currently identical in NB03 and lambda_classify.py. Drift risk noted.
5. **`dominant_party` vs `bill_dominant_party`** — Standardized on `bill_dominant_party` across all notebooks, SageMaker, Lambda, Neo4j, API.
6. **`state_r_sponsorship_ratio` vs `r_sponsorship_ratio`** — Standardized on `state_r_sponsorship_ratio` across all notebooks, SageMaker, Lambda, Neo4j, API, frontend.
7. **`state_lean` 5-tier vs binary** — Removed binary `state_R_leaning`. SageMaker now computes 5-tier `state_lean` from ratio + gap (matches NB03). Added as ordinal feature (0-4) to stance classifier. Contradiction-aware logic downgrades toward Competitive when ratio and gap disagree.
8. **Empty string in Other sponsor count** — Aligned all `parse_party_counts` to exclude empty strings: `p not in ('R', 'D', '')`.
9. **NB03-only `pass_rate_gap`, `overall_pass_rate`** — SageMaker now computes both, bakes into `state_profiles.joblib`, and outputs in endpoint response. Lambda passes through to Neo4j.
10. **SageMaker-only `percent_yea`, `percent_nay`** — Internal to SageMaker inference only. Not needed in Neo4j or API. No action taken.
11. **FL/KS bill number normalization** — Resolved in commit 3768e33. `STATE_PREFIX_MAP` updated in NB02.
12. **`bipartisan_ratio` removed** — Low-value display metric removed from NB03, SageMaker, Neo4j ingest, API, RAG context, and frontend.
