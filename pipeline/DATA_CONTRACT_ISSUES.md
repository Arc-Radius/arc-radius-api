# Data Contract Issues — Pipeline → Neo4j → API

### Status Column Issues
1. **Status type mismatch risk**: `matched_lgbtq_bills.csv` has LegiScan numeric `status` (4=Passed, 5=Vetoed, 6=Failed). Stored as `b.status` on Bill node. `formatting.py` falls back to `m.get("status")` as display text — would show `4` instead of `"Passed"` if booleans are falsy.
2. **Boolean string round-trip**: `passed`/`failed`/`vetoed` could be stored as strings `"True"`/`"False"` from CSV, making `if m.get("passed")` always truthy.
3. **ACLU/Plural status dropped**: Text status from notebook 02 ("Introduced", "Signed") is dropped during LegiScan merge. Unmatched bills lose status entirely.

### TOPIC_RULES Drift
4. **Duplicate TOPIC_RULES**: Notebook 03 (cell 19) and Lambda classify (`lambda_classify.py`) each have their own copy. These could drift out of sync.

### Computed Field Column Name Mismatches
5. **`dominant_party` vs `bill_dominant_party`**: SageMaker notebook uses `dominant_party`, NB03 uses `bill_dominant_party`. Neo4j ingest expects `bill_dominant_party`.
6. **`state_r_sponsorship_ratio` vs `r_sponsorship_ratio`**: SageMaker uses `state_r_sponsorship_ratio`, NB03 uses `r_sponsorship_ratio`. Neo4j ingest expects `r_sponsorship_ratio`.

### Computed Field Logic Mismatches
7. **`state_lean` granularity**: NB03 computes 5-tier (Strong R, Lean R, Competitive, Lean D, Strong D) using `r_sponsorship_ratio` + `pass_rate_gap`. SageMaker uses binary `state_R_leaning` boolean (`ratio > 0.5`). Neo4j stores the 5-tier version.
8. **Empty string in Other sponsor count**: NB03 `parse_party_counts` filters `not in ('R', 'D')` — counts empty strings as Other. SageMaker filters `not in ('R', 'D', '')` — excludes empty strings. This shifts `bill_dominant_party` for bills with no sponsor party data.

### Fields Only in One Notebook
9. **NB03 only**: `pass_rate_gap`, `overall_pass_rate` — not computed in SageMaker. Stored in Neo4j.
10. **SageMaker only**: `percent_yea`, `percent_nay` — not computed in NB03. Not stored in Neo4j.

### Bill Number Normalization
11. **Florida/Kansas fix**: Added `FL` and `KS` to `STATE_PREFIX_MAP` in notebook 02. Need to re-run pipeline to regenerate `matched_lgbtq_bills.csv` with correct FL/KS bill numbers.

**Why:** These mismatches mean the Neo4j graph may have incorrect or missing data for certain bills, and the API could display wrong status info or scores. If the SageMaker endpoint recomputes features at inference time with different column names or logic, predictions won't align with what's stored in Neo4j.

**How to apply:** Verify each field end-to-end from notebook computation → CSV → Neo4j ingest → API response. Standardize column names and logic across NB03 and SageMaker. Fix at the source and re-run the pipeline.
