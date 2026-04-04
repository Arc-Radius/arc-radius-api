# Bill Number Format & Matching Pipeline

## Overview

We match LGBTQ+ bills from two advocacy sources (ACLU, Plural) against the LegiScan legislative database. The key challenge is **bill number normalization** — each source formats bill numbers differently, and LegiScan's format varies by state.

## Data Sources

| Source | Format Example | Coverage |
|--------|---------------|----------|
| ACLU Legislation Tracker | `H.B. 1570`, `S.B. 14`, `HB 1` | 2021-2026, ~1900 bills |
| Plural (formerly TrackBill) | `HB 1604`, `AB 600`, `S 1505` | 2024-2025, ~600 bills |
| LegiScan | `HB1570`, `SB14`, `H1357` | All US states, all years |

## Bill Number Structure

A bill number has three parts: **Chamber** + **Type** + **Number**

```
HB1570
│││
│││── 1570 = bill number
││─── B = Bill (type)
│──── H = House (chamber)
```

## State-Specific Formats

Most states follow `{Chamber}{Type}{Number}` (e.g., `HB1570`, `SB14`), but several states have unique conventions:

### States where B is omitted (just Chamber + Number)

| State | LegiScan Format | Example |
|-------|----------------|---------|
| Florida | `H{num}` / `S{num}` (zero-padded to 4) | `H0731`, `S0440` |
| Idaho | `H{num}` / `S{num}` | `H730`, `S1234` |
| Massachusetts | `H{num}` / `S{num}` | `H1023`, `S3499` |
| North Carolina | `H{num}` / `S{num}` | `H2082`, `S1461` |
| New York | `A{num}` / `S{num}` | `A09502`, `S08296` |
| New Jersey | `A{num}` / `S{num}` | `A2956`, `S1234` |
| Rhode Island | `H{num}` / `S{num}` | `H7199`, `S2646` |
| South Carolina | `H{num}` / `S{num}` | `H5047`, `S1463` |
| Vermont | `H{num}` / `S{num}` | `H793`, `S123` |

### States with standard HB/SB format (ACLU uses H/S shorthand)

| State | LegiScan Format | ACLU/Plural Format | Mapping |
|-------|----------------|-------------------|---------|
| Kansas | `HB{num}` / `SB{num}` | `H{num}` / `S{num}` | `H → HB`, `S → SB` |

### States with non-standard prefixes

| State | Chamber | Type | Example |
|-------|---------|------|---------|
| California | A (Assembly) / S (Senate) | B (Bill) | `AB2799`, `SB59` |
| Nevada | A (Assembly) / S (Senate) | B (Bill) | `AB564`, `SB435` |
| Wisconsin | A (Assembly) / S (Senate) | B (Bill) | `AB978`, `SB123` |
| Iowa | H / S | F (File) | `HF2534`, `SF1234` |
| Minnesota | H / S | F (File) | `HF3864`, `SF3430` |
| Nebraska | L (Legislature) | B (Bill) | `LB1109` (unicameral) |
| Maine | — | LD (Bill) / HP (Paper) | `LD1831`, `HP746` |
| Wyoming | H / S | B / F | `HB132`, `SF79` |

### States with special formatting

| State | Convention | Example |
|-------|-----------|---------|
| Colorado | Year prefix removed: `HB21-1186` → `HB1186` | `HB1186` |
| Connecticut | 5-digit zero-padded | `HB05934` |
| Florida | 4-digit zero-padded | `H0731` |
| North Dakota | Number-only, position determines type | `1577` (odd=House, even=Senate) |
| Michigan | Joint Resolutions use letters (A-Z, AA-ZZ) | `SJRB` |

## Normalization Pipeline (`02_aclu_plural_bills_concat.ipynb`)

### Step 1: State Normalization
Full state names → 2-letter abbreviations (e.g., `"Florida"` → `"FL"`)

### Step 2: Bill Number Normalization
`normalize_bill_number(bill_name, state)` handles:

1. **Remove dots and extra spaces**: `H.B. 1570` → `HB 1570` → `HB1570`
2. **Colorado year stripping**: `HB21-1186` → `HB1186`
3. **Extract prefix and number**: Split `HB1570` → prefix=`HB`, number=`1570`
4. **State-specific prefix mapping**: `HB` → `H` for FL, ID, etc.; `H` → `HB` for KS
5. **Leading zero handling**: Strip zeros for most states, pad to 5 for CT, pad to 4 for FL
6. **Suffix handling**: NY amendment letters like `A691A`

### `STATE_PREFIX_MAP`

Maps ACLU/Plural prefixes to LegiScan format for non-standard states:

```python
STATE_PREFIX_MAP = {
    # B-omitted states (LegiScan uses H/S without B)
    'FL': {'HB': 'H', 'SB': 'S', 'H': 'H', 'S': 'S'},
    'ID': {'HB': 'H', 'SB': 'S', 'H': 'H', 'S': 'S'},
    'MA': {'HB': 'H', 'SB': 'S', 'H': 'H', 'S': 'S', 'HD': 'HD'},
    'NC': {'HB': 'H', 'SB': 'S', 'H': 'H', 'S': 'S'},
    'NJ': {'AB': 'A', 'SB': 'S', 'A': 'A', 'S': 'S'},
    'NY': {'AB': 'A', 'SB': 'S', 'A': 'A', 'S': 'S'},
    'RI': {'HB': 'H', 'SB': 'S', 'H': 'H', 'S': 'S'},
    'SC': {'HB': 'H', 'SB': 'S', 'H': 'H', 'S': 'S'},
    'VT': {'HB': 'H', 'SB': 'S', 'H': 'H', 'S': 'S'},
    # B-added state (LegiScan uses HB/SB, ACLU uses H/S)
    'KS': {'H': 'HB', 'S': 'SB', 'HB': 'HB', 'SB': 'SB'},
    # Non-standard prefixes
    'CA': {'AB': 'AB', 'SB': 'SB'},
    'NV': {'AB': 'AB', 'SB': 'SB'},
    'WI': {'AB': 'AB', 'SB': 'SB'},
    'IA': {'HF': 'HF', 'SF': 'SF', 'HSB': 'HSB', 'SSB': 'SSB'},
    'MN': {'HF': 'HF', 'SF': 'SF'},
    'NE': {'LB': 'LB'},
    'ME': {'HP': 'HP', 'LD': 'LD'},
    'WY': {'HB': 'HB', 'SF': 'SF'},
}
```

### Step 3: Label Classification
ACLU bills default to `harmful`. Reclassified as `supportive` if the `issues` field contains patterns like:
- `LGBTQ Equality Bills`
- `Nondiscrimination protections`
- `Allowing updated gender markers on ID`
- `Protections in healthcare`

### Step 4: Status Normalization
Plural status values mapped to ACLU-style labels, then to LegiScan numeric codes in Step 8b:

| Plural Status | Normalized Text | LegiScan Code |
|---|---|---|
| INTRODUCED, REFERRED TO COMMITTEE | Introduced | 1 |
| ENGROSSED, PASSED UPPER, PASSED LOWER | Advancing | 2 |
| PASSED, ENROLLED | Passed | 4 |
| SIGNED, SIGNED BY GOVERNOR, BECAME LAW | Signed | 4 |
| FAILED, DEAD | Failed | 6 |
| VETOED | Vetoed | 5 |

### Step 5: Deduplication & Merge
1. Deduplicate within each source on `(state, bill_number)`
2. Concatenate ACLU + Plural
3. Priority: ACLU first, then Plural for overlaps

### Step 6: Status Alignment (Step 8b)
ACLU/Plural text statuses are mapped to LegiScan numeric codes. Original text preserved as `status_desc`, numeric code stored as `status`.

## Matching Against LegiScan (`03_bills_data_prep_eda.ipynb`)

The merged CSV is matched against LegiScan bulk data using a two-pass approach:
1. **Pass 1**: Exact match on `(state, session_year, bill_number)`
2. **Pass 2**: For unmatched, fuzzy match on `(state, bill_number)` across years

### Status
LegiScan uses numeric status codes (0-6). No boolean columns — consumers use `status` directly:
- `status = 4` → Passed
- `status = 5` → Vetoed
- `status = 6` → Failed

### Output
`matched_lgbtq_bills_with_duplicates.csv` → `relevance_and_stance_classifiers_final.ipynb` → final `matched_lgbtq_bills.csv`

## Pipeline Flow

```
ACLU CSVs (2021-2026)  ──┐
                          ├── 02_concat ──→ all_lgbtq_bills_merged.csv
Plural CSV (2024-2025) ──┘       (status normalized + numeric codes)
                                │
                                ▼
                          03_data_prep ──→ matched_lgbtq_bills_with_duplicates.csv
                          (match against    (with LegiScan metadata,
                           LegiScan bulk)    computed political features)
                                │
                                ▼
                          relevance_classifier ──→ matched_lgbtq_bills.csv
                          (resolve duplicates      (final, deduplicated)
                           via Legal-BERT)
                                │
                                ▼
                          graph/make ingest ──→ Neo4j Bill nodes
```

## Known Issues & Fixes

- **Florida zero-padding** (fixed): LegiScan uses `H0731`, ACLU has `H731`. Added 4-digit zero-padding for FL.
- **Kansas prefix** (fixed): LegiScan uses `HB2071`, ACLU has `H2071`. Updated `STATE_PREFIX_MAP` to map `H → HB`, `S → SB`. Removed KS from `h_s_states`.
- **North Dakota**: Uses number-only format (`1577`) — not currently in ACLU/Plural data, so untested.
- **DC**: 0 matched bills — LegiScan tracks DC but ACLU/Plural may not include DC bills.
