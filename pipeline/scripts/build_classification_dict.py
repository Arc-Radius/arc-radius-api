"""
Build bill classification dictionary from ACLU legislation tracker.
Normalizes bill numbers for LegiScan API matching.
"""
import pandas as pd
import re
from pathlib import Path

# State name to abbreviation
STATE_ABBREV = {
    'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR',
    'California': 'CA', 'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE',
    'Florida': 'FL', 'Georgia': 'GA', 'Hawaii': 'HI', 'Idaho': 'ID',
    'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA', 'Kansas': 'KS',
    'Kentucky': 'KY', 'Louisiana': 'LA', 'Maine': 'ME', 'Maryland': 'MD',
    'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN', 'Mississippi': 'MS',
    'Missouri': 'MO', 'Montana': 'MT', 'Nebraska': 'NE', 'Nevada': 'NV',
    'New Hampshire': 'NH', 'New Jersey': 'NJ', 'New Mexico': 'NM', 'New York': 'NY',
    'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH', 'Oklahoma': 'OK',
    'Oregon': 'OR', 'Pennsylvania': 'PA', 'Rhode Island': 'RI', 'South Carolina': 'SC',
    'South Dakota': 'SD', 'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT',
    'Vermont': 'VT', 'Virginia': 'VA', 'Washington': 'WA', 'West Virginia': 'WV',
    'Wisconsin': 'WI', 'Wyoming': 'WY', 'District of Columbia': 'DC'
}


def normalize_bill_number(bill_name: str) -> str:
    """
    Normalize bill number for LegiScan matching.

    Examples:
        'S.350'              -> 'S350'
        'H.B.158'            -> 'HB158'
        'S.F.473'            -> 'SF473'  
        'L.D. 1134 (S.P. 461)' -> 'LD1134'
        'H.B. 229'           -> 'HB229'
        'H.C.R.2042'         -> 'HCR2042'
        'S.B.0009'           -> 'SB9' (strip leading zeros)
    """
    if pd.isna(bill_name):
        return None

    # Handle Maine's special format: 'L.D. 1134 (S.P. 461)' -> 'LD1134'
    maine_match = re.match(r'L\.D\.?\s*(\d+)', bill_name)
    if maine_match:
        return f"LD{int(maine_match.group(1))}"  # int() strips leading zeros

    # General normalization
    normalized = bill_name.upper()
    normalized = re.sub(r'\(.*?\)', '', normalized)  # Remove parentheticals
    normalized = re.sub(r'\.', '', normalized)       # Remove periods
    normalized = re.sub(r'\s+', '', normalized)      # Remove spaces

    # Strip leading zeros from bill number: 'SB0009' -> 'SB9'
    match = re.match(r'([A-Z]+)0*(\d+)', normalized)
    if match:
        prefix, number = match.groups()
        normalized = f"{prefix}{number}"

    return normalized.strip()


def extract_year(status_date: str) -> int:
    """Extract year from MM/DD/YYYY format."""
    if pd.isna(status_date):
        return 2025  # Default for this dataset
    match = re.search(r'(\d{4})', str(status_date))
    return int(match.group(1)) if match else 2025


def categorize_issues(issues_str: str) -> list:
    """Map issue categories to broader labels for analysis."""
    if pd.isna(issues_str):
        return ['other']

    categories = set()
    issues_lower = issues_str.lower()

    if any(x in issues_lower for x in ['healthcare', 'medical']):
        categories.add('healthcare')
    if any(x in issues_lower for x in ['school', 'student', 'educator']):
        categories.add('education')
    if 'sports' in issues_lower:
        categories.add('sports')
    if 'facilities' in issues_lower or 'bathroom' in issues_lower:
        categories.add('facilities')
    if 'religious' in issues_lower:
        categories.add('religious_exemption')
    if any(x in issues_lower for x in ['curriculum', 'outing', 'don\'t say']):
        categories.add('schools_speech')
    if any(x in issues_lower for x in ['id', 'definition of sex', 're-definition']):
        categories.add('identity_documents')
    if any(x in issues_lower for x in ['drag', 'expression']):
        categories.add('expression')
    if 'accommodation' in issues_lower:
        categories.add('public_accommodations')

    return list(categories) if categories else ['other']


def build_classification_dict(input_csv: str, output_dir: str = '.'):
    """Build classification dictionary from ACLU tracker CSV."""

    # Load data
    df = pd.read_csv(input_csv)

    # Remove footer row
    df = df[df['State'].notna() & ~df['State'].str.contains(
        'Data is current', na=False)]

    print(f"Loaded {len(df)} bills from {input_csv}")

    # Build records
    records = []
    for _, row in df.iterrows():
        bill_raw = row['Bill Name']
        bill_normalized = normalize_bill_number(bill_raw)
        state_abbrev = STATE_ABBREV.get(row['State'], row['State'][:2].upper())
        year = extract_year(row['Status Date'])

        record = {
            # Core identifiers for LegiScan
            'state': state_abbrev,
            'bill_number': bill_normalized,
            'year': year,

            # Original data for reference
            'state_full': row['State'],
            'bill_number_raw': bill_raw,

            # Status info
            'status': row['Status'],
            'status_detail': row['Status Detail'],

            # Issue categorization
            'issues_raw': row['Issues'],
            'issue_categories': categorize_issues(row['Issues']),

            # Classification label
            'label': 'harmful',  # ALL bills in ACLU tracker are anti-LGBTQ+

            # Data source
            'source': 'aclu_tracker',

            # LegiScan fields (to be filled later)
            'legiscan_bill_id': None,
            'legiscan_text_url': None,
        }
        records.append(record)

    # Create output DataFrame
    out_df = pd.DataFrame(records)

    # Save outputs
    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True)

    # CSV (flat format for easy viewing)
    csv_path = output_dir / 'bill_classification_dict.csv'
    out_df.to_csv(csv_path, index=False)

    # JSON (preserves list fields properly)
    json_path = output_dir / 'bill_classification_dict.json'
    out_df.to_json(json_path, orient='records', indent=2)

    print(f"\nSaved:")
    print(f"  {csv_path}")
    print(f"  {json_path}")

    return out_df


if __name__ == '__main__':
    import sys

    # Get script directory for relative paths
    script_dir = Path(__file__).parent

    # Default input file (in same directory as script)
    default_input = script_dir / 'aclu-legislation-tracker_2026-01-19_19-52.csv'

    input_file = sys.argv[1] if len(sys.argv) > 1 else str(default_input)
    output_dir = sys.argv[2] if len(sys.argv) > 2 else str(script_dir)

    df = build_classification_dict(input_file, output_dir)

    # Print summary
    print("\n" + "=" * 60)
    print("CLASSIFICATION DICTIONARY SUMMARY")
    print("=" * 60)
    print(f"Total bills: {len(df)}")
    print(f"States: {df['state'].nunique()}")
    print(f"All labeled: harmful")
    print(f"\nStatus breakdown:")
    print(df['status'].value_counts().to_string())

    print("\n" + "=" * 60)
    print("BILL NUMBER NORMALIZATION SAMPLES")
    print("=" * 60)
    samples = df[['bill_number_raw', 'bill_number']].drop_duplicates().head(20)
    for _, r in samples.iterrows():
        print(f"  '{r['bill_number_raw']:25s}' -> '{r['bill_number']}'")
