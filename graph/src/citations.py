import re

#IMPORTANT: regex patterns for legislative citations generated with AI - should be manually reviewed and updated as needed
#--------- REGEX PATTERNS ----------------------------------------------
PATTERNS = [
    # ── Federal ────────────────────────────────────────────────────────
    # 42 U.S.C. § 1396a
    ("federal", re.compile(r"\b(\d+)\s+U\.S\.C\.\s*§+\s*([\w\-().]+)\b"), 0.85),
    # Pub. L. No. 117-103
    ("federal", re.compile(r"\bPub\.\s*L\.\s*(?:No\.\s*)?(\d+[\-]\d+)"), 0.9),
    # 42 C.F.R. § 438.6
    ("federal", re.compile(r"\b(\d+)\s+C\.F\.R\.\s*§+\s*([\d.]+)"), 0.9),

    # ── State-specific: distinctive prefix (no § needed) ──────────────
    # Alaska: AS 47.07.020
    ("AK", re.compile(r"\bAS\s+(\d{2}\.\d{2}\.\d{2,4})"), 0.9),
    # Hawaii: HRS §302A-1132
    ("HI", re.compile(r"\bHRS\s*§?\s*([\d\w\-]+)"), 0.8),
    # Illinois: 720 ILCS 5/12-1
    ("IL", re.compile(r"\b(\d+)\s+ILCS\s+([\d]+/[\d\w\-.]+)"), 0.9),
    # Indiana: IC 20-33-13
    ("IN", re.compile(r"\bIC\s+(\d[\d\-]+)"), 0.8),
    # Kansas: K.S.A. 21-5503
    ("KS", re.compile(r"\bK\.S\.A\.\s*([\d\-]+)"), 0.9),
    # Kentucky: KRS 311.732
    ("KY", re.compile(r"\bKRS\s+([\d.]+)"), 0.9),
    # Michigan: MCL 750.520b
    ("MI", re.compile(r"\bMCL\s+([\d.]+\w?)"), 0.9),
    # Missouri: RSMo 191.227 or "436.266, RSMo"
    ("MO", re.compile(r"\bRSMo\s*§?\s*(\d+\.\d+)"), 0.9),
    ("MO", re.compile(r"(\d+\.\d+)\s*,?\s*RSMo\b"), 0.85),
    # Montana: MCA 45-5-625
    ("MT", re.compile(r"\bMCA\s+([\d\-]+)"), 0.85),
    # Nevada: NRS 432B.220
    ("NV", re.compile(r"\bNRS\s+([\d.]+[A-Z]?[\d.]*)"), 0.85),
    # New Hampshire: RSA 135-C:5
    ("NH", re.compile(r"\bRSA\s+([\d\w\-:]+)"), 0.85),
    # North Carolina: G.S. 14-27.21 or G.S. 115C-457.2
    ("NC", re.compile(r"\bG\.S\.\s*([\d\w\-.]+)"), 0.85),
    # North Dakota: NDCC 12.1-20-03
    ("ND", re.compile(r"\bNDCC\s+([\d.\-]+)"), 0.9),
    # New Mexico: "Section 22-13-1 NMSA 1978" or "NMSA 1978, Section 30-9-11"
    ("NM", re.compile(r"(\d+-[\d\-.]+)\s*,?\s*NMSA\s+1978"), 0.9),
    ("NM", re.compile(r"NMSA\s+1978\s*,?\s*Section\s+([\d\-.]+)"), 0.9),
    # Oregon: ORS 163.355
    ("OR", re.compile(r"\bORS\s+([\d.]+)"), 0.85),
    # South Dakota: SDCL 22-22-1
    ("SD", re.compile(r"\bSDCL\s+([\d\-]+)"), 0.9),
    # Washington: RCW 9A.36.080
    ("WA", re.compile(r"\bRCW\s+([\d.]+)"), 0.9),

    # ── State-specific: abbreviation + Code/Stat § ────────────────────
    # Alabama: Ala. Code § 13A-6-62
    ("AL", re.compile(r"\bAla\.\s*Code\s*§+\s*([\w\-().]+)"), 0.9),
    # Arizona: A.R.S. § 13-1401
    ("AZ", re.compile(r"\bA\.R\.S\.\s*§+\s*([\w\-().]+)"), 0.9),
    # Arkansas: Ark. Code Ann. § 5-14-103
    ("AR", re.compile(r"\bArk\.\s*Code\s*(?:Ann\.)?\s*§+\s*([\w\-().]+)"), 0.9),
    # California: Cal. Penal Code § 261
    ("CA", re.compile(r"\bCal\.\s+[\w&.\s]+Code\s*§+\s*([\w\-().]+)"), 0.9),
    # Colorado: C.R.S. § 18-3-402
    ("CO", re.compile(r"\bC\.R\.S\.\s*§+\s*([\w\-().]+)"), 0.9),
    # Connecticut: Conn. Gen. Stat. § 53-21
    ("CT", re.compile(r"\bConn\.\s*Gen\.\s*Stat\.\s*§+\s*([\w\-().]+)"), 0.9),
    # Delaware: Del. Code tit. 11, § 761
    ("DE", re.compile(r"\bDel\.\s*Code\s*(?:Ann\.\s*)?tit\.\s*\d+\s*,?\s*§+\s*([\w\-().]+)"), 0.9),
    # Florida: s. 1000.21, F.S.  or  § 1000.21, Florida Statutes
    ("FL", re.compile(r"(?:§+\s*|s\.\s*)([\d.]+)\s*,?\s*(?:F\.S\.|Florida Statutes)"), 0.9),
    # Georgia: O.C.G.A. § 31-7-1
    ("GA", re.compile(r"O\.C\.G\.A\.\s*§+\s*([\w\-().]+)"), 0.9),
    # Idaho: Idaho Code § 18-1506 or "Section 67-5901, Idaho Code"
    ("ID", re.compile(r"\bIdaho\s+Code\s*§+\s*([\w\-().]+)"), 0.9),
    ("ID", re.compile(r"Section\s+([\d\-]+)\s*,?\s*Idaho\s+Code"), 0.85),
    # Iowa: Iowa Code § 709.1
    ("IA", re.compile(r"\bIowa\s+Code\s*§+\s*([\w\-().]+)"), 0.9),
    # Louisiana: La. R.S. 14:42
    ("LA", re.compile(r"\bLa\.\s*(?:Rev\.\s*Stat\.?|R\.S\.)\s*(?:Ann\.)?\s*§?\s*([\w\-:().]+)"), 0.9),
    # Maine: 17-A M.R.S.A. § 253
    ("ME", re.compile(r"\b(\d[\w\-]*)\s+M\.R\.S\.(?:A\.)?\s*§+\s*([\w\-().]+)"), 0.9),
    # Maryland: Md. Code Ann., Crim. Law § 3-303
    ("MD", re.compile(r"\bMd\.\s*(?:Code\s*Ann\.\s*,?\s*)?[\w.\s]*§+\s*([\w\-().]+)"), 0.9),
    # Massachusetts: M.G.L. c. 265, § 22
    ("MA", re.compile(r"\bM\.G\.L\.\s*c\.\s*(\d+)\s*,?\s*§+\s*([\w\-().]+)"), 0.9),
    # Minnesota: Minn. Stat. § 144.343
    ("MN", re.compile(r"Minn\.\s*Stat\.\s*§+\s*([\w\-().]+)"), 0.9),
    # Mississippi: Miss. Code Ann. § 97-3-65
    ("MS", re.compile(r"\bMiss\.\s*Code\s*(?:Ann\.)?\s*§+\s*([\w\-().]+)"), 0.9),
    # Nebraska: Neb. Rev. Stat. § 28-319
    ("NE", re.compile(r"\bNeb\.\s*Rev\.\s*Stat\.\s*§+\s*([\w\-().]+)"), 0.9),
    # New Jersey: N.J.S.A. 2C:12-1
    ("NJ", re.compile(r"N\.J\.S\.A\.\s*([\w\-:().]+)"), 0.9),
    # New York: N.Y. Penal Law § 130.35
    ("NY", re.compile(r"\bN\.Y\.\s+[\w&.\s]+Law\s*§+\s*([\w\-().]+)"), 0.9),
    # Ohio: R.C. 3313.5316 or ORC § ...
    ("OH", re.compile(r"(?:R\.C\.|ORC)\s*§*\s*([\d.]+)"), 0.85),
    # Oklahoma: Okla. Stat. tit. 21, § 1114
    ("OK", re.compile(r"\bOkla?\.\s*Stat\.?\s*(?:(?:Ann\.\s*)?tit\.\s*\d+\s*,?\s*)?§+\s*([\w\-().]+)"), 0.9),
    # Pennsylvania: 18 Pa.C.S. § 3121
    ("PA", re.compile(r"\b(\d+)\s+Pa\.(?:C\.S\.|Cons\.Stat\.)\s*§+\s*([\w\-().]+)"), 0.9),
    # Rhode Island: R.I. Gen. Laws § 11-37-2
    ("RI", re.compile(r"\bR\.I\.\s*Gen\.\s*Laws\s*§+\s*([\w\-().]+)"), 0.9),
    # South Carolina: S.C. Code Ann. § 16-3-655
    ("SC", re.compile(r"\bS\.C\.\s*Code\s*(?:Ann\.)?\s*§+\s*([\w\-().]+)"), 0.9),
    # Tennessee: T.C.A. § 49-6-310
    ("TN", re.compile(r"T\.C\.A\.\s*§+\s*([\w\-().]+)"), 0.9),
    # Texas: Tex. [Name] Code § 161.001
    ("TX", re.compile(r"Tex(?:as|\.)\s+[\w&\s]+Code\s*§+\s*([\w\-().]+)"), 0.9),
    # Utah: Utah Code § 53G-9-701
    ("UT", re.compile(r"Utah\s+Code\s*§+\s*([\w\-().]+)"), 0.9),
    # Vermont: 13 V.S.A. § 3252
    ("VT", re.compile(r"\b(\d+)\s+V\.S\.A\.\s*§+\s*([\w\-().]+)"), 0.9),
    # Virginia: Va. Code Ann. § 18.2-61
    ("VA", re.compile(r"\bVa\.\s*Code\s*(?:Ann\.)?\s*§+\s*([\w\-().]+)"), 0.9),
    # West Virginia: W. Va. Code § 61-8B-3
    ("WV", re.compile(r"\bW\.\s*Va\.\s*Code\s*§+\s*([\w\-().]+)"), 0.9),
    # Wisconsin: Wis. Stat. § 948.02
    ("WI", re.compile(r"\bWis\.\s*Stat\.\s*§+\s*([\w\-().]+)"), 0.9),
    # Wyoming: Wyo. Stat. Ann. § 6-2-302
    ("WY", re.compile(r"\bWyo\.\s*Stat\.?\s*(?:Ann\.)?\s*§+\s*([\w\-().]+)"), 0.9),

    # ── Generic fallbacks ─────────────────────────────────────────────
    # § 20-2-773 or §§ 20-2-773
    ("state", re.compile(r"§§?\s*(\d[\w\-().]+)"), 0.6),
    # Title X, Chapter Y
    ("state", re.compile(r"Title\s+(\d+)\s*,\s*Chapter\s+(\d+)"), 0.5),
]
#--------- END REGEX PATTERNS ------------------------------------------

# canonicalize citation string by removing spaces and converting to uppercase
def canonicalize(raw: str) -> str:
    return re.sub(r"\s+", "", raw).upper()


# strip trailing punctuation from citation string
def _strip_trailing_punct(s: str) -> str:
    return s.rstrip(".,;:")


# extract citations from text
def extract_citations(text: str) -> list[dict]:
    # keep track of seen citations
    seen = set()
    out = []

    # loop through patterns
    for jurisdiction, pattern, confidence in PATTERNS:
        # loop through matches
        for m in pattern.finditer(text):
            # strip trailing punctuation from match
            raw = _strip_trailing_punct(m.group(0))
            # canonicalize citation string
            canon = canonicalize(raw)

            # skip if citation has already been seen
            if canon in seen:
                continue
            seen.add(canon)

            # add citation to output
            out.append({
                "jurisdiction": jurisdiction,
                "canonical": canon,
                "raw": raw,
                "span_start": m.start(),
                "span_end": m.end(),
                "confidence": confidence,
            })

    return out
