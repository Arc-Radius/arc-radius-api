"""Parameterized read Cypher for UI endpoints (see API_RESPONSE_ENTITIES_CYPHER.md)."""

STATES_LIST = """
MATCH (s:State)
OPTIONAL MATCH (b:Bill)-[:IN_STATE]->(s)
WITH s, collect(b) AS bills
WITH
  s,
  size([x IN bills WHERE x IS NOT NULL AND NOT (
        coalesce(x.passed, false) = true OR toLower(trim(toString(coalesce(x.passed, '')))) IN ['true', '1', 'yes']
      )]) AS activeBills,
  size([x IN bills WHERE x IS NOT NULL AND (
        coalesce(x.passed, false) = true OR toLower(trim(toString(coalesce(x.passed, '')))) IN ['true', '1', 'yes']
      )]) AS passedBills,
  size([x IN bills WHERE x IS NOT NULL AND x.label = 'supportive']) AS supportiveCount,
  size([x IN bills WHERE x IS NOT NULL AND x.label = 'harmful']) AS harmfulCount
OPTIONAL MATCH (sample:Bill)-[:IN_STATE]->(s)
WHERE sample.state_link IS NOT NULL AND trim(sample.state_link) <> ''
WITH s, activeBills, passedBills, supportiveCount, harmfulCount, sample
ORDER BY sample.last_action_date DESC
WITH s, activeBills, passedBills, supportiveCount, harmfulCount, collect(sample)[0] AS latest
RETURN {
  abbr: s.code,
  name: s.code,
  status: CASE
    WHEN supportiveCount > harmfulCount THEN 'supportive'
    WHEN harmfulCount > supportiveCount THEN 'harmful'
    ELSE 'mixed'
  END,
  legislature: '',
  session: '',
  sessionWindow: '',
  stateLink: coalesce(latest.state_link, ''),
  lastUpdated: coalesce(latest.last_action_date, ''),
  counts: {
    activeBills: activeBills,
    passedBills: passedBills
  }
} AS state
ORDER BY state.abbr
"""

BILLS_LIST_PAGE = """
WITH
  $state AS state,
  coalesce($stances, []) AS stances,
  coalesce($categories, []) AS categories,
  coalesce($years, []) AS years,
  $tab AS tab,
  $sortBy AS sortBy,
  $sortDir AS sortDir,
  $cursorSortValue AS cursorSortValue,
  $cursorBillPk AS cursorBillPk
MATCH (b:Bill)
WHERE b.state = state
  AND (size(stances) = 0 OR b.label IN stances)
  AND (size(years) = 0 OR coalesce(toInteger(b.year), -1) IN years)
  AND (
    tab IS NULL
    OR (tab = 'passed' AND (
          coalesce(b.passed, false) = true OR toLower(toString(b.passed)) IN ['true', '1', 'yes']
        ))
    OR (tab = 'active' AND NOT (
          coalesce(b.passed, false) = true OR toLower(toString(b.passed)) IN ['true', '1', 'yes']
        ))
  )
OPTIONAL MATCH (b)-[:HAS_TOPIC]->(t:Topic)
WITH b, [x IN collect(DISTINCT t.name) WHERE x IS NOT NULL] AS topicNames, categories, sortBy, sortDir,
     cursorSortValue, cursorBillPk
WHERE size(categories) = 0 OR any(cat IN categories WHERE cat IN topicNames)
OPTIONAL MATCH (p:Person)-[sp:SPONSORS]->(b)
WITH b, topicNames, p, sp, sortBy, sortDir, cursorSortValue, cursorBillPk
ORDER BY sp.position ASC, p.people_id ASC
WITH b, topicNames, head(collect(p)) AS primarySponsor, sortBy, sortDir, cursorSortValue, cursorBillPk,
  CASE sortBy
    WHEN 'year' THEN toString(coalesce(toInteger(b.year), 0))
    WHEN 'relevance' THEN toString(coalesce(b.relevance_score, 0.0))
    ELSE coalesce(b.last_action_date, '')
  END AS sortValue
WHERE cursorSortValue IS NULL
   OR (sortDir = 'desc' AND (
        sortValue < cursorSortValue OR (sortValue = cursorSortValue AND b.bill_pk < cursorBillPk)
      ))
   OR (sortDir = 'asc' AND (
        sortValue > cursorSortValue OR (sortValue = cursorSortValue AND b.bill_pk > cursorBillPk)
      ))
WITH b, topicNames, primarySponsor, sortValue, sortBy, sortDir
ORDER BY
  CASE WHEN sortBy = 'lastActionDate' AND sortDir = 'asc'  THEN b.last_action_date END ASC,
  CASE WHEN sortBy = 'lastActionDate' AND sortDir = 'desc' THEN b.last_action_date END DESC,
  CASE WHEN sortBy = 'year' AND sortDir = 'asc'            THEN toInteger(b.year) END ASC,
  CASE WHEN sortBy = 'year' AND sortDir = 'desc'           THEN toInteger(b.year) END DESC,
  CASE WHEN sortBy = 'relevance' AND sortDir = 'asc'       THEN b.relevance_score END ASC,
  CASE WHEN sortBy = 'relevance' AND sortDir = 'desc'      THEN b.relevance_score END DESC,
  b.bill_pk DESC
LIMIT $fetchLimit
RETURN collect({
  id: b.bill_pk,
  bill_number: coalesce(b.bill_number, ''),
  title: coalesce(b.title, ''),
  description: coalesce(b.description, ''),
  stance: coalesce(b.label, 'mixed'),
  billTab: CASE WHEN coalesce(b.passed, false) = true
             OR toLower(toString(b.passed)) IN ['true', '1', 'yes'] THEN 'passed' ELSE 'active' END,
  status: coalesce(toString(b.status), ''),
  status_desc: coalesce(b.status_desc, ''),
  last_action: coalesce(b.last_action, ''),
  last_action_date: coalesce(b.last_action_date, ''),
  year: coalesce(toInteger(b.year), 0),
  primary_sponsor: coalesce(primarySponsor.name, ''),
  issue_categories: topicNames,
  url: b.url,
  relevance: b.relevance_score,
  _sortValue: sortValue
}) AS rows
"""

BILLS_FACETS = """
WITH
  $state AS state,
  coalesce($stances, []) AS stances,
  coalesce($categories, []) AS categories,
  coalesce($years, []) AS years,
  $tab AS tab
MATCH (b:Bill)
WHERE b.state = state
  AND (size(stances) = 0 OR b.label IN stances)
  AND (size(years) = 0 OR coalesce(toInteger(b.year), -1) IN years)
  AND (
    tab IS NULL
    OR (tab = 'passed' AND (
          coalesce(b.passed, false) = true OR toLower(toString(b.passed)) IN ['true', '1', 'yes']
        ))
    OR (tab = 'active' AND NOT (
          coalesce(b.passed, false) = true OR toLower(toString(b.passed)) IN ['true', '1', 'yes']
        ))
  )
OPTIONAL MATCH (b)-[:HAS_TOPIC]->(t:Topic)
WITH b, [x IN collect(DISTINCT t.name) WHERE x IS NOT NULL] AS topicNames, categories
WHERE size(categories) = 0 OR any(cat IN categories WHERE cat IN topicNames)
WITH collect({b:b, topics:topicNames}) AS rows
WITH rows, size(rows) AS totalCount
UNWIND rows AS r_st
WITH rows, totalCount,
  CASE coalesce(r_st.b.label, '')
    WHEN 'supportive' THEN 'supportive'
    WHEN 'harmful' THEN 'harmful'
    ELSE 'mixed'
  END AS stanceKey
WITH rows, totalCount, stanceKey, count(*) AS scnt
WITH rows, totalCount, collect([stanceKey, scnt]) AS stancePairs
UNWIND rows AS r
WITH totalCount, stancePairs, toString(coalesce(toInteger(r.b.year), 0)) AS yk
WITH stancePairs, totalCount, yk, count(*) AS ycnt
WITH stancePairs, totalCount, collect([yk, ycnt]) AS yearPairs
RETURN {
  stancePairs: stancePairs,
  totalCount: totalCount,
  yearPairs: yearPairs
} AS facets
"""

# Same filters as BILLS_FACETS; dynamic topic keys cannot use map literals — return rows for Python dict.
BILLS_FACET_CATEGORY_COUNTS = """
WITH
  $state AS state,
  coalesce($stances, []) AS stances,
  coalesce($categories, []) AS categories,
  coalesce($years, []) AS years,
  $tab AS tab
MATCH (b:Bill)
WHERE b.state = state
  AND (size(stances) = 0 OR b.label IN stances)
  AND (size(years) = 0 OR coalesce(toInteger(b.year), -1) IN years)
  AND (
    tab IS NULL
    OR (tab = 'passed' AND (
          coalesce(b.passed, false) = true OR toLower(toString(b.passed)) IN ['true', '1', 'yes']
        ))
    OR (tab = 'active' AND NOT (
          coalesce(b.passed, false) = true OR toLower(toString(b.passed)) IN ['true', '1', 'yes']
        ))
  )
OPTIONAL MATCH (b)-[:HAS_TOPIC]->(t:Topic)
WITH b, [x IN collect(DISTINCT t.name) WHERE x IS NOT NULL] AS topicNames, categories
WHERE size(categories) = 0 OR any(cat IN categories WHERE cat IN topicNames)
WITH collect({b:b, topics:topicNames}) AS rows
UNWIND rows AS r
UNWIND coalesce(r.topics, []) AS cat
RETURN cat AS category, count(*) AS cnt
"""

BILL_DETAIL = """
MATCH (b:Bill)
WHERE b.bill_pk = $billPk OR ($numericOnly = true AND toString(b.bill_id) = $billPk)
WITH b ORDER BY b.bill_pk LIMIT 1
OPTIONAL MATCH (p:Person)-[sp:SPONSORS]->(b)
WITH b, p, sp ORDER BY sp.position ASC, p.people_id ASC
WITH b, collect({
  name: coalesce(p.name, ''),
  party: CASE
    WHEN p.party IN ['D', 'R', 'I'] THEN p.party
    ELSE 'I'
  END
}) AS sponsors
OPTIONAL MATCH (b)-[:HAS_ACTION]->(a:Action)
WITH b, sponsors, a ORDER BY coalesce(a.sequence, 0) ASC
WITH b, sponsors, collect({
  date: coalesce(a.date, ''),
  chamber: coalesce(a.chamber, ''),
  action: coalesce(a.action, '')
}) AS history
OPTIONAL MATCH (b)-[:REFERRED_TO]->(cm:Committee)
WITH b, sponsors, history, head(collect(DISTINCT cm.name)) AS pendingCommittee
OPTIONAL MATCH (b)-[:HAS_TOPIC]->(t:Topic)
WITH b, sponsors, history, pendingCommittee, [x IN collect(DISTINCT t.name) WHERE x IS NOT NULL] AS subjects
RETURN {
  bill: {
    id: b.bill_pk,
    number: coalesce(b.bill_number, ''),
    title: coalesce(b.title, ''),
    summary: coalesce(b.description, ''),
    fullText: '',
    state: coalesce(b.state, ''),
    status: coalesce(toString(b.status), ''),
    progression: 0.0,
    lastAction: coalesce(b.last_action, ''),
    lastActionDate: coalesce(b.last_action_date, ''),
    pendingCommittee: coalesce(pendingCommittee, ''),
    sponsors: sponsors,
    spectrum: CASE
      WHEN b.label = 'supportive' THEN 'Supportive'
      WHEN b.label = 'harmful' THEN 'Harmful'
      ELSE 'Neutral'
    END,
    introducedDate: '',
    history: history,
    subjects: subjects,
    similarBills: [],
    relatedBills: [],
    keyDates: [],
    aiAnalysis: null,
    researchEvidence: null,
    sponsorContact: null,
    billTab: CASE WHEN coalesce(b.passed, false) = true
             OR toLower(toString(b.passed)) IN ['true', '1', 'yes'] THEN 'passed' ELSE 'active' END
  },
  graphRecord: properties(b)
} AS payload
"""
