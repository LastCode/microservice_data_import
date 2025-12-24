// ============================================================
// Create Relationships Between Summary Nodes
// ============================================================
// Purpose: Create CONTAINS_GFCID relationships between
//          Summary_CAGID and Summary_GFCID nodes
// ============================================================

// Create relationship: Summary_CAGID -[:CONTAINS_GFCID]-> Summary_GFCID
MATCH (t:Transaction)
WHERE t.cagid IS NOT NULL AND t.gfcid IS NOT NULL AND t.cob_date IS NOT NULL

// Find the unique combinations of cagid, gfcid, and cob_date from transactions
WITH DISTINCT t.cagid AS cagid, t.gfcid AS gfcid, t.cob_date AS cob_date

// Match the corresponding summary nodes
MATCH (sc:Summary_CAGID {cagid: cagid, cob_date: cob_date})
MATCH (sg:Summary_GFCID {gfcid: gfcid, cob_date: cob_date})

// Create the relationship between them
MERGE (sc)-[:CONTAINS_GFCID]->(sg);


// ============================================================
// Additional Relationship Examples (if needed)
// ============================================================

// Create relationship: Summary_GFCID -[:CONTAINS_NETTINGID]-> Summary_NETTINGID
// MATCH (t:Transaction)
// WHERE t.gfcid IS NOT NULL AND t.nettingid IS NOT NULL AND t.cob_date IS NOT NULL
// WITH DISTINCT t.gfcid AS gfcid, t.nettingid AS nettingid, t.cob_date AS cob_date
// MATCH (sg:Summary_GFCID {gfcid: gfcid, cob_date: cob_date})
// MATCH (sn:Summary_NETTINGID {nettingid: nettingid, cob_date: cob_date})
// MERGE (sg)-[:CONTAINS_NETTINGID]->(sn);

// Create relationship: Summary_CAGID -[:CONTAINS_NETTINGID]-> Summary_NETTINGID
// MATCH (t:Transaction)
// WHERE t.cagid IS NOT NULL AND t.nettingid IS NOT NULL AND t.cob_date IS NOT NULL
// WITH DISTINCT t.cagid AS cagid, t.nettingid AS nettingid, t.cob_date AS cob_date
// MATCH (sc:Summary_CAGID {cagid: cagid, cob_date: cob_date})
// MATCH (sn:Summary_NETTINGID {nettingid: nettingid, cob_date: cob_date})
// MERGE (sc)-[:CONTAINS_NETTINGID]->(sn);
