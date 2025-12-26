// ============================================================
// Load Transaction Nodes from CSV
// ============================================================
// Purpose: Load transaction data from CSV file into Neo4j
//          Creates Transaction nodes and links to Summary_GFCID
// Method:  Uses LOAD CSV with batch transactions
// Note:    {file_name} placeholder is replaced at runtime
// ============================================================

CALL {{
LOAD CSV WITH HEADERS FROM 'file://{file_name}' AS row
FIELDTERMINATOR ','
WITH row WHERE row.transaction_id IS NOT NULL AND row.gfcid IS NOT NULL
MATCH (g:Summary_GFCID {{gfcid: row.gfcid}})
MERGE (t:Transaction {{transaction_id: row.transaction_id}})
ON CREATE SET
    t.gfcid = row.gfcid,
    t.cagid = row.cagid,
    t.obligor_name = row.obligor_name,
    t.cagid_name = row.cagid_name,
    t.uitid = row.uitid,
    t.netting_id = row.netting_id,
    t.netting_type = row.netting_type,
    t.trade_date = row.trade_date,
    t.cob_date = row.cob_date,
    t.is_stress_eligible = row.is_stress_eligible,
    t.mtm_usd_amount = toFloat(row.mtm_usd_amount),
    t.mtm_local_amount = toFloat(row.mtm_local_amount),
    t.mtm_currency_code = row.mtm_currency_code
MERGE (t)-[:TRANSACTIONS]->(g)
}} IN TRANSACTIONS OF 1000 ROWS