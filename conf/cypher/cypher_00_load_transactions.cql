// ============================================================
// Load Transaction Nodes from CSV
// ============================================================
// Purpose: Load transaction data from CSV file into Neo4j
//          Creates Transaction nodes and links to Summary_GFCID
// Method:  Uses MERGE to prevent duplicates on re-import
// Key:     transaction_id + cob_date (composite unique key)
// Note:    {file_name} placeholder is replaced at runtime
// ============================================================

CALL {{
LOAD CSV WITH HEADERS FROM 'file://{file_name}' AS row
FIELDTERMINATOR ','
WITH row WHERE row.transaction_id IS NOT NULL AND row.gfcid IS NOT NULL AND row.cob_date IS NOT NULL
MATCH (g:Summary_GFCID {{gfcid: row.gfcid}})
MERGE (t:Transaction {{transaction_id: row.transaction_id, cob_date: row.cob_date}})
ON CREATE SET
  t.is_stress_eligible = row.is_stress_eligible,
  t.netting_type = row.netting_type,
  t.uuitid = row.uuitid,
  t.trade_date = row.trade_date,
  t.netting_id = row.netting_id,
  t.mtm_usd_amount = toFloat(row.mtm_usd_amount),
  t.mtm_local_amount = toFloat(row.mtm_local_amount),
  t.mtm_currency_code = row.mtm_currency_code,
  t.gfcid = row.gfcid,
  t.obligor_name = row.obligor_name,
  t.cagid = row.cagid,
  t.cagid_name = row.cagid_name,
  t.dsft_illiquid_market_value = row.dsft_illiquid_market_value,
  t.dsft_liquid_market_value = row.dsft_liquid_market_value,
  t.bear_stp_fxdown_si_amount = toFloat(row.bear_stp_fxdown_si_amount),
  t.bear_stp_fxdown_exp_amount = toFloat(row.bear_stp_fxdown_exp_amount),
  t.bear_stp_fxdown_exp_cp_amount = toFloat(row.bear_stp_fxdown_exp_cp_amount),
  t.bear_flt_fxdown_si_amount = toFloat(row.bear_flt_fxdown_si_amount),
  t.bear_flt_fxdown_exp_amount = toFloat(row.bear_flt_fxdown_exp_amount),
  t.bear_flt_fxdown_exp_cp_amount = toFloat(row.bear_flt_fxdown_exp_cp_amount)
ON MATCH SET
  t.is_stress_eligible = row.is_stress_eligible,
  t.netting_type = row.netting_type,
  t.uuitid = row.uuitid,
  t.trade_date = row.trade_date,
  t.netting_id = row.netting_id,
  t.mtm_usd_amount = toFloat(row.mtm_usd_amount),
  t.mtm_local_amount = toFloat(row.mtm_local_amount),
  t.mtm_currency_code = row.mtm_currency_code,
  t.gfcid = row.gfcid,
  t.obligor_name = row.obligor_name,
  t.cagid = row.cagid,
  t.cagid_name = row.cagid_name,
  t.dsft_illiquid_market_value = row.dsft_illiquid_market_value,
  t.dsft_liquid_market_value = row.dsft_liquid_market_value,
  t.bear_stp_fxdown_si_amount = toFloat(row.bear_stp_fxdown_si_amount),
  t.bear_stp_fxdown_exp_amount = toFloat(row.bear_stp_fxdown_exp_amount),
  t.bear_stp_fxdown_exp_cp_amount = toFloat(row.bear_stp_fxdown_exp_cp_amount),
  t.bear_flt_fxdown_si_amount = toFloat(row.bear_flt_fxdown_si_amount),
  t.bear_flt_fxdown_exp_amount = toFloat(row.bear_flt_fxdown_exp_amount),
  t.bear_flt_fxdown_exp_cp_amount = toFloat(row.bear_flt_fxdown_exp_cp_amount)
MERGE (t)-[:TRANSACTIONS]->(g)
}} IN TRANSACTIONS OF 1000 ROWS
