// ============================================================
// Summary_GFCID Aggregation Query
// ============================================================
// Purpose: Aggregate Transaction data by (gfcid, cob_date)
//          and create Summary_GFCID nodes
// Method:  Uses apoc.periodic.iterate for batch processing
// ============================================================

CALL apoc.periodic.iterate(
  // Driver Query: Get all distinct (gfcid, cob_date) pairs
  "MATCH (t:Transaction)
   WHERE t.gfcid IS NOT NULL AND t.cob_date IS NOT NULL
   RETURN DISTINCT t.gfcid AS gfcid, t.cob_date AS cob_date",

  // Execution Query: Aggregate and MERGE for each pair
  "MATCH (t:Transaction)
   WHERE t.gfcid = gfcid AND t.cob_date = cob_date
   WITH
     t.cagid AS cagid,
     t.gfcid AS gfcid,
     t.cob_date AS cob_date,
     t.obligor_name AS obligor_name,

     // ========== FS Amount Fields ==========
     sum(toFloat(t.cmdl_fs_amount)) AS cmdl_fs_amount,
     sum(toFloat(t.cmvg_fs_amount)) AS cmvg_fs_amount,
     sum(toFloat(t.crdl_fs_amount)) AS crdl_fs_amount,
     sum(toFloat(t.eqdl_fs_amount)) AS eqdl_fs_amount,
     sum(toFloat(t.equl_fs_amount)) AS equl_fs_amount,
     sum(toFloat(t.eqvg_fs_amount)) AS eqvg_fs_amount,
     sum(toFloat(t.fxdl_fs_amount)) AS fxdl_fs_amount,
     sum(toFloat(t.fxvg_fs_amount)) AS fxvg_fs_amount,
     sum(toFloat(t.irdl_fs_amount)) AS irdl_fs_amount,
     sum(toFloat(t.irvg_fs_amount)) AS irvg_fs_amount,
     sum(toFloat(t.j2df_fs_amount)) AS j2df_fs_amount,
     sum(toFloat(t.lodl_fs_amount)) AS lodl_fs_amount,
     sum(toFloat(t.omdl_fs_amount)) AS omdl_fs_amount,
     sum(toFloat(t.osdl_fs_amount)) AS osdl_fs_amount,

     // ========== Interest & Cash Fields ==========
     sum(toFloat(t.accrued_interest)) AS accrued_interest,
     sum(toFloat(t.citi_payable_cash)) AS citi_payable_cash,
     sum(toFloat(t.citi_receivable_cash)) AS citi_receivable_cash,
     sum(toFloat(t.citi_receivable_security)) AS citi_receivable_security,

     // ========== FY1974 Fields ==========
     sum(toFloat(t.fy1974_10d_margined_amount)) AS fy1974_10d_margined_amount,
     sum(toFloat(t.fy1974_1y_unmargined_amount)) AS fy1974_1y_unmargined_amount,
     sum(toFloat(t.fy1974_exp_amount)) AS fy1974_exp_amount,
     sum(toFloat(t.fy1974_exp_counter_party_amount)) AS fy1974_exp_counter_party_amount,
     sum(toFloat(t.fy1974_si_amount)) AS fy1974_si_amount,
     sum(toFloat(t.fy1974_loss_wo_wwr_amount)) AS fy1974_loss_wo_wwr_amount,

     // ========== Bull/Bear FX Down Fields ==========
     sum(toFloat(t.bull_flt_fxdown_exp_amount)) AS bull_flt_fxdown_exp_amount,
     sum(toFloat(t.bull_flt_fxdown_exp_cp_amount)) AS bull_flt_fxdown_exp_cp_amount,
     sum(toFloat(t.bear_stp_fxdown_si_amount)) AS bear_stp_fxdown_si_amount,
     sum(toFloat(t.bear_stp_fxdown_exp_amount)) AS bear_stp_fxdown_exp_amount,
     sum(toFloat(t.bear_stp_fxdown_exp_cp_amount)) AS bear_stp_fxdown_exp_cp_amount,
     sum(toFloat(t.bear_flt_fxdown_si_amount)) AS bear_flt_fxdown_si_amount,
     sum(toFloat(t.bear_flt_fxdown_exp_amount)) AS bear_flt_fxdown_exp_amount,
     sum(toFloat(t.bear_flt_fxdown_exp_cp_amount)) AS bear_flt_fxdown_exp_cp_amount

     // NOTE: Full query has ~270 aggregation fields
     // This is a condensed version with representative fields

   // MERGE Summary_GFCID node
   MERGE (s:Summary_GFCID {gfcid: gfcid, cob_date: cob_date})

   ON CREATE SET s += {
     // Identity Fields
     cagid: cagid,
     obligor_name: obligor_name,
     // FS Amount Fields
     cmdl_fs_amount: cmdl_fs_amount,
     cmvg_fs_amount: cmvg_fs_amount,
     crdl_fs_amount: crdl_fs_amount,
     eqdl_fs_amount: eqdl_fs_amount,
     equl_fs_amount: equl_fs_amount,
     eqvg_fs_amount: eqvg_fs_amount,
     fxdl_fs_amount: fxdl_fs_amount,
     fxvg_fs_amount: fxvg_fs_amount,
     irdl_fs_amount: irdl_fs_amount,
     irvg_fs_amount: irvg_fs_amount,
     j2df_fs_amount: j2df_fs_amount,
     lodl_fs_amount: lodl_fs_amount,
     omdl_fs_amount: omdl_fs_amount,
     osdl_fs_amount: osdl_fs_amount,
     // Interest & Cash Fields
     accrued_interest: accrued_interest,
     citi_payable_cash: citi_payable_cash,
     citi_receivable_cash: citi_receivable_cash,
     citi_receivable_security: citi_receivable_security,
     // FY1974 Fields
     fy1974_10d_margined_amount: fy1974_10d_margined_amount,
     fy1974_1y_unmargined_amount: fy1974_1y_unmargined_amount,
     fy1974_exp_amount: fy1974_exp_amount,
     fy1974_exp_counter_party_amount: fy1974_exp_counter_party_amount,
     fy1974_si_amount: fy1974_si_amount,
     fy1974_loss_wo_wwr_amount: fy1974_loss_wo_wwr_amount,
     // Bull/Bear FX Down Fields
     bull_flt_fxdown_exp_amount: bull_flt_fxdown_exp_amount,
     bull_flt_fxdown_exp_cp_amount: bull_flt_fxdown_exp_cp_amount,
     bear_stp_fxdown_si_amount: bear_stp_fxdown_si_amount,
     bear_stp_fxdown_exp_amount: bear_stp_fxdown_exp_amount,
     bear_stp_fxdown_exp_cp_amount: bear_stp_fxdown_exp_cp_amount,
     bear_flt_fxdown_si_amount: bear_flt_fxdown_si_amount,
     bear_flt_fxdown_exp_amount: bear_flt_fxdown_exp_amount,
     bear_flt_fxdown_exp_cp_amount: bear_flt_fxdown_exp_cp_amount
   }

   ON MATCH SET s += {
     // Identity Fields
     cagid: cagid,
     obligor_name: obligor_name,
     // FS Amount Fields
     cmdl_fs_amount: cmdl_fs_amount,
     cmvg_fs_amount: cmvg_fs_amount,
     crdl_fs_amount: crdl_fs_amount,
     eqdl_fs_amount: eqdl_fs_amount,
     equl_fs_amount: equl_fs_amount,
     eqvg_fs_amount: eqvg_fs_amount,
     fxdl_fs_amount: fxdl_fs_amount,
     fxvg_fs_amount: fxvg_fs_amount,
     irdl_fs_amount: irdl_fs_amount,
     irvg_fs_amount: irvg_fs_amount,
     j2df_fs_amount: j2df_fs_amount,
     lodl_fs_amount: lodl_fs_amount,
     omdl_fs_amount: omdl_fs_amount,
     osdl_fs_amount: osdl_fs_amount,
     // Interest & Cash Fields
     accrued_interest: accrued_interest,
     citi_payable_cash: citi_payable_cash,
     citi_receivable_cash: citi_receivable_cash,
     citi_receivable_security: citi_receivable_security,
     // FY1974 Fields
     fy1974_10d_margined_amount: fy1974_10d_margined_amount,
     fy1974_1y_unmargined_amount: fy1974_1y_unmargined_amount,
     fy1974_exp_amount: fy1974_exp_amount,
     fy1974_exp_counter_party_amount: fy1974_exp_counter_party_amount,
     fy1974_si_amount: fy1974_si_amount,
     fy1974_loss_wo_wwr_amount: fy1974_loss_wo_wwr_amount,
     // Bull/Bear FX Down Fields
     bull_flt_fxdown_exp_amount: bull_flt_fxdown_exp_amount,
     bull_flt_fxdown_exp_cp_amount: bull_flt_fxdown_exp_cp_amount,
     bear_stp_fxdown_si_amount: bear_stp_fxdown_si_amount,
     bear_stp_fxdown_exp_amount: bear_stp_fxdown_exp_amount,
     bear_stp_fxdown_exp_cp_amount: bear_stp_fxdown_exp_cp_amount,
     bear_flt_fxdown_si_amount: bear_flt_fxdown_si_amount,
     bear_flt_fxdown_exp_amount: bear_flt_fxdown_exp_amount,
     bear_flt_fxdown_exp_cp_amount: bear_flt_fxdown_exp_cp_amount
   }",

  // Batch configuration
  {batchSize: 500, parallel: true}
)
