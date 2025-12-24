// ============================================================
// Summary_CAGID Aggregation Query
// ============================================================
// Purpose: Aggregate Transaction data by (cagid, cob_date)
//          and create Summary_CAGID nodes
// Method:  Uses apoc.periodic.iterate for batch processing
// ============================================================

CALL apoc.periodic.iterate(
  // Driver Query: Get all distinct (cagid, cob_date) pairs
  "MATCH (t:Transaction)
   WHERE t.cagid IS NOT NULL AND t.cob_date IS NOT NULL
   RETURN DISTINCT t.cagid AS cagid, t.cob_date AS cob_date",

  // Execution Query: Aggregate and MERGE for each pair
  "MATCH (t:Transaction)
   WHERE t.cagid = cagid AND t.cob_date = cob_date
   WITH
     t.cagid AS cagid,
     t.cob_date AS cob_date,

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
     sum(toFloat(t.fy1974_loss_wwr_amount)) AS fy1974_loss_wwr_amount,

     // ========== FY2008 Fields ==========
     sum(toFloat(t.fy2008_10d_margined_amount)) AS fy2008_10d_margined_amount,
     sum(toFloat(t.fy2008_1y_unmargined_amount)) AS fy2008_1y_unmargined_amount,
     sum(toFloat(t.fy2008_exp_amount)) AS fy2008_exp_amount,
     sum(toFloat(t.fy2008_exp_cp_amount)) AS fy2008_exp_cp_amount,
     sum(toFloat(t.fy2008_si_amount)) AS fy2008_si_amount,
     sum(toFloat(t.fy2008_loss_wo_wwr_amount)) AS fy2008_loss_wo_wwr_amount,
     sum(toFloat(t.fy2008_loss_wwr_amount)) AS fy2008_loss_wwr_amount,

     // ========== BHC Light Fields ==========
     sum(toFloat(t.bhc_light_nse_citi_amount)) AS bhc_light_nse_citi_amount,
     sum(toFloat(t.bhc_light_nse_cp_amount)) AS bhc_light_nse_cp_amount,
     sum(toFloat(t.bhc_light_si_amount)) AS bhc_light_si_amount,
     sum(toFloat(t.bhc_light_loss_wo_wwr_amount)) AS bhc_light_loss_wo_wwr_amount,
     sum(toFloat(t.bhc_light_loss_wwr_amount)) AS bhc_light_loss_wwr_amount,

     // ========== Commodity Fields ==========
     sum(toFloat(t.cmdty_up_1pct_si_amount)) AS cmdty_up_1pct_si_amount,
     sum(toFloat(t.cmdty_vol_up_1pct_si_amount)) AS cmdty_vol_up_1pct_si_amount,
     sum(toFloat(t.cr_up_1bps_si_amount)) AS cr_up_1bps_si_amount,

     // ========== Deep Down Fields ==========
     sum(toFloat(t.deep_down_10d_margined_amount)) AS deep_down_10d_margined_amount,
     sum(toFloat(t.deep_down_1y_unmargined_amount)) AS deep_down_1y_unmargined_amount,
     sum(toFloat(t.deep_down_exp_amount)) AS deep_down_exp_amount,
     sum(toFloat(t.deep_down_exp_cp_amount)) AS deep_down_exp_cp_amount,
     sum(toFloat(t.deep_downturn_si_amount)) AS deep_downturn_si_amount,
     sum(toFloat(t.deep_down_loss_wo_wwr_amount)) AS deep_down_loss_wo_wwr_amount,
     sum(toFloat(t.deep_down_loss_wwr_amount)) AS deep_down_loss_wwr_amount,

     // ========== Dollar Decline Fields ==========
     sum(toFloat(t.dollar_decline_10d_margined_amount)) AS dollar_decline_10d_margined_amount,
     sum(toFloat(t.dollar_decline_1y_unmargined_amount)) AS dollar_decline_1y_unmargined_amount,
     sum(toFloat(t.dollar_decline_exp_amount)) AS dollar_decline_exp_amount,
     sum(toFloat(t.dollar_decline_exp_cp_amount)) AS dollar_decline_exp_cp_amount,
     sum(toFloat(t.dollar_decline_si_amount)) AS dollar_decline_si_amount,
     sum(toFloat(t.dollar_decline_loss_wo_wwr_amount)) AS dollar_decline_loss_wo_wwr_amount,
     sum(toFloat(t.dollar_decline_loss_wwr_amount)) AS dollar_decline_loss_wwr_amount,

     // ========== EM Crisis Fields ==========
     sum(toFloat(t.em_crisis_exp_amount)) AS em_crisis_exp_amount,
     sum(toFloat(t.em_crisis_exp_counter_party_amount)) AS em_crisis_exp_counter_party_amount,
     sum(toFloat(t.em_crisis_si_amount)) AS em_crisis_si_amount,
     sum(toFloat(t.em_crisis_loss_wo_wwr_amount)) AS em_crisis_loss_wo_wwr_amount,
     sum(toFloat(t.em_crisis_loss_wwr_amount)) AS em_crisis_loss_wwr_amount,

     // ========== Equity Fields ==========
     sum(toFloat(t.eq_up_1pct_si_amount)) AS eq_up_1pct_si_amount,
     sum(toFloat(t.eq_vol_up_1pct_si_amount)) AS eq_vol_up_1pct_si_amount,
     sum(toFloat(t.eq_rally_exp_amount)) AS eq_rally_exp_amount,
     sum(toFloat(t.eq_rally_exp_cp_amount)) AS eq_rally_exp_cp_amount,
     sum(toFloat(t.eq_rally_si_amount)) AS eq_rally_si_amount,

     // ========== Euro Crisis Fields ==========
     sum(toFloat(t.euro_crisis_exp_amount)) AS euro_crisis_exp_amount,
     sum(toFloat(t.euro_crisis_exp_cp_amount)) AS euro_crisis_exp_cp_amount,
     sum(toFloat(t.euro_crisis_si_amount)) AS euro_crisis_si_amount,
     sum(toFloat(t.euro_crisis_loss_wo_wwr_amount)) AS euro_crisis_loss_wo_wwr_amount,

     // ========== Bear Flat FX Down Fields ==========
     sum(toFloat(t.bear_flt_fxdown_si_amount)) AS bear_flt_fxdown_si_amount,
     sum(toFloat(t.bear_flt_fxdown_exp_amount)) AS bear_flt_fxdown_exp_amount,
     sum(toFloat(t.bear_flt_fxdown_exp_cp_amount)) AS bear_flt_fxdown_exp_cp_amount

     // NOTE: Full query has ~270 aggregation fields
     // This is a condensed version with representative fields

   // MERGE Summary_CAGID node
   MERGE (s:Summary_CAGID {cagid: cagid, cob_date: cob_date})

   ON CREATE SET s += {
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
     fy1974_loss_wwr_amount: fy1974_loss_wwr_amount,
     // FY2008 Fields
     fy2008_10d_margined_amount: fy2008_10d_margined_amount,
     fy2008_1y_unmargined_amount: fy2008_1y_unmargined_amount,
     fy2008_exp_amount: fy2008_exp_amount,
     fy2008_exp_cp_amount: fy2008_exp_cp_amount,
     fy2008_si_amount: fy2008_si_amount,
     fy2008_loss_wo_wwr_amount: fy2008_loss_wo_wwr_amount,
     fy2008_loss_wwr_amount: fy2008_loss_wwr_amount,
     // ... (other fields follow same pattern)
     bear_flt_fxdown_exp_cp_amount: bear_flt_fxdown_exp_cp_amount
   }

   ON MATCH SET s += {
     // Same fields as ON CREATE SET
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
     accrued_interest: accrued_interest,
     citi_payable_cash: citi_payable_cash,
     citi_receivable_cash: citi_receivable_cash,
     citi_receivable_security: citi_receivable_security,
     fy1974_10d_margined_amount: fy1974_10d_margined_amount,
     fy1974_1y_unmargined_amount: fy1974_1y_unmargined_amount,
     fy1974_exp_amount: fy1974_exp_amount,
     fy1974_exp_counter_party_amount: fy1974_exp_counter_party_amount,
     fy1974_si_amount: fy1974_si_amount,
     fy1974_loss_wo_wwr_amount: fy1974_loss_wo_wwr_amount,
     fy1974_loss_wwr_amount: fy1974_loss_wwr_amount,
     fy2008_10d_margined_amount: fy2008_10d_margined_amount,
     fy2008_1y_unmargined_amount: fy2008_1y_unmargined_amount,
     fy2008_exp_amount: fy2008_exp_amount,
     fy2008_exp_cp_amount: fy2008_exp_cp_amount,
     fy2008_si_amount: fy2008_si_amount,
     fy2008_loss_wo_wwr_amount: fy2008_loss_wo_wwr_amount,
     fy2008_loss_wwr_amount: fy2008_loss_wwr_amount,
     // ... (other fields follow same pattern)
     bear_flt_fxdown_exp_cp_amount: bear_flt_fxdown_exp_cp_amount
   }",

  // Batch configuration
  {batchSize: 500, parallel: true}
)