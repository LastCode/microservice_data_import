#!/usr/bin/env python3
"""
CREATE INDEX gfcid_gfcid_index IF NOT EXISTS FOR (n:Summary_GFCID) ON (n.gfcid);

# This setting specifies a directory from which CSV files can be imported using
# LOAD CSV. It is disabled by default.
dbms.directories.import=/mnt/nas

1. Restart your Neo4j server for the changes to take effect.
2. UPDATE COBs in line 38, 39, 167,
3. UPDATE Base directory path if needed
python3 import_transactions.py
"""

import csv
import logging
import multiprocessing
import os
import re
import sys
from typing import Iterable, Set

from neo4j import GraphDatabase

# --- Configuration ---

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Neo4j Connection Details
#NEO4J_URI = "bolt://sd-fb5e-ceca.nam.nsroot.net:7687"  # Or your Neo4j Aura URI
#NEO4J_USER = "as28185"
#NEO4J_PASSWORD = "Nass@123"  # Replace with your password
#NEO4J_DATABASE = "datalineageas28185"  # Default database name

NEO4J_URI = "bolt://sd-fb5e-ceca.nam.nsroot.net:7687"  # Or your Neo4j Aura URI
NEO4J_USER = "as28185"
NEO4J_PASSWORD = "Nass@123"  # Replace with your password
NEO4J_DATABASE = "datalineageas28185"  # Default database name

# Base directories where the split transaction files are located
BASE_DIRECTORIES = [
    #"/mnt/nas/20251106/split",
    "/mnt/nas/20251107/split",
]

# --- Cypher Query Template ---
# This query is executed for each transaction file.
# It uses PERIODIC COMMIT for memory efficiency.
CYPHER_QUERY_TEMPLATE = """
CALL {{
LOAD CSV WITH HEADERS FROM 'file:///{file_name}' AS row
FIELDTERMINATOR ','
MATCH (g:Summary_GFCID {{gfcid: row.gfcid}})
MERGE (t:Transaction {{
    is_stress_eligible: row.is_stress_eligible,
    netting_type: row.netting_type,
    transaction_id: row.transaction_id,
    uitid: row.uitid,
    trade_date: row.trade_date,
    netting_id: row.netting_id,
    mtm_usd_amount: toFloat(row.mtm_usd_amount),
    mtm_local_amount: toFloat(row.mtm_local_amount),
    mtm_currency_code: row.mtm_currency_code,
    gfcid: row.gfcid,
    obligor_name: row.obligor_name,
    cagid: row.cagid,
    cagid_name: row.cagid_name,
    dsft_illiquid_market_value: row.dsft_illiquid_market_value,
    dsft_liquid_market_value: row.dsft_liquid_market_value,
    cob_date: row.cob_date,
    cmdl_fs_amount: toFloat(row.cmdl_fs_amount),
    cmvg_fs_amount: toFloat(row.cmvg_fs_amount),
    crdl_fs_amount: toFloat(row.crdl_fs_amount),
    eqdl_fs_amount: toFloat(row.eqdl_fs_amount),
    equl_fs_amount: toFloat(row.equl_fs_amount),
    eqvg_fs_amount: toFloat(row.eqvg_fs_amount),
    fxdl_fs_amount: toFloat(row.fxdl_fs_amount),
    fxvg_fs_amount: toFloat(row.fxvg_fs_amount),
    irdl_fs_amount: toFloat(row.irdl_fs_amount),
    irvg_fs_amount: toFloat(row.irvg_fs_amount),
    j2df_fs_amount: toFloat(row.j2df_fs_amount),
    lodl_fs_amount: toFloat(row.lodl_fs_amount),
    omdl_fs_amount: toFloat(row.omdl_fs_amount),
    osdl_fs_amount: toFloat(row.osdl_fs_amount),
    accrued_interest: row.accrued_interest,
    citi_payable_cash: row.citi_payable_cash,
    citi_receivable_cash: row.citi_receivable_cash,
    citi_receivable_security: row.citi_receivable_security,
    fy1974_10d_margined_amount: toFloat(row.fy1974_10d_margined_amount),
    fy1974_1y_unmargined_amount: toFloat(row.fy1974_1y_unmargined_amount),
    fy1974_exp_amount: toFloat(row.fy1974_exp_amount),
    fy1974_exp_counter_party_amount: toFloat(row.fy1974_exp_counter_party_amount),
    fy1974_si_amount: toFloat(row.fy1974_si_amount),
    fy1974_loss_wo_wwr_amount: toFloat(row.fy1974_loss_wo_wwr_amount),
    fy1974_loss_wwr_amount: toFloat(row.fy1974_loss_wwr_amount),
    fy2008_10d_margined_amount: toFloat(row.fy2008_10d_margined_amount),
    fy2008_1y_unmargined_amount: toFloat(row.fy2008_1y_unmargined_amount),
    fy2008_exp_amount: toFloat(row.fy2008_exp_amount),
    fy2008_exp_cp_amount: toFloat(row.fy2008_exp_cp_amount),
    fy2008_si_amount: toFloat(row.fy2008_si_amount),
    fy2008_loss_wo_wwr_amount: toFloat(row.fy2008_loss_wo_wwr_amount),
    fy2008_loss_wwr_amount: toFloat(row.fy2008_loss_wwr_amount),
    bhc_light_nse_citi_amount: toFloat(row.bhc_light_nse_citi_amount),
    bhc_light_nse_cp_amount: toFloat(row.bhc_light_nse_cp_amount),
    bhc_light_si_amount: toFloat(row.bhc_light_si_amount),
    bhc_light_loss_wo_wwr_amount: toFloat(row.bhc_light_loss_wo_wwr_amount),
    bhc_light_loss_wwr_amount: toFloat(row.bhc_light_loss_wwr_amount),
    cmdty_up_1pct_si_amount: toFloat(row.cmdty_up_1pct_si_amount),
    cmdty_vol_up_1pct_si_amount: toFloat(row.cmdty_vol_up_1pct_si_amount),
    cr_up_1bps_si_amount: toFloat(row.cr_up_1bps_si_amount),
    deep_down_10d_margined_amount: toFloat(row.deep_down_10d_margined_amount),
    deep_down_1y_unmargined_amount: toFloat(row.deep_down_1y_unmargined_amount),
    deep_down_exp_amount: toFloat(row.deep_down_exp_amount),
    deep_down_exp_cp_amount: toFloat(row.deep_down_exp_cp_amount),
    deep_downturn_si_amount: toFloat(row.deep_downturn_si_amount),
    deep_down_loss_wo_wwr_amount: toFloat(row.deep_down_loss_wo_wwr_amount),
    deep_down_loss_wwr_amount: toFloat(row.deep_down_loss_wwr_amount),
    dollar_decline_10d_margined_amount: toFloat(row.dollar_decline_10d_margined_amount),
    dollar_decline_1y_unmargined_amount: toFloat(row.dollar_decline_1y_unmargined_amount),
    dollar_decline_exp_amount: toFloat(row.dollar_decline_exp_amount),
    dollar_decline_exp_cp_amount: toFloat(row.dollar_decline_exp_cp_amount),
    dollar_decline_si_amount: toFloat(row.dollar_decline_si_amount),
    dollar_decline_loss_wo_wwr_amount: toFloat(row.dollar_decline_loss_wo_wwr_amount),
    dollar_decline_loss_wwr_amount: toFloat(row.dollar_decline_loss_wwr_amount),
    em_crisis_exp_amount: toFloat(row.em_crisis_exp_amount),
    em_crisis_exp_counter_party_amount: toFloat(row.em_crisis_exp_counter_party_amount),
    em_crisis_si_amount: toFloat(row.em_crisis_si_amount),
    em_crisis_loss_wo_wwr_amount: toFloat(row.em_crisis_loss_wo_wwr_amount),
    em_crisis_loss_wwr_amount: toFloat(row.em_crisis_loss_wwr_amount),
    eq_up_1pct_si_amount: toFloat(row.eq_up_1pct_si_amount),
    eq_vol_up_1pct_si_amount: toFloat(row.eq_vol_up_1pct_si_amount),
    eq_rally_exp_amount: toFloat(row.eq_rally_exp_amount),
    eq_rally_exp_cp_amount: toFloat(row.eq_rally_exp_cp_amount),
    eq_rally_si_amount: toFloat(row.eq_rally_si_amount),
    euro_crisis_exp_amount: toFloat(row.euro_crisis_exp_amount),
    euro_crisis_exp_cp_amount: toFloat(row.euro_crisis_exp_cp_amount),
    euro_crisis_si_amount: toFloat(row.euro_crisis_si_amount),
    euro_crisis_loss_wo_wwr_amount: toFloat(row.euro_crisis_loss_wo_wwr_amount),
    euro_crisis_loss_wwr_amount: toFloat(row.euro_crisis_loss_wwr_amount),
    frb_sams_nse_citi_amount: toFloat(row.frb_sams_nse_citi_amount),
    frb_sams_nse_cp_amount: toFloat(row.frb_sams_nse_cp_amount),
    frb_sams_si_amount: toFloat(row.frb_sams_si_amount),
    frb_sams_loss_wo_wwr_amount: toFloat(row.frb_sams_loss_wo_wwr_amount),
    frb_sams_loss_wwr_amount: toFloat(row.frb_sams_loss_wwr_amount),
    fx_up_1pct_si_amount: toFloat(row.fx_up_1pct_si_amount),
    fxvl_up_1pct_si_amount: toFloat(row.fxvl_up_1pct_si_amount),
    interest_rate_10d_margined_amount: toFloat(row.interest_rate_10d_margined_amount),
    interest_rate_1y_unmargined_amount: toFloat(row.interest_rate_1y_unmargined_amount),
    interest_rate_exp_amount: toFloat(row.interest_rate_exp_amount),
    interest_rate_exp_cp_amount: toFloat(row.interest_rate_exp_cp_amount),
    interest_rate_si_amount: toFloat(row.interest_rate_si_amount),
    interest_rate_loss_wo_wwr_amount: toFloat(row.interest_rate_loss_wo_wwr_amount),
    interest_rate_loss_wwr_amount: toFloat(row.interest_rate_loss_wwr_amount),
    interest_rate_up_1bps_si_amount: toFloat(row.interest_rate_up_1bps_si_amount),
    interest_rate_up_1pct_si_amount: toFloat(row.interest_rate_up_1pct_si_amount),
    lost_decade_10d_margined_amount: toFloat(row.lost_decade_10d_margined_amount),
    lost_decade_1y_unmargined_amount: toFloat(row.lost_decade_1y_unmargined_amount),
    lost_decade_exp_amount: toFloat(row.lost_decade_exp_amount),
    lost_decade_exp_cp_amount: toFloat(row.lost_decade_exp_cp_amount),
    lost_decade_si_amount: toFloat(row.lost_decade_si_amount),
    lost_decade_loss_wo_wwr_amount: toFloat(row.lost_decade_loss_wo_wwr_amount),
    lost_decade_loss_wwr_amount: toFloat(row.lost_decade_loss_wwr_amount),
    climate_risk1_10d_margined_amount: toFloat(row.climate_risk1_10d_margined_amount),
    climate_risk1_1y_unmargined_amount: toFloat(row.climate_risk1_1y_unmargined_amount),
    climate_risk1_agg_si_amount: toFloat(row.climate_risk1_agg_si_amount),
    climate_risk2_10d_margined_amount: toFloat(row.climate_risk2_10d_margined_amount),
    climate_risk2_1y_unmargined_amount: toFloat(row.climate_risk2_1y_unmargined_amount),
    climate_risk2_agg_si_amount: toFloat(row.climate_risk2_agg_si_amount),
    cgml_icaap1_10d_margined_amount: toFloat(row.cgml_icaap1_10d_margined_amount),
    cgml_icaap1_1y_unmargined_amount: toFloat(row.cgml_icaap1_1y_unmargined_amount),
    cgml_icaap1_agg_si_amount: toFloat(row.cgml_icaap1_agg_si_amount),
    cgml_icaap2_10d_margined_amount: toFloat(row.cgml_icaap2_10d_margined_amount),
    cgml_risk_appetite_10d_amount: toFloat(row.cgml_risk_appetite_10d_amount),
    cgml_icaap2_1y_unmargined_amount: toFloat(row.cgml_icaap2_1y_unmargined_amount),
    cgml_risk_appetite_1y_amount: toFloat(row.cgml_risk_appetite_1y_amount),
    cgml_icaap2_agg_si_amount: toFloat(row.cgml_icaap2_agg_si_amount),
    cgml_risk_appetite_si_amount: toFloat(row.cgml_risk_appetite_si_amount),
    global_pandemic_si_amount: toFloat(row.global_pandemic_si_amount),
    asian_crisis_si_amount: toFloat(row.asian_crisis_si_amount),
    counterparty_threshold_amount: toFloat(row.counterparty_threshold_amount),
    asian_10d_margined_amount: toFloat(row.asian_10d_margined_amount),
    asian_1y_unmargined_amount: toFloat(row.asian_1y_unmargined_amount),
    asian_agg_exp_amount: toFloat(row.asian_agg_exp_amount),
    asian_agg_loss_wo_wwr_amount: toFloat(row.asian_agg_loss_wo_wwr_amount),
    asian_agg_loss_wwr_amount: toFloat(row.asian_agg_loss_wwr_amount),
    mexican_10d_margined_amount: toFloat(row.mexican_10d_margined_amount),
    mexican_1y_unmargined_amount: toFloat(row.mexican_1y_unmargined_amount),
    mexican_agg_exp_amount: toFloat(row.mexican_agg_exp_amount),
    mexican_agg_si_citi_amount: toFloat(row.mexican_agg_si_citi_amount),
    mexican_agg_loss_wo_wwr_amount: toFloat(row.mexican_agg_loss_wo_wwr_amount),
    mexican_agg_wwr_amount: toFloat(row.mexican_agg_wwr_amount),
    pandemic_10d_margined_amount: toFloat(row.pandemic_10d_margined_amount),
    pandemic_1y_unmargined_amount: toFloat(row.pandemic_1y_unmargined_amount),
    pandemic_agg_exp_amount: toFloat(row.pandemic_agg_exp_amount),
    pandemic_agg_loss_wo_wwr_amount: toFloat(row.pandemic_agg_loss_wo_wwr_amount),
    pandemic_agg_loss_wwr_amount: toFloat(row.pandemic_agg_loss_wwr_amount),
    inflation_rates_10d_margined_amount: toFloat(row.inflation_rates_10d_margined_amount),
    inflation_rates_1y_unmargined_amount: toFloat(row.inflation_rates_1y_unmargined_amount),
    inflation_rates_agg_si_citi_amount: toFloat(row.inflation_rates_agg_si_citi_amount),
    inflation_rates_agg_exp_amount: toFloat(row.inflation_rates_agg_exp_amount),
    inflation_rates_agg_loss_wo_wwr_amount: toFloat(row.inflation_rates_agg_loss_wo_wwr_amount),
    inflation_rates_agg_loss_wwr_amount: toFloat(row.inflation_rates_agg_loss_wwr_amount),
    global_fx_10d_margined_amount: toFloat(row.global_fx_10d_margined_amount),
    global_fx_1y_unmargined_amount: toFloat(row.global_fx_1y_unmargined_amount),
    global_fx_agg_si_citi_amount: toFloat(row.global_fx_agg_si_citi_amount),
    global_fx_agg_exp_amount: toFloat(row.global_fx_agg_exp_amount),
    global_fx_agg_loss_wo_wwr_amount: toFloat(row.global_fx_agg_loss_wo_wwr_amount),
    global_fx_agg_loss_wwr_amount: toFloat(row.global_fx_agg_loss_wwr_amount),
    dsft_multirisk_10d_margined_amount: toFloat(row.dsft_multirisk_10d_margined_amount),
    dsft_multirisk_1y_unmargined_amount: toFloat(row.dsft_multirisk_1y_unmargined_amount),
    dsft_multirisk_agg_si_amount: toFloat(row.dsft_multirisk_agg_si_amount),
    dsft_multirisk_agg_exp_amount: toFloat(row.dsft_multirisk_agg_exp_amount),
    dsft_multirisk_agg_exp_cp_amount: toFloat(row.dsft_multirisk_agg_exp_cp_amount),
    dsft_multirisk_loss_wo_wwr_amount: toFloat(row.dsft_multirisk_loss_wo_wwr_amount),
    dsft_multirisk_loss_wwr_amount: toFloat(row.dsft_multirisk_loss_wwr_amount),
    european_10d_margined_amount: toFloat(row.european_10d_margined_amount),
    european_1y_unmargined_amount: toFloat(row.european_1y_unmargined_amount),
    european_agg_si_citi_amount: toFloat(row.european_agg_si_citi_amount),
    european_agg_exp_amount: toFloat(row.european_agg_exp_amount),
    european_loss_wo_wwr_amount: toFloat(row.european_loss_wo_wwr_amount),
    european_agg_loss_wwr_amount: toFloat(row.european_agg_loss_wwr_amount),
    gsst_bhc_10d_margined_amount: toFloat(row.gsst_bhc_10d_margined_amount),
    gsst_bhc_1y_unmargined_amount: toFloat(row.gsst_bhc_1y_unmargined_amount),
    bhc_gsst_agg_si_citi_amount: toFloat(row.bhc_gsst_agg_si_citi_amount),
    bhc_gsst_agg_exp_amount: toFloat(row.bhc_gsst_agg_exp_amount),
    bhc_gsst_agg_loss_wo_wwr_amount: toFloat(row.bhc_gsst_agg_loss_wo_wwr_amount),
    bhc_gsst_agg_loss_wwr_amount: toFloat(row.bhc_gsst_agg_loss_wwr_amount),
    bhc_gsst_1y_exp_amount: toFloat(row.bhc_gsst_1y_exp_amount),
    bhc_gsst_1y_cp_exp_amount: toFloat(row.bhc_gsst_1y_cp_exp_amount),
    bhc_gsst_1y_loss_wo_wwr_amount: toFloat(row.bhc_gsst_1y_loss_wo_wwr_amount),
    bhc_gsst_1y_loss_wwr_amount: toFloat(row.bhc_gsst_1y_loss_wwr_amount),
    inflation_rates_agg_exp_cp_amount: toFloat(row.inflation_rates_agg_exp_cp_amount),
    global_fx_agg_exp_cp_amount: toFloat(row.global_fx_agg_exp_cp_amount),
    european_agg_exp_cp_amount: toFloat(row.european_agg_exp_cp_amount),
    asian_agg_exp_cp_amount: toFloat(row.asian_agg_exp_cp_amount),
    mexican_agg_exp_cp_amount: toFloat(row.mexican_agg_exp_cp_amount),
    bhc_gsst_agg_exp_cp_amount: toFloat(row.bhc_gsst_agg_exp_cp_amount),
    pandemic_agg_exp_cp_amount: toFloat(row.pandemic_agg_exp_cp_amount),
    max_gsst_nse_citi_amount: toFloat(row.max_gsst_nse_citi_amount),
    max_gsst_nse_cp_amount: toFloat(row.max_gsst_nse_cp_amount),
    max_limit_monitor_nse_citi_amount: toFloat(row.max_limit_monitor_nse_citi_amount),
    max_limit_monitor_nse_cp_amount: toFloat(row.max_limit_monitor_nse_cp_amount),
    issuer_jth_si_amount: toFloat(row.issuer_jth_si_amount),
    issuer_jth_large_si_amount: toFloat(row.issuer_jth_large_si_amount),
    ijtd_allocated_amount: toFloat(row.ijtd_allocated_amount),
    dispersion_risk_allocated_amount: toFloat(row.dispersion_risk_allocated_amount),
    pandemic_agg_si_citi_amount: toFloat(row.pandemic_agg_si_citi_amount),
    net_mtm_citi_amount: toFloat(row.net_mtm_citi_amount),
    net_mtm_cp_amount: toFloat(row.net_mtm_cp_amount),
    commodity_selloff_agg_si_amount: toFloat(row.commodity_selloff_agg_si_amount),
    commodity_selloff_agg_exp_amount: toFloat(row.commodity_selloff_agg_exp_amount),
    commodity_selloff_agg_exp_cp_amount: toFloat(row.commodity_selloff_agg_exp_cp_amount),
    commodity_rally_agg_si_amount: toFloat(row.commodity_rally_agg_si_amount),
    commodity_rally_agg_exp_amount: toFloat(row.commodity_rally_agg_exp_amount),
    commodity_rally_agg_exp_cp_amount: toFloat(row.commodity_rally_agg_exp_cp_amount),
    global_rates_up_si_amount: toFloat(row.global_rates_up_si_amount),
    global_rates_up_exp_amount: toFloat(row.global_rates_up_exp_amount),
    global_rates_up_exp_cp_amount: toFloat(row.global_rates_up_exp_cp_amount),
    global_rates_down_si_amount: toFloat(row.global_rates_down_si_amount),
    global_rates_down_exp_amount: toFloat(row.global_rates_down_exp_amount),
    global_rates_down_exp_cp_amount: toFloat(row.global_rates_down_exp_cp_amount),
    em_crisis_10d_margined_amount: toFloat(row.em_crisis_10d_margined_amount),
    em_crisis_1y_unmargined_amount: toFloat(row.em_crisis_1y_unmargined_amount),
    euro_crisis_10d_margined_amount: toFloat(row.euro_crisis_10d_margined_amount),
    euro_crisis_1y_unmargined_amount: toFloat(row.euro_crisis_1y_unmargined_amount),
    frb_sams_inflationary_nse_citi_amount: toFloat(row.frb_sams_inflationary_nse_citi_amount),
    frb_sams_inflationary_nse_cp_amount: toFloat(row.frb_sams_inflationary_nse_cp_amount),
    frb_sams_inflationary_si_amount: toFloat(row.frb_sams_inflationary_si_amount),
    frb_sams_deflationary_nse_citi_amount: toFloat(row.frb_sams_deflationary_nse_citi_amount),
    frb_sams_deflationary_nse_cp_amount: toFloat(row.frb_sams_deflationary_nse_cp_amount),
    frb_sams_deflationary_si_amount: toFloat(row.frb_sams_deflationary_si_amount),
    gas_power_selloff_agg_si_amount: toFloat(row.gas_power_selloff_agg_si_amount),
    gas_power_selloff_agg_exp_amount: toFloat(row.gas_power_selloff_agg_exp_amount),
    gas_power_selloff_agg_exp_cp_amount: toFloat(row.gas_power_selloff_agg_exp_cp_amount),
    gas_power_rally_agg_si_amount: toFloat(row.gas_power_rally_agg_si_amount),
    gas_power_rally_agg_exp_amount: toFloat(row.gas_power_rally_agg_exp_amount),
    gas_power_rally_agg_exp_cp_amount: toFloat(row.gas_power_rally_agg_exp_cp_amount),
    eq_up30_si_amount: toFloat(row.eq_up30_si_amount),
    eq_up30_exp_amount: toFloat(row.eq_up30_exp_amount),
    eq_up30_exp_cp_amount: toFloat(row.eq_up30_exp_cp_amount),
    eq_down30_si_amount: toFloat(row.eq_down30_si_amount),
    eq_down30_exp_amount: toFloat(row.eq_down30_exp_amount),
    eq_down30_exp_cp_amount: toFloat(row.eq_down30_exp_cp_amount),
    credit_300bps_si_amount: toFloat(row.credit_300bps_si_amount),
    credit_300bps_exp_amount: toFloat(row.credit_300bps_exp_amount),
    credit_300bps_exp_cp_amount: toFloat(row.credit_300bps_exp_cp_amount),
    rates_up_100bps_si_amount: toFloat(row.rates_up_100bps_si_amount),
    rates_up_100bps_exp_amount: toFloat(row.rates_up_100bps_exp_amount),
    rates_up_100bps_exp_cp_amount: toFloat(row.rates_up_100bps_exp_cp_amount),
    rates_down_100bps_si_amount: toFloat(row.rates_down_100bps_si_amount),
    rates_down_100bps_exp_amount: toFloat(row.rates_down_100bps_exp_amount),
    rates_down_100bps_exp_cp_amount: toFloat(row.rates_down_100bps_exp_cp_amount),
    fx_up_10pct_si_amount: toFloat(row.fx_up_10pct_si_amount),
    fx_up_10pct_exp_amount: toFloat(row.fx_up_10pct_exp_amount),
    fx_up_10pct_exp_cp_amount: toFloat(row.fx_up_10pct_exp_cp_amount),
    fx_down_10pct_si_amount: toFloat(row.fx_down_10pct_si_amount),
    fx_down_10pct_exp_amount: toFloat(row.fx_down_10pct_exp_amount),
    fx_down_10pct_exp_cp_amount: toFloat(row.fx_down_10pct_exp_cp_amount),
    commodity_up_30pct_si_amount: toFloat(row.commodity_up_30pct_si_amount),
    commodity_up_30pct_exp_amount: toFloat(row.commodity_up_30pct_exp_amount),
    commodity_up_30pct_exp_cp_amount: toFloat(row.commodity_up_30pct_exp_cp_amount),
    commodity_down_30pct_si_amount: toFloat(row.commodity_down_30pct_si_amount),
    commodity_down_30pct_exp_amount: toFloat(row.commodity_down_30pct_exp_amount),
    commodity_down_30pct_exp_cp_amount: toFloat(row.commodity_down_30pct_exp_cp_amount),
    max_lmt_mon_nocpm_nse_citi_amount: toFloat(row.max_lmt_mon_nocpm_nse_citi_amount),
    max_lmt_mon_nocpm_nse_cp_amount: toFloat(row.max_lmt_mon_nocpm_nse_cp_amount),
    gsst_adhoc1_agg_si_amount: toFloat(row.gsst_adhoc1_agg_si_amount),
    gsst_adhoc1_agg_exp_amount: toFloat(row.gsst_adhoc1_agg_exp_amount),
    gsst_adhoc1_agg_exp_cp_amount: toFloat(row.gsst_adhoc1_agg_exp_cp_amount),
    gsst_adhoc2_agg_si_amount: toFloat(row.gsst_adhoc2_agg_si_amount),
    gsst_adhoc2_agg_exp_amount: toFloat(row.gsst_adhoc2_agg_exp_amount),
    gsst_adhoc2_agg_exp_cp_amount: toFloat(row.gsst_adhoc2_agg_exp_cp_amount),
    gsst_adhoc7_agg_si_amount: toFloat(row.gsst_adhoc7_agg_si_amount),
    gsst_adhoc7_agg_exp_amount: toFloat(row.gsst_adhoc7_agg_exp_amount),
    gsst_adhoc7_agg_exp_cp_amount: toFloat(row.gsst_adhoc7_agg_exp_cp_amount),
    bull_steepener_agg_si_amount: toFloat(row.bull_steepener_agg_si_amount),
    bull_steepener_agg_exp_amount: toFloat(row.bull_steepener_agg_exp_amount),
    bull_steepener_agg_exp_cp_amount: toFloat(row.bull_steepener_agg_exp_cp_amount),
    bull_flattener_agg_si_amount: toFloat(row.bull_flattener_agg_si_amount),
    bull_flattener_agg_exp_amount: toFloat(row.bull_flattener_agg_exp_amount),
    bull_flattener_agg_exp_cp_amount: toFloat(row.bull_flattener_agg_exp_cp_amount),
    bear_steepener_agg_si_amount: toFloat(row.bear_steepener_agg_si_amount),
    bear_steepener_agg_exp_amount: toFloat(row.bear_steepener_agg_exp_amount),
    bear_steepener_agg_exp_cp_amount: toFloat(row.bear_steepener_agg_exp_cp_amount),
    bear_flattener_agg_si_amount: toFloat(row.bear_flattener_agg_si_amount),
    bear_flattener_agg_exp_amount: toFloat(row.bear_flattener_agg_exp_amount),
    bear_flattener_agg_exp_cp_amount: toFloat(row.bear_flattener_agg_exp_cp_amount),
    bull_stp_fxdown_si_amount: toFloat(row.bull_stp_fxdown_si_amount),
    bull_stp_fxdown_exp_amount: toFloat(row.bull_stp_fxdown_exp_amount),
    bull_stp_fxdown_exp_cp_amount: toFloat(row.bull_stp_fxdown_exp_cp_amount),
    bull_flt_fxdown_si_amount: toFloat(row.bull_flt_fxdown_si_amount),
    bull_flt_fxdown_exp_amount: toFloat(row.bull_flt_fxdown_exp_amount),
    bull_flt_fxdown_exp_cp_amount: toFloat(row.bull_flt_fxdown_exp_cp_amount),
    bear_stp_fxdown_si_amount: toFloat(row.bear_stp_fxdown_si_amount),
    bear_stp_fxdown_exp_amount: toFloat(row.bear_stp_fxdown_exp_amount),
    bear_stp_fxdown_exp_cp_amount: toFloat(row.bear_stp_fxdown_exp_cp_amount),
    bear_flt_fxdown_si_amount: toFloat(row.bear_flt_fxdown_si_amount),
    bear_flt_fxdown_exp_amount: toFloat(row.bear_flt_fxdown_exp_amount),
    bear_flt_fxdown_exp_cp_amount: toFloat(row.bear_flt_fxdown_exp_cp_amount)
}})
CREATE (t)-[:TRANSACTIONS]->(g)
}} IN TRANSACTIONS OF 1000 ROWS
"""


class Neo4jImporter:
    """
    A class to handle the import of transaction files into Neo4j.
    """

    def __init__(self, uri, user, password, database):
        self.driver = None
        try:
            self.driver = GraphDatabase.driver(uri, auth=(user, password), database=database)
            self.driver.verify_connectivity()
            logging.info("Successfully connected to Neo4j database.")
        except Exception as e:
            logging.error(f"Failed to create Neo4j driver: {e}")
            raise

    def close(self):
        if self.driver:
            self.driver.close()
            logging.info("Neo4j connection closed.")

    def import_file(self, file_path):
        file_name = os.path.basename(file_path)
        print(file_name)
        relative_file_path = file_path.replace('/mnt/nas/', '').replace(' ', '%20')
        query = CYPHER_QUERY_TEMPLATE.format(file_name=relative_file_path)
        try:
            with self.driver.session(database=NEO4J_DATABASE) as session:
                gfcids = self._collect_gfcids(file_path)
                self._ensure_summary_nodes(session, gfcids)
                result = session.run(query)
                logging.info(f"Successfully executed query for file: {file_name}")
                logging.info(f"Query summary: {result.consume().counters}")
        except Exception as e:
            logging.error(f"Error importing file {file_name}: {e}")

    @staticmethod
    def _collect_gfcids(file_path: str) -> Set[str]:
        """
        Collect unique gfcid values from the CSV so Summary_GFCID nodes can be prepared.
        """
        gfcids: set[str] = set()
        try:
            with open(file_path, newline="", encoding="utf-8") as csv_file:
                reader = csv.DictReader(csv_file)
                if not reader.fieldnames or "gfcid" not in reader.fieldnames:
                    logging.warning("File %s does not contain a 'gfcid' column.", file_path)
                    return gfcids
                for row in reader:
                    value = (row.get("gfcid") or "").strip()
                    if value:
                        gfcids.add(value)
        except FileNotFoundError:
            logging.error("CSV file %s was not found when collecting gfcids.", file_path)
        except Exception as exc:
            logging.error("Failed to collect gfcids from %s: %s", file_path, exc)
        return gfcids

    def _ensure_summary_nodes(self, session, gfcids: Iterable[str]) -> None:
        """
        Ensure the uniqueness constraint exists and seed Summary_GFCID nodes for the provided list.
        """
        gfcids = list({gid for gid in gfcids if gid})
        if not gfcids:
            logging.info("No gfcids discovered for Summary_GFCID creation.")
            return

        constraint_query = (
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Summary_GFCID) REQUIRE n.gfcid IS UNIQUE"
        )
        create_query = """
UNWIND $gfcids AS gfcid
MERGE (n:Summary_GFCID {gfcid: gfcid})
"""
        session.run(constraint_query)
        session.run(create_query, gfcids=gfcids)
        logging.info("Ensured %d Summary_GFCID nodes exist.", len(gfcids))


def find_transaction_files(directory=None, prefix=''):
    """
    Finds all files matching the transaction file pattern in a single base directory.
    """
    file_pattern = re.compile(rf"{prefix}[A-Za-z0-9\-_]+\.csv")
    matched_files = []
    if not os.path.isdir(directory):
        logging.warning(f"Directory not found, skipping: {directory}")
        return matched_files

    logging.info(f"Scanning for files in: {directory}")
    for root, _, files in os.walk(directory):
        for file in files:
            if file_pattern.match(file):
                matched_files.append(os.path.join(root, file))

    matched_files.sort()
    logging.info(f"Found {len(matched_files)} files to process in this directory.")
    return matched_files


def import_file_worker(file_path):
    importer = Neo4jImporter(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, NEO4J_DATABASE)
    try:
        importer.import_file(file_path)
        return (file_path, True)
    except Exception as e:
        logging.error(f"Worker error for {file_path}: {e}")
        return (file_path, False)
    finally:
        importer.close()


def main():
    prefix = 'olympus_credit_txn_'
    logging.info("Starting Neo4j bulk import process...")
    try:
        for base_dir in BASE_DIRECTORIES:
            logging.info(f"--- Processing Base Directory: {base_dir} ---")
            files_to_process = find_transaction_files(base_dir, prefix)

            if not files_to_process:
                logging.warning("No transaction files found in this directory. Moving to next.")
                continue

            # Test run with the first file
            test_file = files_to_process.pop(0)
            import_file_worker(test_file)
            logging.info(f"--- Test run finished for: {os.path.basename(test_file)} ---")

            if not files_to_process:
                logging.info("Only one file was found and it has been processed. Moving to next directory.")
                continue

            if not sys.stdout.isatty():
                logging.warning("Non-interactive mode detected. Skipping confirmation and stopping further processing for this directory.")
                continue

            try:
                print('## chmod -R  777 /mnt/nas')
                proceed = input(f"Test successful. Proceed with the remaining {len(files_to_process)} files in this directory? (y/n): ")
            except EOFError:
                logging.warning("Could not read user input. Skipping remaining files for this directory.")
                continue

            if proceed.lower() in ['y', 'yes']:
                logging.info(f"User confirmed. Processing remaining {len(files_to_process)} files in parallel...")
                with multiprocessing.Pool(processes=min(8, len(files_to_process))) as pool:
                    results = pool.map(import_file_worker, files_to_process)
                    failed_files = [f for f, success in results if not success]
                    if failed_files:
                        logging.warning(f"Failed files: {failed_files}")
            else:
                logging.info("User declined. Skipping remaining files in this directory.")

            logging.info(f"--- Finished processing directory: {base_dir} ---")
        logging.info("All directories have been processed.")
    except Exception as e:
        logging.error(f"An unrecoverable error occurred during the import process: {e}")


if __name__ == "__main__":
    main()
