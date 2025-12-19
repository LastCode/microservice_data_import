#!/usr/bin/env python3
import os
import pandas as pd
import sys


def split_row_by_key(key, date):
    prefix = "olympus_credit_txn_"
    input_filename = f"/mnt/nas/{date}/{prefix}{date}.dat"
    output_dir = f"/mnt/nas/{date}/split"

    headers_list = [
        # Core identifiers (cols 0-19)
        "TXN_ID",
        "CAGID",
        "COUNTERPARTY_NAME",
        "GFCID",
        "NETTING_ID",
        "LEI",
        "BOOK_ID",
        "DESK_ID",
        "TRADER_ID",
        "LEGAL_ENTITY",
        "PRODUCT_TYPE",
        "SECTOR",
        "REGION",
        "TRADE_STATUS",
        "TRADE_DATE",
        "MATURITY_DATE",
        "VALUE_DATE",
        "BASE_CURRENCY",
        "CREDIT_RATING",
        "PD",
        # Exposure metrics (cols 20-99)
        *[f"EXPOSURE_AMT_{i}" for i in range(1, 41)],
        *[f"EXPOSURE_RATE_{i}" for i in range(1, 25)],
        *[f"EXPOSURE_COUNT_{i}" for i in range(1, 17)],
        # Risk metrics by scenario (cols 100-299)
        *[f"BASE_RISK_{i}" for i in range(1, 34)],
        *[f"ADVERSE_RISK_{i}" for i in range(1, 34)],
        *[f"SEVERE_RISK_{i}" for i in range(1, 34)],
        *[f"DOLLAR_DECLINE_RISK_{i}" for i in range(1, 34)],
        *[f"RATE_SHOCK_RISK_{i}" for i in range(1, 34)],
        *[f"CREDIT_SPREAD_RISK_{i}" for i in range(1, 34)],
        "SCENARIO_EXTRA_1",
        "SCENARIO_EXTRA_2",
        # Collateral data (cols 300-399)
        *[f"COLLATERAL_TYPE_{i}" for i in range(1, 11)],
        *[f"COLLATERAL_AMT_{i}" for i in range(1, 41)],
        *[f"COLLATERAL_HAIRCUT_{i}" for i in range(1, 21)],
        *[f"COLLATERAL_FLAG_{i}" for i in range(1, 31)],
        # Netting data (cols 400-499)
        *[f"NETTING_STATUS_{i}" for i in range(1, 6)],
        *[f"NETTING_AMT_{i}" for i in range(1, 46)],
        *[f"NETTING_SET_{i}" for i in range(1, 51)],
        # Historical metrics (cols 500-599)
        *[f"HIST_METRIC_{i}" for i in range(1, 101)],
        # Regulatory metrics (cols 600-699)
        *[f"RWA_{i}" for i in range(1, 31)],
        *[f"CAPITAL_RATIO_{i}" for i in range(1, 31)],
        *[f"REG_DAYS_{i}" for i in range(1, 21)],
        *[f"REG_FRAMEWORK_{i}" for i in range(1, 21)],
        # Additional attributes (cols 700-799)
        *[f"ATTR_ID_{i}" for i in range(1, 21)],
        *[f"ATTR_AMT_{i}" for i in range(1, 31)],
        *[f"ATTR_DATE_{i}" for i in range(1, 21)],
        *[f"ATTR_FLAG_{i}" for i in range(1, 21)],
        *[f"ATTR_VALUE_{i}" for i in range(1, 11)],
    ]

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Check if input file exists
    if not os.path.exists(input_filename):
        print(f"Error: Input file '{input_filename}' not found.")
        return

    try:
        reader = pd.read_csv(input_filename, chunksize=50000, header=None, names=headers_list, delimiter='\x01')
    except Exception as e:
        print(f"Unexpected error while reading the file: {e}")
        return

    print("Processing file...")
    for i, chunk in enumerate(reader):
        print(f"Processing chunk {i + 1} with {len(chunk)} rows...")
        for group_key, group_df in chunk.groupby(key):
            output_filename = os.path.join(output_dir, f"{prefix}{date}-{group_key}.csv")
            file_exists = os.path.exists(output_filename)
            group_df.to_csv(output_filename, mode="a", header=not file_exists, index=False)
            print(f"Written to: {output_filename}")

    print(f"File successfully split and saved in '{output_dir}' directory.")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: ./split_row.py <key> <date>")
        sys.exit(1)
    split_row_by_key(sys.argv[1], sys.argv[2])
