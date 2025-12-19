
# import data process steps

1. fetch source file :
- user provide input : {domain_type}, {domain_name}, {cob_date}  
- lookup conf/data_map.csv to find source file connector type and parameters
- use connector to fetch file from remote location
- save to /mnt/nas/olympus_credit_txn_{cob_date}.dat
  ex: /mnt/nas/olympus_credit_txn_20240630.dat

2. cut columns to processed file :
- lookup  predefined columns list from conf/column_map.yaml
- source: /mnt/nas/olympus_credit_txn_{cob_date}.dat
  ex: /mnt/nas/olympus_credit_txn_20240630.dat
- cut file by predefined columns :
- save to : /mnt/nas/{cob_date}/olympus_credit_txn_{cob_date}.dat

3. split by GFCID :
source: /mnt/nas/{cob_date}/olympus_credit_txn_{cob_date}.dat
