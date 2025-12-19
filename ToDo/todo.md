
# import data process steps

below are the API  /imports  data import process steps:

please  implement the following steps in the data import pipeline:
make  sure to handle errors and log progress at each step.
each of  the  steps should be modular to allow for easy maintenance and updates.
configure  necessary  settings  in  conf/ directory as needed.


1. fetch source file :
- user provide input : {domain_type}, {domain_name}, {cob_date}  
  ex:
  {
    "domain_type": "CURATED",
    "domain_name": "Credit Risk Transactions",
    "cob_date": "20251106" 
  }
- lookup conf/data_map.csv to find source file connector type and parameters
    ex:
    |domain_type|domain_name               |connector_type|connector_params                                      |source_file_path_template                          |
    |-----------|--------------------------|--------------|------------------------------------------------------|--------------------------------------------------|
    |CURATED    |Credit Risk Transactions  |linux         |{"host":"localhost","port":22,"username":"user","password":"pass"}|/data/olympus/credit_txn/olympus_credit_txn_{cob_date}.dat|
    
    then  lookup auth  info  from  conf/settings.yaml if needed.
    from above example:
    - connector_type: linux
    - connector_params: {"host":"sftp.example.com","port":22,"username":"user","password":"pass"}
    - source_file_path_template: /data/olympus/credit_txn/olympus_credit_txn_{cob_date}.dat
- use connector to fetch file from remote location
- save to /mnt/nas/olympus_credit_txn_{cob_date}.dat
  ex: /mnt/nas/olympus_credit_txn_20240630.dat

2. cut columns to processed file :
- lookup  predefined columns list from conf/column_map.yaml
- source: /mnt/nas/olympus_credit_txn_{cob_date}.dat
  ex: /mnt/nas/olympus_credit_txn_20240630.dat
- cut file by predefined columns :
  ex: 
  cut  -d $'\x01'  -f 2,4,5,6,16,26,43 mnt/src/olympus_credit_txn_20240630.dat  > /mnt/nas/20240630/olympus_credit_txn_20240630.dat
- save to : /mnt/nas/{cob_date}/olympus_credit_txn_{cob_date}.dat

3. split by GFCID :
- lookup  required columns list from conf/column_map.yaml
reference to file: bin/split_row.py (use the column list above to split file by GFCID column)
source: /mnt/nas/{cob_date}/olympus_credit_txn_{cob_date}.dat

4. load  transaction  data to neo4j :
- reference to  script:  bin/load_csv_multiprocess.py