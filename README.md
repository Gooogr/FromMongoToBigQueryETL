# FromMongoToBigQueryETL

Data connectors for DWHs Crontab command:
0 */4 * * * /home/<google account name>/env/bin/python3 /home/<google account name>/FromMongoToBigQueryETL/etl_script.py >> /home/<google account name>/crontab_logs.log 2>&1
