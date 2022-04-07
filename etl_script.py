import os
import json
import logging
from tqdm import tqdm
import pymongo
from pymongo import MongoClient
from google.cloud import bigquery
from datetime import datetime, timedelta

# utils                   
from utils import (copy_json_to_storage, 
                   copy_data_from_storage_to_bigquery, delete_temp_file)                 
# connectors
from utils import (connect_to_mongodb_database, connect_to_bigquery,
                   connect_to_google_storage_bucket)   
### Select collections ####
COLLECTION_NAMES = ['events', 'orderitems', 'orders',  'products', 
                     'recurringcharges', 'sessions', 'shops', 'users']

# If the collections fileds is unstable and we need refresh table structure every time      
REDEFINE_SCHEMA = ['events']

### Set up logging ###
logging.basicConfig(
    filename="logger.log",
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')
log = logging.getLogger("CONNECTOR")
log.info(f"START JOB") 

### Read .env_connector parameters ###
dir_path = os.path.dirname(os.path.realpath(__file__))
env_path = os.path.join(dir_path, '.env_connector')                
with open(env_path, 'r') as fh: 
    env_dict = dict(
        tuple(line.replace('\n', '').split('='))
        for line in fh.readlines() if (not line.startswith('#')) and (line.strip())
                   )

# MongoDB 
login = env_dict['LOGIN']
password = env_dict['PASSWORD'] 
db_name = env_dict['CLUSTER_NAME']
mongodb_database = env_dict['MONGODB_DATABASE_NAME'] 

# Google cloud service key
json_service_key_name = env_dict['JSON_SERVICE_KEY_NAME']
json_service_key_path = os.path.join(dir_path, json_service_key_name)

# Google cloud services
project_id = env_dict['GOOGLE_CLOUD_PROJECT_ID'] 
bucket_name = env_dict['GOOGLE_STORAGE_BUCKET_NAME'] 
bucket_folder_path = env_dict['GOOGLE_STORAGE_BUCKET_FOLDER'] 
bigquery_dataset_id = env_dict['GOOGLE_BIGQUERY_DATASET_ID']


### Set job configs ###
# Remove old table, autodetect schema and create the new one 
job_config_truncate_autodetect = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
    create_disposition="CREATE_IF_NEEDED",
    autodetect=True,
    ignore_unknown_values=True,
    max_bad_records=1e3,  
    # ~ schema_update_option="ALLOW_FIELD_ADDITION",
    source_format="NEWLINE_DELIMITED_JSON"
)
# Rewrite data in the table based on pre-configured schema 
job_config_truncate_pre_configured = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
    create_disposition="CREATE_IF_NEEDED",
    autodetect=False,
    ignore_unknown_values=True,
    # ~ schema_update_option="ALLOW_FIELD_ADDITION",
    source_format="NEWLINE_DELIMITED_JSON"
)
               
###-------------------------Helper functions-------------------------###
           
def save_data_from_mongo(collection_name, database, query={}):           # TODO: For some reason I can't use this function from utils.py. Fix it.
    '''
    Save MongoDB's collection to the JSON linearized format. 
    File name template: 'temp_{collection_name}.json'
    Parameters:
            collection_name - str, collection name 
            database - str, database name (that contain target collection)
    Optional:
            query - dict, filetring condition. Read more here:
                https://docs.mongodb.com/manual/reference/method/db.collection.find/
    Return
        None
    '''
    collection = database[collection_name]
    table = collection.find(query)
    with open(f'temp_{collection_name}.json', 'w') as out_jsonl:                  
            for x in tqdm(table, desc=collection_name):
                    out_jsonl.write(json.dumps(x, default=str)+'\n') 

def create_schema_with_sample(collection, sample_query):
    '''
    Perform ETL steps: 
    1) Save data sample based on query from MongoDB in VM as temp_{collection_name}.json
    2) Send it to Google Storage as <bucket_name>/<bucket_folder>/temp_{collection_name}.json
    3) Delete previous table version in BigQuery, autodetect table schema and create new table 
    4) Delete temp_{collection_name}.json file from VM
    Parameters:
        collection - str, collection name 
        sample_query - dict, filetring condition. Read more here:
            https://docs.mongodb.com/manual/reference/method/db.collection.find/
    Return
        None
    '''
    save_data_from_mongo(collection, suggestr_db, sample_query)
    log.info(f"Get {collection} data sample from MongoDB") 

    copy_json_to_storage(collection, bucket, bucket_folder_path)  
    log.info(f"Upload {collection} data sample to Google Cloud") 

    copy_data_from_storage_to_bigquery(collection, 
                                   client = bigquery_client,
                                   bucket_name=bucket_name, 
                                   bucket_folder_path=bucket_folder_path, 
                                   project_id=project_id, 
                                   dataset_id=bigquery_dataset_id, 
                                   job_config=job_config_truncate_autodetect)
    log.info(f"Upload {collection} data sample to BigQuery") 

    delete_temp_file(collection)
    log.info(f"Delete {collection} temporary file") 


def upload_table(collection, job_config):
    '''
    Perform ETL steps:
    1) Save full data table from MongoDB in VM as temp_{collection_name}.json
    2) Send it to Google Stortage as <bucket_name>/<bucket_folder>/temp_{collection_name}.json
    3) Rewrite previous table in BigQuery based on job_config conditions:
        * Rewrite table based on previous table schema
        * Autodetect table schema from scratch and rewrite table 
    4) Delete temp_{collection_name}.json file from VM
    Parameters:
        collection - str, collection name 
        job_config - bigquery.LoadJobConfig object, define update behavior
    Return
        None    
    '''
    save_data_from_mongo(collection, suggestr_db)
    log.info(f"Get {collection} full dataset from MongoDB") 
    
    copy_json_to_storage(collection, bucket, bucket_folder_path)  
    log.info(f"Upload {collection} dataset to Google Cloud") 
    
    copy_data_from_storage_to_bigquery(collection, 
                                   client = bigquery_client,
                                   bucket_name=bucket_name, 
                                   bucket_folder_path=bucket_folder_path, 
                                   project_id=project_id, 
                                   dataset_id=bigquery_dataset_id, 
                                   job_config=job_config)           
    log.info(f"Upload {collection} dataset to BigQuery") 
    
    delete_temp_file(collection)
    log.info(f"Delete {collection} temporary file") 
                                      
###------------------------------------------------------------------###        
                            
try:
    # Connect to services:
    print('Create MongoDB connection...')
    suggestr_db = connect_to_mongodb_database(db_name, mongodb_database, login, password)
    log.info("Succesfully connect to MongoDB server")
    print('Create Google Serices connection...')
    bucket = connect_to_google_storage_bucket(json_service_key_path, project_id, bucket_name)  
    log.info("Succesfully connect to Google Storage")
    bigquery_client = connect_to_bigquery(json_service_key_path, project_id)
    log.info("Succesfully connect to BigQuery")

    # Perform ETL steps                                                                                                                                 
    for collection in COLLECTION_NAMES:
        print(collection)
        if collection in REDEFINE_SCHEMA:
            log.info(f"Stage 1: create table schema for {collection}")
            week_ago_date = today_date = datetime.today() - timedelta(days=7)
            sample_filtering_query = {'createdAt': {'$gte': week_ago_date}} 
            create_schema_with_sample(collection, sample_filtering_query)
            
            log.info(f"Stage 2: upload {collection}")
            upload_table(collection, job_config_truncate_pre_configured)
        else:
            log.info(f"Upload {collection}")
            upload_table(collection, job_config_truncate_autodetect) 
    print('FINISH JOB')
    log.info(f"FINISH JOB") 
    log.info("\n") 
    
except Exception as e:
    print('Failed to update database')
    log.info('Failed to update database')
    log.exception(e)
