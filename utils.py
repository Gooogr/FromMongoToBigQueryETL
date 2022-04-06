import os
import json
import tqdm

import pymongo
from pymongo import MongoClient

from google.cloud import storage, bigquery
from google.cloud.storage import blob
from google.oauth2 import service_account

###-----------------------Connectors functions-----------------------###

def connect_to_mongodb_database(db_cluster_name, db_database_name, login, password):
	'''
	Set connection to the MongoDB database. Don't forget to add VM's IP to the white list!
	Parameters:
			db_cluster_name - str, cluster name
			db_database_name - str, name of the database from cluster
			login - str, user's login
			password - str, user'password
	Return:
			Databse instance. You can get access to the collections in 
			it with db['collection_name']
	'''
	conn_str = f'mongodb+srv://{login}:{password}@shopify.8bhcu.mongodb.net/{db_cluster_name}?retryWrites=true&w=majority'
	mongodb_client = pymongo.MongoClient(conn_str, 
										 serverSelectionTimeoutMS=5000)
	server_info = mongodb_client.server_info() # raise error in case of incorrect server connection
	db = mongodb_client[db_database_name]  
	return db

def connect_to_google_storage_bucket(json_service_key_path, project_id, bucket_name):
	'''
	Set connection to the Google Storage. Read more here:
	https://cloud.google.com/docs/authentication/getting-started
	Paramters:
		json_service_key_path - str, path to the service JSON key
		project_id - str, project id, check the main project dashbard
		bucket_name - str, target bucket in Google Storage
	Return:
		Bucket instance
	'''
	with open(json_service_key_path) as source:
		info = json.load(source)
	credentials = service_account.Credentials.from_service_account_info(info)
	storage_client = storage.Client(project=project_id, credentials=credentials)
	bucket = storage_client.get_bucket(bucket_name) 
	return bucket

def connect_to_bigquery(json_service_key_path, project_id):
	'''
	Set connection to the Google BigQuery. Read more here:
	https://cloud.google.com/docs/authentication/getting-started
	Paramters:
		json_service_key_path - str, path to the service JSON key
		project_id - str, project id, check the main project dashbard
	Return:
		BigQuery instance
	'''
	with open(json_service_key_path) as source:
		info = json.load(source)
	credentials = service_account.Credentials.from_service_account_info(info)	
	bigquery_client = bigquery.Client(project=project_id, 
	                                  credentials=credentials)	
	return bigquery_client

###-------------------------Helper functions-------------------------###

def save_data_from_mongo(collection_name, database, query={}):
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

def copy_json_to_storage(collection_name, bucket, folder):
	'''
	Send JSON to Google Cloud storage.
	Parameters:
		collection_name - str, MongoDB's collection name 
		bucket - str, target Google Storage bucket
	Optional:
		folder - str, target folder or path to the target folder in bucket
	Return:
		None
	'''
	file_path = os.path.join(folder, f'temp_{collection_name}.json')
	blob = bucket.blob(file_path)
	with open(f'temp_{collection_name}.json', 'rb') as json_file:
		blob.upload_from_file(json_file)
		
def copy_data_from_storage_to_bigquery(collection_name, 
									   client, 	
                                       bucket_name, 
                                       bucket_folder_path, 
                                       project_id, 
                                       dataset_id, 
                                       job_config):
    '''
    Create BigQuery Table from selected linearized JSON file from Google Storage.
    Parameters:
		
    '''
    uri = 'gs://' + os.path.join(bucket_name, bucket_folder_path, 
                                 f'temp_{collection_name}.json') 
    table_id = f"{project_id}.{dataset_id}.{collection_name}"  
    load_job = client.load_table_from_uri(
                            source_uris = uri,
                            destination = table_id,
                            job_config=job_config
                        )
    load_job.result() 
    
def delete_temp_file(collection_name):
	os.remove(f"temp_{collection_name}.json")      
