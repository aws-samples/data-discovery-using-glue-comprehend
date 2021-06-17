import json
import boto3
import csv
import os

#####Get the file name #####

import boto3
s3 =boto3.client('s3')
bucket = 'Your S3 Bucket name' # replace with your S3 bucket name 
account_id = boto3.client("sts").get_caller_identity()["Account"]
key = 'file_location/file_name.txt'
obj= s3.get_object (Bucket = bucket , Key = key)
first_line_row= obj ['Body'].read()
first_line = first_line_row.decode("utf-8")
first_line_list = first_line.split(",")
bucket = first_line_list[0]
key = first_line_list[1]
key_list = key.split("/")
key_prefix= key_list[0]
crawler_name = key_list[1].split(".")[0]
crawler_DB = crawler_name +"_DB"



import sys
sys.path.insert(0, '/glue/lib/installation')
keys = [k for k in sys.modules.keys() if 'boto' in k]
for k in keys:
    if 'boto' in k:
       del sys.modules[k]

import boto3
print('boto3 version')
print(boto3.__version__)

s3 =boto3.client('s3')


# TODO implement #https://www.youtube.com/watch?v=5nuUmlezrHs
#bucket = event ['Records'][0]['s3']['bucket']['name']
#key = event ['Records'][0]['s3']['object']['key']
#key = 'row-data/row_all.csv'
obj= s3.get_object (Bucket = bucket , Key = key)
data = obj['Body'].read().decode('utf-8').splitlines()
lines = csv.reader(data)
headers = next(lines)
first_line_list = next(lines)




escalation_intent_name = os.getenv('ESCALATION_INTENT_NAME', None)

client_comprehend = boto3.client('comprehend')
client_glue = boto3.client('glue')


#####################Comprehend#################################\
comprehend_entities= []

for Column in first_line_list:
    entities=client_comprehend.detect_pii_entities(Text=Column,LanguageCode='en')
    dict_pairs = entities.items()
    pairs_iterator = iter(dict_pairs)
    first_pair = next(pairs_iterator)
    Entities =first_pair[1]
    Entities_len = len(Entities)
    if Entities_len < 1:
        Entities= [{'Score': 0.99, 'Type': 'Null', 'BeginOffset': 0, 'EndOffset': 0}]
    Entities_response =Entities[0]['Type']
    comprehend_entities.append(Entities_response)

 ###############################Update Glue Table ######################################

DatabaseName = crawler_DB
Gluetablename = crawler_name+"_"+key_prefix
glue_table = client_glue.get_table( CatalogId=account_id, DatabaseName= DatabaseName , Name= Gluetablename)
updated_table = dict()
original_table = glue_table['Table']
allowed_keys = ["Name","Description","Owner","LastAccessTime","LastAnalyzedTime","Retention","StorageDescriptor","PartitionKeys","ViewOriginalText","ViewExpandedText","TableType","Parameters"]
for key in allowed_keys:
    if key in original_table:
        updated_table[key] = original_table[key]
table_len = (len(updated_table['StorageDescriptor']['Columns']))
update_comment_count = 0

for comment in comprehend_entities:
    updated_table['StorageDescriptor']['Columns'][update_comment_count]['Comment'] = comment
    glue_table = client_glue.update_table( DatabaseName=DatabaseName,TableInput=updated_table)
    update_comment_count = update_comment_count+1
#####################################################################
