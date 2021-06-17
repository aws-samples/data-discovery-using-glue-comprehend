import json
import boto3
import csv
import boto3
account_id = boto3.client("sts").get_caller_identity()["Account"]
Glue_role_arn = 'arn:aws:iam::'+account_id+":role/Lambda-S3-Glue-comprehend" # replace with your IAM role created in step X
client_glue = boto3.client('glue')
iam = boto3.client('iam')
s3 =boto3.client('s3')


def main(event, context):
    bucket = event ['Records'][0]['s3']['bucket']['name']
    key = event ['Records'][0]['s3']['object']['key']
    path = "s3://"+bucket+"/"+key
    key_list = key.split("/")
    key_prefix = key_list[0]

    key_list = key.split("/")
    table_name = key_list[1].split(".")[0]
    crawler_DB = table_name +"_DB"
    response_crawler_name_list = client_glue.list_crawlers(MaxResults=123)
    crawler_name_list = (response_crawler_name_list)['CrawlerNames']
    #check =  crawler_name in crawler_name_list
    check =  "glue_crawler_comprehend" in crawler_name_list
    if check is True:
        response_delete_crawler = client_glue.delete_crawler(Name='glue_crawler_comprehend')


    ##########list DB and escape recreate DB if it's already created""#######
    response_db_list = client_glue.get_databases(CatalogId=account_id,ResourceShareType='ALL')
    DB_list = (response_db_list)['DatabaseList']
    db_name_list=[]
    db_index=0
    for db_name in DB_list:
        db_name_list.append(DB_list[db_index]['Name'])
        db_index += 1
    check_db =  crawler_DB.lower() in  db_name_list

    if check_db is False:
        #creat DB for Glue
        response_database = client_glue.create_database(
        DatabaseInput={
        'Name': crawler_DB,
        'Description': 'glue_crawler_comprehend',
        'LocationUri': 'string',
        'Parameters': {'string': 'string'} })


    #creat Crawler
    table_prefix = table_name+"_"
    path_crawler = "s3://"+bucket+"/"+key_prefix
    response_crawler = client_glue.create_crawler(
    #Name=crawler_name,
    Name="glue_crawler_comprehend",
    Role=Glue_role_arn,
    DatabaseName=crawler_DB,
    Description='glue_crawler_comprehend',
    Targets={'S3Targets': [
                {'Path': path_crawler},],} ,
                TablePrefix=table_prefix,

        )


    #run crawler
    #response = client_glue.start_crawler(Name=crawler_name)
    response = client_glue.start_crawler(Name= "glue_crawler_comprehend")
    #write the file name in text file to read it later in Glue job
    file_name = "file_name.txt"
    file_location = "file_location/" + file_name
    lambda_path = "/tmp/" + file_name
    s3_path = bucket+","+ key
    string = s3_path
    encoded_string = string.encode("utf-8")
    s3_resource = boto3.resource("s3")
    s3_resource.Bucket(bucket).put_object(Key=file_location, Body=encoded_string)
