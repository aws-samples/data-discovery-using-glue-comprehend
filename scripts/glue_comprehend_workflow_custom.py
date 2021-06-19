# read comprehend job output https://labs.consol.de/aws/cloud/machine-learning/2020/11/03/AWS-Comprehend-and-the-output.html
import os, boto3
import json
import csv
import tarfile
import ast
s3 =boto3.client('s3')
comprehend = boto3.client('comprehend')
client_glue = boto3.client('glue')
s3_resource = boto3.resource('s3')
bucket ="<Your S3 bucket name>"
key = "comprehend_output/<Random Number>/output/output.tar.gz" # change with comprehend output file location
account_id = boto3.client("sts").get_caller_identity()["Account"]
key_filename = 'file_location/file_name.txt'
obj= s3.get_object (Bucket = bucket , Key = key_filename)
first_line_row= obj ['Body'].read()
first_line = first_line_row.decode("utf-8")
first_line_list = first_line.split(",")
bucket = first_line_list[0]
key_filename = first_line_list[1]
key_list = key_filename.split("/")
key_prefix= key_list[0]
crawler_name = key_list[1].split(".")[0]
crawler_DB = crawler_name +"_db"
Gluetablename = crawler_name+"_"+key_prefix
s3_resource.Bucket(bucket).download_file(key, 'output.tar.gz')
tmp='../tmp/'
#our exracted, but not yet flattened output
json_filename=tmp+'Entities.json'
#now simply extract  the local file
tar = tarfile.open(tmp+'output.tar.gz', "r:gz")
tar.extractall(path=tmp)
tar.close()
#give the file a propper name
os.rename(tmp+'output',json_filename)
#cleanup
os.remove(tmp+'output.tar.gz')
import json

#location of the flatetned output
flat_json = tmp+'flat.json'

# open json-like file, that we have extracted in step 1
f = open(json_filename, 'r')

#variables for the parsed output document
doc=""
line_out=""
flat_list=[]

# read each line of the file
while True:
    line = f.readline()
    if not line: #don't forget to stop the loop ;)
        break
    else:
        #read each line as json
        j = json.loads(line)

        #if there are entities, write a line for each match
        if len(j['Entities']) > 0:
            line_out = ""
            for index, val in enumerate(j['Entities']):
                val['Line']=j['Line']
                val['File']=j['File']
                line_out += json.dumps(val)+'\n'
            doc += line_out
        #if there is no entity, remove the key and add the rest
        else:
            j.pop('Entities')
            line_out = json.dumps(j)+'\n'
            doc += line_out

flat_list= doc.splitlines()
f.close()

# write the final output
f = open(flat_json, "w")
f.write(doc)
f.close()

#read Glue Table
glue_table = client_glue.get_table( CatalogId=account_id, DatabaseName=crawler_DB, Name=Gluetablename)
updated_table = dict()
original_table = glue_table['Table']
allowed_keys = ["Name","Description","Owner","LastAccessTime","LastAnalyzedTime","Retention","StorageDescriptor","PartitionKeys","ViewOriginalText","ViewExpandedText","TableType","Parameters"]
for key in allowed_keys:
    if key in original_table:
        updated_table[key] = original_table[key]
table_len = (len(updated_table['StorageDescriptor']['Columns']))
update_comment_count = 0
#read the one line file to try to map it to the inference output from custom comprehend
obj= s3.get_object (Bucket = bucket , Key = 'row-data-out/row-data-out.csv')
first_line_row= obj ['Body'].read()
first_line = first_line_row.decode("utf-8")
first_line_list = first_line.split(",")
print (first_line_list)
#read the Glue flat file


flat_list_index = 0
for i in flat_list:
    #read flatted file and extract BeginOffset, EndOffset, detected_text_comprehend, detected_text_comprehend_comment
    dic_comprehend_output = ast.literal_eval(flat_list[flat_list_index])
    BeginOffset = dic_comprehend_output['BeginOffset']
    EndOffset = dic_comprehend_output['EndOffset']
    detected_text_comprehend = dic_comprehend_output['Text']
    detected_text_comprehend_comment = dic_comprehend_output['Type']
    # use row data to get the glue index by matching the orginal text with the detected comprehent text
    detected_text =first_line[BeginOffset:EndOffset]
    detected_text_index =[]
    detected_text_index =  [i for i, s in enumerate(first_line_list) if detected_text in s]
    detected_text_index = int(detected_text_index[0])
    updated_table['StorageDescriptor']['Columns'][detected_text_index]['Comment'] = detected_text_comprehend_comment
    glue_table = client_glue.update_table( DatabaseName=crawler_DB,TableInput=updated_table)
    flat_list_index = flat_list_index+1
