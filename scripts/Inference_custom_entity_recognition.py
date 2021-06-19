import csv

def lambda_handler(event, context):
    ####################read first line of dataset to use it for inference###
    import boto3
    s3 =boto3.client('s3')
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    bucket = '<Your Bucket name>' # change to your s3 bucke
    EntityRecognizerArn_job = "arn:aws:comprehend:us-east-1:"+account_id+:"entity-recognizer/Recognizer-blog" #  replace with your Entity Recognizer Arn
    DataAccessRoleArn_user  = "arn:aws:iam::"+account_id+":role/Lambda-S3-Glue-comprehend"  # replace with your IAM user created before
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    key = 'file_location/file_name.txt'
    obj= s3.get_object (Bucket = bucket , Key = key)
    first_line_row= obj ['Body'].read()
    first_line = first_line_row.decode("utf-8")
    first_line_list = first_line.split(",")
    bucket = first_line_list[0]
    key = first_line_list[1]
    obj= s3.get_object (Bucket = bucket , Key = key)
    data = obj['Body'].read().decode('utf-8').splitlines()
    lines = csv.reader(data)
    headers = next(lines)
    first_line_list = next(lines)

    # write to an in-memory raw connection

    with open("/tmp/csv_file.csv", 'w') as file:
         writer = csv.writer(file)
         writer.writerow(first_line_list)

    # upload file from tmp to s3 key
    s3_resource = boto3.resource('s3')
    bucket_object = s3_resource.Bucket(bucket)
    key_output ='row-data-out/row-data-out.csv'
    bucket_object.upload_file('/tmp/csv_file.csv', key_output)
    S3Uri_file = "s3://"+bucket+"/"+key_output
    S3Uri_out = "s3://"+bucket+"/"+"comprehend_output"
    #####################Comprehend Custom create Custom entity  #################################\

    import boto3
    import uuid
    comprehend = boto3.client("comprehend")
    response = comprehend.start_entities_detection_job(
    EntityRecognizerArn=EntityRecognizerArn_job,
    JobName="Detection-Job-Name-{}".format(str(uuid.uuid4())),
    LanguageCode="en",
    DataAccessRoleArn=DataAccessRoleArn_user,
    InputDataConfig={
        "InputFormat": "ONE_DOC_PER_LINE",
        "S3Uri": S3Uri_file
    },
    OutputDataConfig={
        "S3Uri": S3Uri_out
    }
)
