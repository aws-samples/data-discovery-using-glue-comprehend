def lambda_handler(event, context):


    #####################Comprehend create Custom entity  #################################\
    ######## 1- creat Lambda function Author from scratch with runtime Python 3.8
    ########2- Use Lambda-S3-Glue-comprehend role
    ########3- Increase Timeout to 2 min
     #Note that the S3 bucket must be in the same region of Comprehend.
    import boto3
    import uuid
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    comprehend = boto3.client("comprehend")

    response = comprehend.create_entity_recognizer(
    RecognizerName="Recognizer-blog".format(str(uuid.uuid4())),
    LanguageCode="en",
    DataAccessRoleArn  = "arn:aws:iam::"+account_id+":role/Lambda-S3-Glue-comprehend", 
    InputDataConfig={
        'DataFormat': 'COMPREHEND_CSV',
        "EntityTypes": [
            {"Type": "customer information ID",
            },
            {"Type": "current level of education",
            },
            {"Type": "customer credit rating",
            }

        ],
        'Documents': {
            'S3Uri': 's3://<Your S3 Bucket>/traning/training_dataset.csv'},

        'EntityList': {
        'S3Uri': 's3://<Your S3 Bucket>/traning/labels_entity_list.csv'},


    } )
