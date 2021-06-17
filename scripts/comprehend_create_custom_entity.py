def lambda_handler(event, context):


    #####################Comprehend create Custom entity  #################################\
     #Note that the S3 bucket must be in the same region of Comprehend.
    import boto3
    import uuid
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    comprehend = boto3.client("comprehend", region_name="us-east-1")

    response = comprehend.create_entity_recognizer(
    RecognizerName="Recognizer-blog-v3".format(str(uuid.uuid4())),
    LanguageCode="en",
    DataAccessRoleArn_user  = "arn:aws:iam::"+account_id+":role/Lambda-S3-Glue-comprehend",  # replace with your IAM user created before
    InputDataConfig={
        'DataFormat': 'COMPREHEND_CSV',
        "EntityTypes": [
            {"Type": "customer_id",
            },
            {"Type": "education_status",
            },
            {"Type": "credit_rating",
            }

        ],
        'Documents': {
            'S3Uri': 's3://<Your S3 Bucket>/traning/training_dataset.csv'},

        'EntityList': {
        'S3Uri': 's3://<Your S3 Bucket>/traning/labels_entity_list.csv'},


    } )
