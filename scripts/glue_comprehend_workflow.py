import json
import os
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Import Boto 3 for AWS Glue
import boto3
client = boto3.client('glue')

# Variables for the job:
glueJobName = "Glue_Comprehend_Job" # replace with your glue job name

# Define Lambda function
def lambda_handler(event, context):

    response = client.start_job_run(JobName = glueJobName)

    return response
