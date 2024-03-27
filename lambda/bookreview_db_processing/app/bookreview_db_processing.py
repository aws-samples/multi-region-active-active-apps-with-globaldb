###
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this
# software and associated documentation files (the "Software"), to deal in the Software
# without restriction, including without limitation the rights to use, copy, modify,
# merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
# PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
# Copyright Amazon.com, Inc. and its affiliates. All Rights Reserved.
#   SPDX-License-Identifier: MIT
######
import json
import pymysql
import boto3
import logging
import os
import traceback
import codecs
from botocore.exceptions import ClientError
from timeit import default_timer as timer
from datetime import timedelta


logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3 = boto3.client('s3')

dbSecretName = os.environ['DBSecretName']
aws_region = os.environ.get('AWS_REGION')
database_name = os.environ['DB_NAME']




def get_secret(secret_name, region_name):
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
        return get_secret_value_response
    except ClientError as e:
        traceback.print_exc()
        logger.info("Failed with except:")
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            raise e

##write a function to return RDS DB Connection
def get_db_connection():

    secret = get_secret(dbSecretName, aws_region)
    logger.info(f'secret is: {secret}')
    secret_string = secret['SecretString']
    secret_dict = json.loads(secret_string)
    database_token = secret_dict['password']
    database_user = secret_dict['username']
    database_host = secret_dict['host']


    conn = pymysql.connect(host=database_host, user=database_user,
                           passwd=database_token,
                           db=database_name,
                           connect_timeout=5,
                           cursorclass=pymysql.cursors.DictCursor,
                           init_command="SET aurora_replica_read_consistency = 'session'"
                           )
    return conn


#write a function to read json object from s3
def readS3FileAndWriteToAurora(s3Bucket, prefix, conn):
    obj = s3.get_object(Bucket=s3Bucket, Key=prefix)
    logger.info(f'obj is: {obj}')
    body = obj['Body']
    reviews = []
    recordsProcessed = 0

    insertSql = "insert into reviews(reviewer_id, asin, reviewer_name, review_text, summary,overall, " \
                "verified, unix_review_time, review_time) values(%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    i = 0
    with conn.cursor() as cur:

        for ln in codecs.getreader('utf-8')(body):
            try:
                data = json.loads(ln)
                reviews.append([data["reviewerID"],data["asin"], data["reviewerName"], data["reviewText"],data["summary"],
                    data["overall"], data["verified"], data["unixReviewTime"], data["reviewTime"]])
                i += 1
                recordsProcessed += 1
                if i > 500:
                    print("inserting reviews to db")
                    cur.executemany(insertSql, reviews)
                    conn.commit()
                    reviews = []
                    i = 0
            except KeyError as e:
                logger.info("Failed with KeyError: Ignore the data")

        if i > 0:
            print("inserting reviews to db")
            cur.executemany(insertSql, reviews)
            conn.commit()
    return recordsProcessed


def lambda_handler(event, context):
    start = timer()
    logger.info(pymysql.__version__)
    reviews = []
    recordsProcessed = 0
    statusCode = 200
    statusMessage = "Success"
    s3Bucket = event["s3Bucket"]
    prefix = event["prefix"]


    try:
        conn = get_db_connection()

        logger.info(" dbconnection created")
        recordsProcessed = readS3FileAndWriteToAurora(s3Bucket, prefix, conn)
        logger.info("readJsonObjectFromS3 completed")

        conn.close()
        end = timer()
        deltaTime = timedelta(milliseconds=end - start)
        logger.info(f'time taken to process is: {deltaTime.microseconds}')

    except Exception as e:
        traceback.print_exc()
        logger.info("Failed with except:")
        statusCode = 500
        statusMessage = "Failed"

    return {
        'statusCode': statusCode,
        'statusMessage': statusMessage,
        's3Bucket': s3Bucket,
        'prefix': prefix,
        'recordsProcessed': recordsProcessed,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
            'Access-Control-Allow-Credentials': 'true',
            'Content-Type': 'application/json'
        }
    }

