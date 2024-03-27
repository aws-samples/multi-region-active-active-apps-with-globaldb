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
# This lambda program gets triggered by S3 write event and writes data to dynamodb table

import json
from typing import Tuple

import boto3
import logging
from datetime import datetime
import os
from timeit import default_timer as timer
from datetime import timedelta, datetime
import random
import string
from aws_utils import *

logger = logging.getLogger()
# logging.basicConfig(level=logging.NOTSET)
s3Client = boto3.client('s3')
bucket_prefix_completed = os.environ.get('BOOK_REVIEW_BUCKET_PREFIX_COMPLETED', "book-review/completed")
bucket_prefix = os.environ.get('BOOK_REVIEW_BUCKET_PREFIX', "book-review/reviews")


logger.setLevel(logging.INFO)
logging.basicConfig(level=logging.INFO)


def lambda_handler(event, context):
    start = timer()
    now = datetime.now()  # current date and time
    logger.info(f"Book review process completed step {json.dumps(event)}")

    #if there is nothing to process, just exit
    if len(event['s3Bucket']) == 0:
        return {
            'statusCode': 200,
            'timeTaken': 0,
            's3_objects': 0
        }

    bucket = event['s3Bucket'][0]
    for prefix in event['prefix']:
        #logger.info( f"bucket name {bucket} and prefix {prefix}")
        new_prefix = prefix.replace( bucket_prefix, bucket_prefix_completed)
        source_prefix = f"{bucket}/{prefix}"
        s3Client.copy_object(Bucket=bucket, CopySource=source_prefix, Key=new_prefix)
        s3Client.delete_object( Bucket=bucket, Key=prefix)

    noOfObjects = noOfObjectsInS3(bucket, bucket_prefix)
    end = timer()
    deltaTime = timedelta(milliseconds=end - start)
    totalMilliseconds = deltaTime.microseconds
    logger.info(f' Book review completed step - noOfObjects: {noOfObjects}')
    logger.info(f' Book review completed step - totalMilliseconds is: {totalMilliseconds}')


    return {
        'statusCode': 200,
        'timeTaken': totalMilliseconds,
        's3_objects': noOfObjects
    }
