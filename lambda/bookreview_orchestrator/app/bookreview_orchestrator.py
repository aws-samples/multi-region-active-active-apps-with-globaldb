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
import boto3
import logging
from datetime import datetime
import os
from timeit import default_timer as timer
from datetime import timedelta
from datetime import datetime
from aws_utils import *

logger = logging.getLogger()
# logging.basicConfig(level=logging.NOTSET)

s3Client = boto3.client('s3')
processing_s3_bucket = os.environ.get('BOOK_REVIEW_BUCKET')
bucket_prefix = os.environ.get('BOOK_REVIEW_BUCKET_PREFIX')

logger.setLevel(logging.INFO)
logging.basicConfig(level=logging.INFO)

def lambda_handler(event, context):
    chunk_size = int(os.environ.get('CHUNK_SIZE', "100"))
    
    now = datetime.now()  # current date and time

    date_time = now.strftime("%m-%d-%Y-%H-%M-%S")
    print("date and time:", date_time)
    
    if "lambda_out" in event:
        chunk_size_tmp = event["lambda_out"]["payload"]["s3_objects"]
        if chunk_size_tmp < chunk_size:
            chunk_size = chunk_size_tmp
    else:
        numberOfObjects = noOfObjectsInS3(processing_s3_bucket, bucket_prefix)
        if numberOfObjects < chunk_size:
            chunk_size = numberOfObjects
    
    print("chunk size:", chunk_size)
    
    start = timer()
    response = listFilesFromS3(processing_s3_bucket, bucket_prefix, chunk_size)
    book_reviews = []
    for book_review in response:
        book_reviews.append({"s3Bucket": processing_s3_bucket, "prefix": book_review})
    end = timer()
    deltaTime = timedelta(milliseconds=end - start)
    totalMilliseconds = deltaTime.microseconds
    logger.info(f'#files to process {len(response)}')
    logger.info(f'totalMilliseconds is: {totalMilliseconds}')

    return {
        'statusCode': 200,
        'files_to_process': len(response),
        's3Bucket': processing_s3_bucket,
        'bookReviews': book_reviews
    }
