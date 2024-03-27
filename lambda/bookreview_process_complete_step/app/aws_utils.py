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
import boto3
import json

_s3Client = None

def getS3Client():
    global _s3Client
    if( _s3Client is None):
        _s3Client = boto3.client('s3')
    return _s3Client

def writeTextFileFromS3( bucketName, prefix, content ):
    s3Client = getS3Client()
    print( "writing to bucket ", bucketName, " key ", prefix)
    #tempfile = f"/tmp/{prefix}.json"
    #with open(tempfile, "w") as outfile:
    #    json.dump(content, outfile)
    s3Client.put_object(Bucket=bucketName, Key=prefix, Body=json.dumps(content).encode('utf8'))
    #data = s3Client.get_object(Bucket=bucketName, Key=prefix)
    #contents = data['Body'].read()
    return prefix

def readTextFileFromS3( bucketName, prefix ):
    s3Client = getS3Client()
    print( "reading from bucket ", bucketName, " key ", prefix)
    data = s3Client.get_object(Bucket=bucketName, Key=prefix)
    contents = data['Body'].read()
    return contents.decode("utf-8")

def split_s3_path(s3_path):
    path_parts = s3_path.replace("s3://","").split("/")
    bucket = path_parts.pop(0)
    key = "/".join(path_parts)
    return bucket, key

def readJSONFileFromS3( bucketName, prefix ):
    s3Client = getS3Client()
    print( "reading from bucket ", bucketName, " key ", prefix)
    data = s3Client.get_object(Bucket=bucketName, Key=prefix)
    contents = data['Body'].read()
    return contents.decode("utf-8")


def listFilesFromS3( bucketName, prefix, max_limit ):
    s3Client = getS3Client()
    print( "list file from bucket ", bucketName, " key ", prefix)
    data = s3Client.list_objects_v2(Bucket=bucketName, Prefix=prefix)
    print( "s3 list returned ", data['KeyCount'])
    #iterate thru the list to get the max number of objects
    no_keys = 0
    keys = []
    while no_keys < max_limit:
        for content in data['Contents']:
            key = content["Key"]
            if "json" not in key:
                print("ignoring key which is not json ", key)
                continue

            no_keys += 1
            keys.append(content["Key"])
            if no_keys >= max_limit:
                return keys
        if data['IsTruncated'] and no_keys < max_limit:
            data = s3Client.list_objects_v2(Bucket=bucketName, Prefix=prefix, ContinuationToken=data['NextContinuationToken'])
            print("s3 list returned ", data['KeyCount'])
    return keys

def noOfObjectsInS3( bucketName, prefix):
    s3Client = getS3Client()

    data = s3Client.list_objects_v2(Bucket=bucketName, Prefix=prefix)

    no_of_objects = 0
    for object_data in data['Contents']:
        key = object_data['Key']
        if key.endswith('.json'):
            no_of_objects += 1

    print( 'number of s3 objects=', no_of_objects)
    return no_of_objects






