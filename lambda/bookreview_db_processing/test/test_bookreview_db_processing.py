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
#write unit test for bookreview_db_processing
import json
import logging
import unittest
from bookreview_db_processing import lambda_handler

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class TestBookReviewDBProcessing(unittest.TestCase):
    def test_get_book_reviews(self):
        event = '{"s3Bucket":"bookreviewprocessingappea-auroraglobaldbactiveacti-azbg0egntgmd", "prefix":"book-review/reviews-bkp/book_reviewtu.json"}'

        response = lambda_handler(json.loads(event), "")
        assert response["statusCode"] == 200
        logger.info(f"response  {json.dumps(response)}")

