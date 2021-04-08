"""Tests for RDSLogsToS3 lambda.

Run `make testenv` to bring up dependencies then `make test` to run tests.
"""

import os
import unittest

import boto3

from main import get_last_received_timestamp

LOCALSTACK_URL = "http://localhost:4566"
BUCKET = "test-bucket"


class TestGetLastReceivedTimestamp(unittest.TestCase):
    def setUp(self):
        os.environ["AWS_ACCESS_KEY_ID"] = "test"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
        self.s3_client = boto3.client("s3", endpoint_url=LOCALSTACK_URL)
        self.s3 = boto3.resource("s3", endpoint_url=LOCALSTACK_URL)
        self.s3.create_bucket(Bucket=BUCKET)

    def test_reads_int_from_file(self):
        expected = 123456789
        self.s3.Object(BUCKET, "timestamp-file").put(Body=str(expected))
        actual = get_last_received_timestamp(self.s3_client, BUCKET, "timestamp-file")
        self.assertEqual(actual, expected)

    def test_returns_zero_when_file_not_found(self):
        actual = get_last_received_timestamp(self.s3_client, BUCKET, "timestamp-file")
        self.assertEqual(actual, 0)

    def tearDown(self):
        b = self.s3.Bucket(BUCKET)
        b.objects.all().delete()
        b.delete()


if __name__ == '__main__':
    unittest.main()
