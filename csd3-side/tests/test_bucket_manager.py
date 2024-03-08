import os
import unittest
from unittest.mock import patch
from bucket_manager import get_keys

class TestBucketManager(unittest.TestCase):
    @patch.dict(os.environ, {'ECHO_ACCESS_KEY': 's3_access_key', 'ECHO_SECRET_KEY': 's3_secret_key'})
    def test_get_keys_s3(self):
        expected_keys = {'access_key': 's3_access_key', 'secret_key': 's3_secret_key'}
        keys = get_keys(api='S3')
        self.assertEqual(keys, expected_keys)

    @patch.dict(os.environ, {'ECHO_SWIFT_USER': 'swift_user', 'ECHO_SWIFT_SECRET_KEY': 'swift_secret_key'})
    def test_get_keys_swift(self):
        expected_keys = {'user': 'swift_user', 'secret_key': 'swift_secret_key'}
        keys = get_keys(api='Swift')
        self.assertEqual(keys, expected_keys)

    def test_get_keys_invalid_api(self):
        with self.assertRaises(KeyError):
            get_keys(api='InvalidAPI')

if __name__ == '__main__':
    unittest.main()