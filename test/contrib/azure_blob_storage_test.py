# -*- coding: utf-8 -*-
#
# Copyright (c) 2013 Mortar Data
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#
from __future__ import print_function

import os
import sys
import tempfile

from target_test import FileSystemTargetTestMixin
from helpers import with_config, unittest, skipOnTravis

from luigi import configuration
from luigi.target import FileSystemException
from luigi.contrib.azure_storage import AzureStorageClient, AzureStorageTarget

STORAGE_ACCOUNT = os.environ['STORAGE_ACCOUNT']
STORAGE_ACCOUNT_KEY = os.environ['STORAGE_ACCOUNT_KEY']
AZURE_TEST_PATH = 'bibdipdata/dev_tests'


class TestAzureTarget(unittest.TestCase, FileSystemTargetTestMixin):

    def setUp(self):
        f = tempfile.NamedTemporaryFile(mode='wb', delete=False)
        self.tempFileContents = (
            b"I'm a temporary file for testing\nAnd this is the second line\n"
            b"This is the third.")
        self.tempFilePath = f.name
        f.write(self.tempFileContents)
        f.close()
        self.addCleanup(os.remove, self.tempFilePath)

    def create_target(self, format=None, path='bibdipdata/dev_tests/tempfile'):
        client = AzureStorageClient(STORAGE_ACCOUNT, STORAGE_ACCOUNT_KEY)
        return AzureStorageTarget(path, client=client, format=format)

    def test_read(self):
        client = AzureStorageClient(STORAGE_ACCOUNT, STORAGE_ACCOUNT_KEY)
        client.put(self.tempFilePath, 'bibdipdata/dev_tests/tempfile')
        t = AzureStorageTarget('bibdipdata/dev_tests/tempfile', client=client)
        read_file = t.open('r')
        file_str = read_file.read()
        self.assertEqual(self.tempFileContents, file_str.encode('utf-8'))

    def test_get_path(self):
        t = self.create_target()
        path = t.path
        self.assertEqual('bibdipdata/dev_tests/tempfile', path)


class TestAzureStorageClient(unittest.TestCase):

    def setUp(self):
        f = tempfile.NamedTemporaryFile(mode='wb', delete=False)
        self.tempFilePath = f.name
        self.tempFileContents = b"I'm a temporary file for testing\n"
        self.tempFileContentsStr = "I'm a temporary file for testing\n"
        f.write(self.tempFileContents)
        f.close()
        self.addCleanup(os.remove, self.tempFilePath)

    def test_init_with_environment_variables(self):
        os.environ['AZURE_STORAGE_ACCOUNT'] = 'foo'
        os.environ['AZURE_STORAGE_ACCOUNT_STORAGE_ACCOUNT_KEY'] = 'bar'
        # Don't read any exsisting config
        old_config_paths = configuration.LuigiConfigParser._config_paths
        configuration.LuigiConfigParser._config_paths = [tempfile.mktemp()]

        azure_storage_client = AzureStorageClient()
        configuration.LuigiConfigParser._config_paths = old_config_paths

        self.assertEqual(azure_storage_client.block_blob_service.account_name, 'foo')
        self.assertEqual(azure_storage_client.block_blob_service.account_key, 'bar')

    @with_config({'azure_storage': {'storage_account': 'foo', 'storage_account_key': 'bar'}})
    def test_init_with_config(self):
        azure_storage_client = AzureStorageClient()
        self.assertEqual(azure_storage_client.block_blob_service.account_name, 'foo')
        self.assertEqual(azure_storage_client.block_blob_service.account_key, 'bar')


    def test_path_to_container_and_blob(self):
        azure_storage_client = AzureStorageClient(STORAGE_ACCOUNT, STORAGE_ACCOUNT_KEY)
        (container, blob_name) = azure_storage_client._path_to_container_and_blob('bibdipdata/dev_tests/putMe')

        self.assertEqual(container, 'bibdipdata')
        self.assertEqual(blob_name, 'dev_tests/putMe')

    def test_put(self):
        azure_storage_client = AzureStorageClient(STORAGE_ACCOUNT, STORAGE_ACCOUNT_KEY)
        azure_storage_client.put(self.tempFilePath, 'bibdipdata/dev_tests/putMe')
        self.assertTrue(azure_storage_client.exists('bibdipdata/dev_tests/putMe'))

    def test_put_string(self):
        azure_storage_client = AzureStorageClient(STORAGE_ACCOUNT, STORAGE_ACCOUNT_KEY)
        azure_storage_client.put_string("SOMESTRING", AZURE_TEST_PATH + '/somestring')
        self.assertTrue(azure_storage_client.exists(AZURE_TEST_PATH + '/somestring'))

    def test_exists(self):
        azure_storage_client = AzureStorageClient(STORAGE_ACCOUNT, STORAGE_ACCOUNT_KEY)

        azure_storage_client.put(self.tempFilePath, AZURE_TEST_PATH + '/tempfile')

        self.assertTrue(azure_storage_client.exists(AZURE_TEST_PATH + '/tempfile'))
        self.assertFalse(azure_storage_client.exists(AZURE_TEST_PATH + '/nope'))


    def test_get(self):
        # put a file on Azure first
        azure_storage_client = AzureStorageClient(STORAGE_ACCOUNT, STORAGE_ACCOUNT_KEY)
        azure_storage_client.put(self.tempFilePath, AZURE_TEST_PATH + '/tempfile')

        tmp_file = tempfile.NamedTemporaryFile(delete=True)
        tmp_file_path = tmp_file.name

        azure_storage_client.get(AZURE_TEST_PATH + '/tempfile', tmp_file_path)
        self.assertEquals(tmp_file.read(), self.tempFileContents)

        tmp_file.close()

    def test_get_as_string(self):
        # put a file on s3 first
        azure_storage_client = AzureStorageClient(STORAGE_ACCOUNT, STORAGE_ACCOUNT_KEY)
        azure_storage_client.put(self.tempFilePath, AZURE_TEST_PATH + '/tempfile')

        contents = azure_storage_client.get_as_string(AZURE_TEST_PATH + '/tempfile')

        self.assertEquals(contents, self.tempFileContentsStr)

    def test_get_as_bytes(self):
        # put a file on s3 first
        azure_storage_client = AzureStorageClient(STORAGE_ACCOUNT, STORAGE_ACCOUNT_KEY)
        azure_storage_client.put(self.tempFilePath, AZURE_TEST_PATH + '/tempfile')

        contents = azure_storage_client.get_as_bytes(AZURE_TEST_PATH + '/tempfile')

        self.assertEquals(contents, self.tempFileContentsStr.encode())

    def test_remove(self):
        azure_storage_client = AzureStorageClient(STORAGE_ACCOUNT, STORAGE_ACCOUNT_KEY)
        azure_storage_client.put(self.tempFilePath, AZURE_TEST_PATH + '/tempfile')

        self.assertFalse(azure_storage_client.remove(AZURE_TEST_PATH + '/doesNotExist'))

        azure_storage_client.put(self.tempFilePath, AZURE_TEST_PATH + '/existingFile0')
        self.assertTrue(azure_storage_client.remove(AZURE_TEST_PATH + '/existingFile0'))
        self.assertFalse(azure_storage_client.exists(AZURE_TEST_PATH + '/existingFile0'))

    def test_copy(self):
        azure_storage_client = AzureStorageClient(STORAGE_ACCOUNT, STORAGE_ACCOUNT_KEY)
        azure_storage_client.put(self.tempFilePath, AZURE_TEST_PATH + '/tempfile.toto')
        copy_results = azure_storage_client.copy(AZURE_TEST_PATH + '/tempfile.toto', AZURE_TEST_PATH + '/tempfile.copied')
        self.assertEqual(copy_results[0], 1)
        self.assertTrue(azure_storage_client.exists(AZURE_TEST_PATH + '/tempfile.copied'))

    def test_move(self):
        azure_storage_client = AzureStorageClient(STORAGE_ACCOUNT, STORAGE_ACCOUNT_KEY)
        azure_storage_client.put(self.tempFilePath, AZURE_TEST_PATH + '/tempfile.toto')
        azure_storage_client.move(AZURE_TEST_PATH + '/tempfile.toto', AZURE_TEST_PATH + '/tempfile.tata')
        self.assertTrue(azure_storage_client.exists(AZURE_TEST_PATH + '/tempfile.tata'))
        self.assertFalse(azure_storage_client.exists(AZURE_TEST_PATH + '/tempfile.toto'))