# -*- coding: utf-8 -*-
#
# Copyright 2012-2015 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
Implementation of Simple Storage Service support.
:py:class:`S3Target` is a subclass of the Target class to support S3 file
system operations. The `boto` library is required to use S3 targets.
"""

from __future__ import division

import logging
import os
import os.path

import time
import tempfile
import io


try:
    from urlparse import urlsplit
except ImportError:
    from urllib.parse import urlsplit
import random

try:
    from ConfigParser import NoSectionError
except ImportError:
    from configparser import NoSectionError

from luigi import six

from luigi import configuration
from luigi.format import FileWrapper, get_default_format

from luigi.target import FileSystem, FileSystemException, FileSystemTarget, AtomicLocalFile

logger = logging.getLogger('luigi-interface')

class InvalidDeleteException(FileSystemException):
    pass


class FileNotFoundException(FileSystemException):
    pass


class AzureStorageClient(FileSystem):
    """
    Azure storage client powered by Azure SDK.
    """

    def __init__(self, storage_account=None, storage_account_key=None):

        from azure.storage.blob import BlockBlobService

        options = self._get_azure_storage_config()

        if not storage_account:
            storage_account = options.get('storage_account')

        if not storage_account_key:
            storage_account_key = options.get('storage_account_key')

        # try to find credentials in the environment variable if not available in config file
        if storage_account is None or storage_account_key is None:
            options = self._get_azure_storage_env_variables()
            storage_account = options.get('storage_account')
            storage_account_key = options.get('storage_account_key')

        for key in ['storage_account', 'storage_account_key']:
            if key in options:
                options.pop(key)

        self.block_blob_service = BlockBlobService(storage_account, storage_account_key)

    def exists(self, path):
        """
        Does provided path to blob exist on Azure Storage?
        """
        (container, blob_name) = self._path_to_container_and_blob(path)

        # grab and validate the container
        if not self.block_blob_service.exists(container):
            logger.warn('Container %s does not exist', container)
            return False

        if self.block_blob_service.exists(container, blob_name):
            return True

        logger.debug('Path %s does not exist', path)
        return False

    def remove(self, path, recursive=False, skip_trash=True):
        """
        Remove a blob from Azure storage.
        """
        if not self.exists(path):
            logger.debug('Could not delete %s; path does not exist', path)
            return False

        (container, blob_name) = self._path_to_container_and_blob(path)

        # root
        if self._is_root(blob_name):
            raise InvalidDeleteException('Cannot delete root of container at path %s' % blob_name)

        if not self.block_blob_service.exists(container):
            logger.warn('Container %s does not exist', container)
            return False

        # delete blob if it exists
        if self.exists(path):
            self.block_blob_service.delete_blob(container, blob_name)
            return True
        else:
            return False

    def put(self, local_path, destination_azure_path, metadata=None):
        """
        Creates a new blob from a file path, or updates the content of an existing blob
        :param metadata:  Name-value pairs associated with the blob as metadata.
        :type metadata: dict
        """
        (container, blob_name) = self._path_to_container_and_blob(destination_azure_path)

        if not self.block_blob_service.exists(container):
            logger.warn('Container %s does not exist', container)
            return False

        self.block_blob_service.create_blob_from_path(container, blob_name, local_path, metadata=metadata)

    def put_string(self, content, destination_azure_path, metadata=None):
        """
        Put a string into a blob.

        :param metadata: Name-value pairs associated with the blob as metadata.
        :type metadata: dict
        """
        (container, blob_name) = self._path_to_container_and_blob(destination_azure_path)

        if not self.block_blob_service.exists(container):
            logger.warn('Container %s does not exist', container)
            return False

        self.block_blob_service.create_blob_from_text(container, blob_name, content, metadata=metadata)

    def get(self, azure_path, destination_local_path):
        """
        Get an object stored in Azure and write it to a local path.
        """
        (container, blob_name) = self._path_to_container_and_blob(azure_path)

        if not self.block_blob_service.exists(container):
            logger.warn('Container %s does not exist', container)
            return False

        if self.block_blob_service.exists(container, blob_name):
            self.block_blob_service.get_blob_to_path(container, blob_name, destination_local_path)
        else:
            raise FileNotFoundException

    def get_as_string(self, azure_path):
        """
        Get the contents of a blob stored in Azure storage as a string.
        """
        (container, blob_name) = self._path_to_container_and_blob(azure_path)

        if not self.block_blob_service.exists(container):
            logger.warn('Container %s does not exist', container)
            return False

        if self.block_blob_service.exists(container, blob_name):
            return self.block_blob_service.get_blob_to_text(container, blob_name).content
        else:
            raise FileNotFoundException

    def get_as_bytes(self, azure_path):
        """
        Get an object stored in S3 and write it to a local path.
        """
        (container, blob_name) = self._path_to_container_and_blob(azure_path)

        if not self.block_blob_service.exists(container):
            logger.warn('Container %s does not exist', container)
            return False

        # download the file
        if self.block_blob_service.exists(container, blob_name):
            return self.block_blob_service.get_blob_to_bytes(container, blob_name).content
        else:
            raise FileNotFoundException


    def copy(self, source_path, destination_path):
        """
        Copy object(s) from one S3 location to another. Works for individual keys or entire directories.

        When files are larger than `part_size`, multipart uploading will be used.

        :param source_path: The azure path of the blob to copy from
        :param destination_path: The azure path of the blob to copy to

        :returns tuple (number_of_files_copied, total_size_copied_in_bytes)
        """

        (src_container, src_blob) = self._path_to_container_and_blob(source_path)
        (dst_container, dst_blob) = self._path_to_container_and_blob(destination_path)

        if src_container != dst_container:
            #TODO create an exception for this
            raise ValueError('You cannot copy file between two different containers')

        src_blob = self.block_blob_service.make_blob_url(src_container, src_blob)
        copy_properties = self.block_blob_service.copy_blob(src_container, dst_blob, src_blob)

        while copy_properties.status == 'pending':
            time.sleep(1)
            copy_properties = self.service.get_blob_properties(src_container, dst_blob).properties.copy

        if copy_properties.status == 'success':
            return 1, copy_properties.progress

    def move(self, source_path, destination_path):
        """
        Rename/move an object from one Azure Storage location to another.
        """
        self.copy(source_path, destination_path)
        self.remove(source_path)

    def _get_azure_storage_config(self, key=None):
        try:
            config = dict(configuration.get_config().items('azure_storage'))
        except NoSectionError:
            return {}
        # So what ports etc can be read without us having to specify all dtypes
        for k, v in six.iteritems(config):
            try:
                config[k] = int(v)
            except ValueError:
                pass
        if key:
            return config.get(key)
        return config

    def _get_azure_storage_env_variables(self):
        try:
            return {'storage_account': os.environ['AZURE_STORAGE_ACCOUNT'],
                    'storage_account_key': os.environ['AZURE_STORAGE_ACCOUNT_STORAGE_ACCOUNT_KEY']}
        except KeyError:
            return {}

    def _path_to_container_and_blob(self, path):
        if path[0] == '/':
            path = path[1:]
        if path[-1] == '/':
            path = path[:-1]

        path_components = path.split('/')
        container = path_components[0]
        blob_name = '/'.join(path_components[1:])

        return container, blob_name

    def _is_root(self, key):
        return (len(key) == 0) or (key == '/')

    def _add_path_delimiter(self, key):
        return key if key[-1:] == '/' or key == '' else key + '/'


class AtomicAzureFile(AtomicLocalFile):
    """
    Simple class that writes to a temp file and upload to Azure Storage on close().

    Also cleans up the temp file if close is not invoked.
    """

    def __init__(self, fs, path):
        """
        Initializes an AtomicAzureFile instance.
        :param fs:
        :param path:
        :type path: str
        """
        self._fs = fs
        super(AtomicAzureFile, self).__init__(path)

    def move_to_final_destination(self):
        self._fs.put(self.tmp_path, self.path)

    @property
    def fs(self):
        return self._fs


class AzureStorageTarget(FileSystemTarget):
    """
    Target Azure storage blob object
    """

    fs = None

    def __init__(self, path, format=None, client=None,):
        super(AzureStorageTarget, self).__init__(path)
        if format is None:
            format = get_default_format()

        self.path = path
        self.format = format
        self.fs = client or AzureStorageClient()

    def open(self, mode='r'):
        """
        Open the FileSystem target.

        This method returns a file-like object which can either be read from or written to depending
        on the specified mode.

        :param mode: the mode `r` opens the FileSystemTarget in read-only mode, whereas `w` will
                     open the FileSystemTarget in write mode. Subclasses can implement
                     additional options.
        :type mode: str
        """
        if mode == 'w':
            return self.format.pipe_writer(AtomicAzureFile(self.fs, self.path))

        if mode == 'r':
            temp_dir = os.path.join(tempfile.gettempdir(), 'luigi-contrib-azure')

            try:
                os.mkdir(temp_dir)
            except OSError:
                pass

            self.__tmp_path = temp_dir + '/' + self.path.split('/')[0] + '-luigi-tmp-%09d' % random.randrange(0, 1e10)
            self.fs.get(self.path, self.__tmp_path)

            return self.format.pipe_reader(
                FileWrapper(io.BufferedReader(io.FileIO(self.__tmp_path, 'r')))
            )
        else:
            raise Exception("mode must be 'r' or 'w' (got: %s)" % mode)


