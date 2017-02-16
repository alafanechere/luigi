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

import datetime
import itertools
import logging
import os
import os.path

import time
from multiprocessing.pool import ThreadPool

try:
    from urlparse import urlsplit
except ImportError:
    from urllib.parse import urlsplit
import warnings

try:
    from ConfigParser import NoSectionError
except ImportError:
    from configparser import NoSectionError

from luigi import six
from luigi.six.moves import range

from luigi import configuration
from luigi.format import get_default_format
from luigi.parameter import Parameter
from luigi.target import FileAlreadyExists, FileSystem, FileSystemException, FileSystemTarget, AtomicLocalFile, \
    MissingParentDirectory
from luigi.task import ExternalTask

logger = logging.getLogger('luigi-interface')

# two different ways of marking a directory
# with a suffix in S3
S3_DIRECTORY_MARKER_SUFFIX_0 = '_$folder$'
S3_DIRECTORY_MARKER_SUFFIX_1 = '/'


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
        Get an object stored in S3 and write it to a local path.
        """
        (container, blob_name) = self._path_to_container_and_blob(azure_path)

        if not self.block_blob_service.exists(container):
            logger.warn('Container %s does not exist', container)
            return False

        # download the file
        self.block_blob_service.get_blob_to_path(container, blob_name, destination_local_path)

    def get_as_string(self, azure_path):
        """
        Get the contents of a blob stored in Azure storage as a string.
        """
        (container, blob_name) = self._path_to_container_and_blob(azure_path)

        if not self.block_blob_service.exists(container):
            logger.warn('Container %s does not exist', container)
            return False

        return self.block_blob_service.get_blob_to_text(container, blob_name).content

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


class AtomicS3File(AtomicLocalFile):
    """
    An S3 file that writes to a temp file and puts to S3 on close.

    :param kwargs: Keyword arguments are passed to the boto function `initiate_multipart_upload`
    """

    def __init__(self, path, s3_client, **kwargs):
        self.s3_client = s3_client
        super(AtomicS3File, self).__init__(path)
        self.s3_options = kwargs

    def move_to_final_destination(self):
        self.s3_client.put_multipart(self.tmp_path, self.path, **self.s3_options)


class ReadableS3File(object):
    def __init__(self, s3_key):
        self.s3_key = s3_key
        self.buffer = []
        self.closed = False
        self.finished = False

    def read(self, size=0):
        f = self.s3_key.read(size=size)

        # boto will loop on the key forever and it's not what is expected by
        # the python io interface
        # boto/boto#2805
        if f == b'':
            self.finished = True
        if self.finished:
            return b''

        return f

    def close(self):
        self.s3_key.close()
        self.closed = True

    def __del__(self):
        self.close()

    def __exit__(self, exc_type, exc, traceback):
        self.close()

    def __enter__(self):
        return self

    def _add_to_buffer(self, line):
        self.buffer.append(line)

    def _flush_buffer(self):
        output = b''.join(self.buffer)
        self.buffer = []
        return output

    def readable(self):
        return True

    def writable(self):
        return False

    def seekable(self):
        return False

    def __iter__(self):
        key_iter = self.s3_key.__iter__()

        has_next = True
        while has_next:
            try:
                # grab the next chunk
                chunk = next(key_iter)

                # split on newlines, preserving the newline
                for line in chunk.splitlines(True):

                    if not line.endswith(os.linesep):
                        # no newline, so store in buffer
                        self._add_to_buffer(line)
                    else:
                        # newline found, send it out
                        if self.buffer:
                            self._add_to_buffer(line)
                            yield self._flush_buffer()
                        else:
                            yield line
            except StopIteration:
                # send out anything we have left in the buffer
                output = self._flush_buffer()
                if output:
                    yield output
                has_next = False
        self.close()


class S3Target(FileSystemTarget):
    """
    Target S3 file object

    :param kwargs: Keyword arguments are passed to the boto function `initiate_multipart_upload`
    """

    fs = None

    def __init__(self, path, format=None, client=None, **kwargs):
        super(S3Target, self).__init__(path)
        if format is None:
            format = get_default_format()

        self.path = path
        self.format = format
        self.fs = client or S3Client()
        self.s3_options = kwargs

    def open(self, mode='r'):
        if mode not in ('r', 'w'):
            raise ValueError("Unsupported open mode '%s'" % mode)

        if mode == 'r':
            s3_key = self.fs.get_key(self.path)
            if not s3_key:
                raise FileNotFoundException("Could not find file at %s" % self.path)

            fileobj = ReadableS3File(s3_key)
            return self.format.pipe_reader(fileobj)
        else:
            return self.format.pipe_writer(AtomicS3File(self.path, self.fs, **self.s3_options))


class S3FlagTarget(S3Target):
    """
    Defines a target directory with a flag-file (defaults to `_SUCCESS`) used
    to signify job success.

    This checks for two things:

    * the path exists (just like the S3Target)
    * the _SUCCESS file exists within the directory.

    Because Hadoop outputs into a directory and not a single file,
    the path is assumed to be a directory.

    This is meant to be a handy alternative to AtomicS3File.

    The AtomicFile approach can be burdensome for S3 since there are no directories, per se.

    If we have 1,000,000 output files, then we have to rename 1,000,000 objects.
    """

    fs = None

    def __init__(self, path, format=None, client=None, flag='_SUCCESS'):
        """
        Initializes a S3FlagTarget.

        :param path: the directory where the files are stored.
        :type path: str
        :param client:
        :type client:
        :param flag:
        :type flag: str
        """
        if format is None:
            format = get_default_format()

        if path[-1] != "/":
            raise ValueError("S3FlagTarget requires the path to be to a "
                             "directory.  It must end with a slash ( / ).")
        super(S3FlagTarget, self).__init__(path, format, client)
        self.flag = flag

    def exists(self):
        hadoopSemaphore = self.path + self.flag
        return self.fs.exists(hadoopSemaphore)


class S3EmrTarget(S3FlagTarget):
    """
    Deprecated. Use :py:class:`S3FlagTarget`
    """

    def __init__(self, *args, **kwargs):
        warnings.warn("S3EmrTarget is deprecated. Please use S3FlagTarget")
        super(S3EmrTarget, self).__init__(*args, **kwargs)


class S3PathTask(ExternalTask):
    """
    A external task that to require existence of a path in S3.
    """
    path = Parameter()

    def output(self):
        return S3Target(self.path)


class S3EmrTask(ExternalTask):
    """
    An external task that requires the existence of EMR output in S3.
    """
    path = Parameter()

    def output(self):
        return S3EmrTarget(self.path)


class S3FlagTask(ExternalTask):
    """
    An external task that requires the existence of EMR output in S3.
    """
    path = Parameter()
    flag = Parameter(default=None)

    def output(self):
        return S3FlagTarget(self.path, flag=self.flag)
