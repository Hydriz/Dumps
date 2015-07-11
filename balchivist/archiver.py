#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (C) 2015 Hydriz Scholz
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

import time

import internetarchive

from . import BALVERSION


class IncorrectUsage(Exception):
    pass


class BALArchiver(object):
    def __init__(self, identifier='', retries=3, retrysleep=30):
        """
        This module is used for providing regular functions used for
        uploading files into the Internet Archive. It is an extension of
        the internetarchive python library, but with better error handling.

        - identifier (string): The identifier for the item.
        - retries (int): The number of times to retry a request to the server.
        - retrysleep (int): Time (in seconds) to sleep before the next request.
        """
        self.IAItem = internetarchive.Item(identifier, max_retries=retries)
        self.retries = retries

        # Files that are present by default in all Internet Archive items
        self.defaultFiles = [
            '%s_archive.torrent' % (identifier),
            '%s_files.xml' % (identifier),
            '%s_meta.sqlite' % (identifier),
            '%s_meta.xml' % (identifier)
        ]

    def getFileList(self):
        """
        This function is used to get the list of files in an item and excludes
        the default files that are present in all Internet Archive items.

        Returns: List of files in the item excluding default files in
        alphabetical order.
        """
        files = self.IAItem.files
        filelist = []
        for thefile in files:
            filename = thefile['name']
            if filename in self.defaultFiles:
                continue
            else:
                filelist.append(filename)
        return sorted(filelist)

    def uploadFile(self, body, key=None, metadata={}, headers={},
                   queuederive=False, verify=True, debug=False):
        """
        This function will upload a single file to the item on the Internet
        Archive.

        - body (string): The path to the file to upload.
        - key (string): The name of the uploaded file.
        - metadata (dict): The metadata for the Internet Archive item.
        - headers (dict): The headers to send when sending the request.
        - queuederive (boolean): Whether or not to derive the item after the
        file is uploaded.
        - verify (boolean): Whether or not to verify that the file is uploaded.
        - debug (boolean): Whether or not to run this upload in debug mode.
        The actual file will not be uploaded, but the requests sent will be
        returned.

        Returns: True if the file is successfully uploaded, False if errors
        are encountered.

        TODO: Implement multipart uploading.
        """
        if not metadata.get('scanner'):
            scanner = 'Balchivist Python Library %s' % (BALVERSION)
            metadata['scanner'] = scanner

        iaupload = self.IAItem.upload_file
        tries = 0

        while tries < self.retries:
            try:
                iaupload(body, key=key, metadata=metadata, headers=headers,
                         queue_derive=queuederive, verify=verify, debug=debug)
                return True
                break
            except Exception as exception:
                tries += 1
                if debug:
                    print "%s was caught" % (type(exception).__name__)
                    return False
                elif tries == self.retries:
                    return False
                else:
                    time.sleep(60*tries)

    def modifyMetadata(self, metadata, target='metadata', priority=None,
                       debug=False):
        """
        This function will modify the metadata of an item on the Internet
        Archive.

        - metadata (dict): The metadata to modify for the item.
        - target (string): The metadata target to update.
        - priority (int): The priority for the metadata update task.
        - debug (boolean): Whether or not to run this function in debug mode.

        Returns: Dict with the status code and response to the request.
        """
        if not metadata.get('scanner'):
            scanner = 'Balchivist Python Library %s' % (BALVERSION)
            metadata['scanner'] = scanner
        tries = 0

        while tries < self.retries:
            try:
                self.IAItem.modify_metadata(metadata=metadata, target=target,
                                            priority=priority, debug=debug)
                return True
                break
            except Exception as exception:
                tries += 1
                if debug:
                    print "%s was caught" % (type(exception).__name__)
                    return False
                elif tries == self.retries:
                    return False
                else:
                    time.sleep(60*tries)

if __name__ == '__main__':
    raise IncorrectUsage("Script cannot be called directly")
