#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (C) 2015-2016 Hydriz Scholz
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

import subprocess
import time

from . import BALConfig
from exception import IncorrectUsage
import message


class BALMaintenance(object):
    def __init__(self, params={}, sqldb=None):
        """
        This module is used for performing maintenance tasks on the Balchivist
        software.

        - params (dict): Information about what is to be done about a given
        item. The "verbose" and "debug" parameters are necessary.
        - sqldb (object): A call to the BALSqlDb class with the required
        parameters.
        """
        self.verbose = params['verbose']
        self.debug = params['debug']
        self.sqldb = sqldb
        self.config = BALConfig('main')
        self.starttime = None

    def getWarningHeaders(self):
        """
        This function is for giving the necessary warnings to the end user when
        starting up the maintenance mode.

        Note: The sleep time is mandatory. We may implement a --force
        parameter in the future to skip this sleep time if necessary.
        """
        sleep = 5
        print ("Maintenance mode entered!")
        print ("Use Ctrl+C to exit in the next %d seconds if this was not "
               "intended." % (sleep))
        time.sleep(sleep)
        print ("Starting maintenance...")
        time.sleep(1)
        self.starttime = time.time()

    def getEnding(self):
        """
        This function is for providing the ending
        """
        timetaken = time.time() - self.starttime
        print ("Maintenance done! Total time taken: %s seconds" % (timetaken))

    def checkVersion(self):
        """
        This maintenance function is used for checking whether the version of
        the software is up-to-date with the one upstream.
        """
        githash = subprocess.check_output(['git', 'rev-parse', 'HEAD']).strip()
        remoteurlcmd = ['git', 'config', '--get', 'remote.origin.url']
        remoteurl = subprocess.check_output(remoteurlcmd).strip()
        remotecmd = ['git', 'ls-remote', remoteurl, 'HEAD']
        remotehash = subprocess.check_output(remotecmd).strip().split('\t')[0]
        print ("Checking if Balchivist is up-to-date...")
        print ("Current version: %s" % (githash))
        print ("Latest version: %s" % (remotehash))
        if (githash == remotehash):
            print ("Great, Balchivist is indeed up-to-date.")
        else:
            print ("ERROR: A new version is available for download. Please "
                   "run \"git pull\" to update the library to the latest "
                   "version!")

    def execute(self):
        """
        The main function for executing this module.
        """
        # Give an introduction to the maintenance mode
        self.getWarningHeaders()
        # Execute all maintenance tasks in this section
        self.checkVersion()
        # End the maintenance mode
        self.getEnding()

if __name__ == '__main__':
    BALMessage = message.BALMessage()
    raise IncorrectUsage(BALMessage.getMessage('exception-incorrectusage'))
