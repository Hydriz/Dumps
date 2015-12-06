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

import sys

from exception import IncorrectUsage
import config
import message


class BALCommon(object):
    def __init__(self, verbose=False, debug=False, log=False):
        """
        This module is used for common functionality that even the package
        itself is using.
        """
        self.verbose = verbose
        self.debug = debug
        self.log = log
        BALConfig = config.BALConfig('main')
        self.logtofile = BALConfig.get('logfile')

    def giveMessage(self, message):
        """
        This function is used for giving a message to the user.

        - message (string): The message to give.
        """
        output = "%s\n" % (message)
        if (self.verbose):
            sys.stdout.write(output)
        else:
            pass

        if (self.debug):
            sys.stderr.write(output)
        else:
            pass

        self.logMessage(output)

    def giveDebugMessage(self, message):
        """
        This function is used for giving a message to the user in debug mode.

        Note: All message sent in verbose mode will also be sent when in debug
        mode.

        - message (string): The message to give.
        """
        output = "%s\n" % (message)
        if (self.debug):
            sys.stderr.write(output)
        else:
            pass

        self.logMessage(output)

    def giveError(self, message):
        """
        This function is used for giving warning/error messages to the user.

        - message (string): The message to give.
        """
        output = "%s\n" % (message)
        sys.stderr.write(output)

        self.logMessage(output)

    def logMessage(self, message):
        """
        This function is used to log to the logfile (given in settings.conf)
        if the --log parameter is given (i.e. self.log == True).

        - message (string): The message to log.
        """
        if (self.log):
            with open(self.logtofile, "a") as logfile:
                logfile.write(message)
        else:
            pass


if __name__ == "__main__":
    BALMessage = message.BALMessage()
    raise IncorrectUsage(BALMessage.getMessage('exception-incorrectusage'))
