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

import json
import os

from exception import IncorrectUsage


class BALMessage(object):
    def __init__(self, language="en"):
        """
        This module is for obtaining the message string that is mapped to an
        ID available in the messages directory. This module is created
        for forward-compatibility in case internationalization becomes a
        possibility in the Internet Archive.
        """
        self.messagesDir = os.path.dirname(os.path.realpath(__file__))
        self.messagesDir += "/../messages"
        # Hard-coded for now, because only English is supported
        self.language = "en"

    def getMessageFile(self):
        """
        This function is for loading the relevant language file as a
        dictionary.

        Returns: Dict with all the mappings of strings and messages.
        """
        jsonfile = self.messagesDir
        jsonfile += "/%s.json" % (self.language)
        with open(jsonfile, "r") as jsonmessages:
            return json.load(jsonmessages)

    def getMessage(self, identifier):
        """
        This function is used for getting the message for a given string.

        Returns: String with the message.
        """
        messages = self.getMessageFile()
        return messages[identifier]


if __name__ == "__main__":
    BALMessage = BALMessage()
    raise IncorrectUsage(BALMessage.getMessage('exception-incorrectusage'))
