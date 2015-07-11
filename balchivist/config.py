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

import ConfigParser
import os


class IncorrectUsage(Exception):
    pass


class BALConfig(object):
    def __init__(self, section, configfile=None):
        """
        This module is for parsing configuration settings in a common
        configuration file.

        - section (string): The section of the configuration file to look at.
        - configfile (string): The path to the configuration file. The file
        "settings.conf" in the root directory will be used by default.
        """
        if configfile is None:
            self.configfile = os.path.dirname(os.path.realpath(__file__))
            self.configfile += "/../settings.conf"
        else:
            self.configfile = configfile
        self.section = section

    def get(self, variable):
        """
        This function is used to get the value for a given configuration
        variable in a certain section.

        - variable (string): The variable in the section to get the value for.

        Returns: Any type depending on the variable.
        """
        config = ConfigParser.SafeConfigParser()
        config.read(self.configfile)
        return config.get(self.section, variable)

if __name__ == '__main__':
    raise IncorrectUsage("Script cannot be called directly")
