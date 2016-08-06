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

import argparse
import json
import time

import balchivist
import modules


class BALRunner(object):
    def __init__(self):
        """
        This script is the main runner script that will call the individual
        archiving scripts given information from the SQL database. This script
        is designed to run indefinitely.
        """
        config = balchivist.BALConfig('main')
        self.sqldb = balchivist.BALSqlDb(database=config.get('database'),
                                         host=config.get('host'),
                                         default=config.get('defaults_file'))
        self.modules = json.loads(config.get('modules'))
        self.message = balchivist.BALMessage()
        # The database table hosting the information on dumps
        self.dbtable = "archive"

    def execute(self):
        """
        This function is the main execution function for the archiving scripts.
        """
        IncorrectUsage = balchivist.exception.IncorrectUsage
        version = balchivist.BALVERSION
        types = self.modules + ["maintenance"]
        # Main parser for all generic arguments
        parser = argparse.ArgumentParser(
            description="A Python library for archiving datasets to the "
                        "Internet Archive.",
            epilog="For more information, visit the GitHub repository: "
                   "https://github.com/Hydriz/Balchivist",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
        parser.add_argument("-V", "--version", action="version",
                            version="Balchivist Python Library %s" % (version))

        # Argument group for general options
        generalopts = parser.add_argument_group(
            title="General options",
            description="Generic options when executing the runner."
        )
        generalopts.add_argument("-D", "--debug", action="store_true",
                                 default=False,
                                 help="Don't modify anything, but output the "
                                 "progress.")
        generalopts.add_argument("-t", "--type", action="store", dest="module",
                                 choices=types,
                                 help="The type of dataset to work on. "
                                 "Use \"maintenance\" to perform maintenance "
                                 "tasks on the software.")
        generalopts.add_argument("-v", "--verbose", action="store_true",
                                 default=False,
                                 help="Provide more verbosity in output.")
        generalopts.add_argument("-c", "--crontab", action="store_true",
                                 default=False,
                                 help="Crontab mode: Exit when everything is "
                                 "done instead of sleeping. Useful if you do "
                                 "not intend to run the script forever.")

        # Declare all the necessary arguments used by each individual modules
        if (self.modules == list()):
            # There are no modules listed, exit the script now
            msg = self.message.getMessage('exception-nomodules')
            raise IncorrectUsage(msg)
        else:
            pass

        for module in self.modules:
            classname = "BALM" + module.title()
            ClassModule = getattr(modules, classname)(argparse=True)
            ClassModule.argparse(parser=parser)

        # Let's parse the arguments given by the user now
        args = parser.parse_args()
        common = balchivist.BALCommon(verbose=args.verbose, debug=args.debug)

        params = {
            "verbose": args.verbose,
            "debug": args.debug
        }
        while True:
            if (args.module is not None) and (args.module != "maintenance"):
                classtype = "BALM" + args.module.title()
                ClassModule = getattr(modules, classtype)(params=params,
                                                          sqldb=self.sqldb)
                ClassModule.execute(args=args)
                break
            elif (args.module == "maintenance"):
                BALMaintenance = balchivist.BALMaintenance(params=params,
                                                           sqldb=self.sqldb)
                BALMaintenance.execute()
                break
            else:
                for module in self.modules:
                    classtype = "BALM" + module.title()
                    ClassModule = getattr(modules, classtype)(params=params,
                                                              sqldb=self.sqldb)
                    status = ClassModule.execute()
                    if (status):
                        continue
                    else:
                        # An error has occurred, exit immediately
                        break

            # Determine if we should exit the script now
            if (args.debug):
                common.giveMessage("Nothing to be done!")
                break
            elif (args.crontab):
                # Crontab mode is active, exit when everything is done
                break
            else:
                # Allow the script to sleep for 6 hours
                timenow = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                common.giveMessage("Sleeping for 6 hours, %s" % (timenow))
                time.sleep(60*60*6)


if __name__ == '__main__':
    BALRunner = BALRunner()
    BALRunner.execute()
