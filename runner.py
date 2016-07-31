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

import argparse
import getopt
import json
import sys
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
        self.common = balchivist.BALCommon()
        self.date = None
        self.job = None
        self.path = None
        self.type = None
        self.subject = None
        self.debug = False
        self.resume = False
        self.verbose = False
        self.crontab = False
        # The database table hosting the information on dumps
        self.dbtable = "archive"

    def dispatch(self, module=None):
        """
        This function is used to dispatch to the individual modules given the
        information provided by the user.

        - module (string): The module to use.

        Returns True if operation is successful, False if an error occurred.
        """
        params = {
            'resume': self.resume,
            'verbose': self.verbose,
            'debug': self.debug
        }
        itemdetails = {
            'job': self.job,
            'path': self.path,
            'subject': self.subject,
            'date': self.date
        }
        if module in self.modules:
            classtype = "BALM" + module.title()
        else:
            msg = self.message.getMessage('error-unknowntype')
            msg += "\nModule %s is not supported!" % (module)
            self.usage(msg)

        ClassModule = getattr(modules, classtype)(params=params,
                                                  sqldb=self.sqldb)
        return ClassModule.execute(params=itemdetails)

    def usage(self, message=None):
        """
        This function is used to show the help message for using this script.

        - message (string): A message to show before the help text.
        """
        if message:
            sys.stderr.write("%s\n\n" % (message))
        text = """Usage: %s [--job <job>] [--type <type>]
    [--subject <subject>] [--date <date>]
    [--path <dirname>]
    [--resume] [--crontab] [--debug]
    [--verbose] [--help]

Options:
    --job (j):      The job to execute for the given --type. Consult the
                    documentation for more details on what jobs are available
                    for each module.

    --type (t):     dumps -- Work on the main Wikimedia database dumps
                    Default: Randomly chosen

    --subject (s):  The subject name to work on, --date must also be given.
                    E.g.: The wiki database name when working with wikis.
                    Default: Randomly chosen

    --wiki (w):     Alias for --subject.

    --date (d):     The date of the dump to work on for the wiki, --wiki must
                    also be given.
                    Default: Randomly chosen

    --path (p):     The path to the dump directory.
                    Default: Directory given in settings.conf for the --type

    --resume (r):   Resume uploading an item instead of uploading from the
                    start.

    --crontab (c):  Crontab mode: Exit when everything is done instead of
                    sleeping for 6 hours. Useful if you do not intend to run
                    the script forever.

    --debug (D):    Don't modify anything, but output the progress.
    --verbose (v):  Provide more verbosity.
    --help (h):     Display this help message and exit.

""" % (sys.argv[0])
        sys.stderr.write(text)
        sys.exit(1)

    def execute(self):
        """
        This function is the main execution function for the archiving scripts.
        """
        shortopts = "d:j:p:t:s:w:cDhrv"
        longopts = [
            "date=",
            "job=",
            "path=",
            "type=",
            "subject=",
            "wiki=",
            "crontab",
            "debug",
            "help",
            "resume",
            "verbose"
        ]
        try:
            options, rem = getopt.gnu_getopt(sys.argv[1:], shortopts, longopts)
        except getopt.GetoptError as error:
            self.usage("Unknown option given: %s" % (str(error)))

        for opt, val in options:
            if opt in ["-d", "--date"]:
                self.date = val
            elif opt in ["-j", "--job"]:
                self.job = val
            elif opt in ["-p", "--path"]:
                self.path = val
            elif opt in ["-t", "--type"]:
                self.type = val
            elif opt in ["-s", "--subject", "-w", "--wiki"]:
                self.subject = val
            elif opt in ["-c", "--crontab"]:
                self.crontab = True
            elif opt in ["-D", "--debug"]:
                self.debug = True
            elif opt in ["-h", "--help"]:
                self.usage()
            elif opt in ["-r", "--resume"]:
                self.resume = True
            elif opt in ["-v", "--verbose"]:
                self.verbose = True
            else:
                self.usage("Unknown option given: %s" % (opt))

        self.common = balchivist.BALCommon(verbose=self.verbose,
                                           debug=self.debug)

        while True:
            if (self.subject is not None and self.date is not None):
                # --subject and --date are specified, work on them only.
                if (self.type is None):
                    self.usage("Error: Please specify a --type!")
                    break
                else:
                    self.dispatch(module=self.type)
                    break
            elif (self.type is not None):
                # --type was given, so we assume that the user only wants to
                # be working on the given type and no other modules.
                self.dispatch(module=self.type)
            else:
                # TODO: We should be able to randomize between different
                # modules so that the backlog can be cleared evenly.
                for module in self.modules:
                    status = self.dispatch(module=module)
                    if (status):
                        continue
                    else:
                        # An error has occurred, exit immediately
                        self.usage()
                        break

            # Determine if we should exit the script now
            if (self.debug):
                self.common.giveMessage("Nothing to be done!")
                break
            elif (self.crontab):
                # Crontab mode is active, exit when everything is done
                break
            else:
                # Allow the script to sleep for 6 hours
                timenow = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                self.common.giveMessage("Sleeping for 6 hours, %s" % (timenow))
                time.sleep(60*60*6)

    def executeArgParse(self):
        """
        This function is the main execution function for the archiving scripts.

        Note that this function uses the argparse module and will replace the
        original execute function above once it is ready.

        Some notes:
        * Before the "args = parser.parse_args()" line, we need to be able to
        parse the required arguments for every module so that we know what to
        accept as arguments.
        """
        IncorrectUsage = balchivist.exception.IncorrectUsage
        # Main parser for all generic arguments
        parser = argparse.ArgumentParser(
            description="A Python library for archiving datasets to the "
                        "Internet Archive.",
            epilog="For more information, visit the GitHub repository: "
                   "https://github.com/Hydriz/Balchivist"
        )
        parser.add_argument("-D", "--debug", action="store_true",
                            default=False,
                            help="Don't modify anything, but output the "
                                 "progress.")
        parser.add_argument("-v", "--verbose", action="store_true",
                            default=False,
                            help="Provide more verbosity in output.")
        parser.add_argument("-c", "--crontab", action="store_true",
                            default=False,
                            help="Crontab mode: Exit when everything is done "
                                 "instead of sleeping. Useful if you do not "
                                 "intend to run the script forever.")
        parser.add_argument("-a", "--auto", action="store_true",
                            default=False,
                            help="Autonomous mode: Automatically pick an item "
                            "that needs to be worked on. Can be used together "
                            "with --crontab to exit once all items are "
                            "completed, else the script will sleep for a "
                            "certain amount of time.")

        # Process the arguments that we currently have so that we know if the
        # next few arguments is still needed
        args, remainders = parser.parse_known_args()
        if (args.auto):
            self.autonomous(debug=args.debug, verbose=args.verbose,
                            crontab=args.crontab)
        else:
            pass

        # Declare all the necessary arguments used by each individual modules
        if (self.modules == list()):
            # There are no modules listed, exit the script now
            msg = self.message.getMessage('exception-nomodules')
            raise IncorrectUsage(msg)
        else:
            pass

        subparser = parser.add_subparsers(
            title="Dataset types",
            description="The type of datasets to work on."
        )
        for module in self.modules:
            classname = "BALM" + module.title()
            ClassModule = getattr(modules, classname)(argparse=True)
            ClassModule.argparse(subparser=subparser)

        args = parser.parse_args()
        params = {
            'resume': args.resume,
            'verbose': args.verbose,
            'debug': args.debug
        }
        if (args.subparser_name in self.modules):
            classtype = "BALM" + args.subparser_name.title()
        else:
            msg = self.message.getMessage('error-unknowntype')
            msg += "\nModule %s is not supported!" % (args.subparser_name)
            raise IncorrectUsage(msg)

        ClassModule = getattr(modules, classtype)(params=params,
                                                  sqldb=self.sqldb)
        return ClassModule.execute(args=args)


if __name__ == '__main__':
    BALRunner = BALRunner()
    BALRunner.execute()
