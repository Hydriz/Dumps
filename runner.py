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

import getopt
import sys
import time

import balchivist
import dumps


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
        self.dumpdate = None
        self.job = "archive"
        self.dumpdir = None
        self.dumptype = None
        self.subject = None
        self.debug = False
        self.resume = False
        self.verbose = False
        # The database table hosting the information on dumps
        self.dbtable = "archive"

    def printv(self, message):
        """
        This function is used to write output into stdout if verbose == True.

        - message (string): The message to output.
        """
        if self.verbose:
            sys.stdout.write("%s\n" % (message))
        else:
            pass

    def getItemsLeft(self, conds={}):
        """
        This function is used to get the number of items left to work with.

        - conds (dict): The conditions to put in the WHERE clause.

        Returns: Int with number of items left to work with.
        """
        output = 0
        cons = []
        for key, val in conds.iteritems():
            cons.append('%s="%s"' % (key, val))
        where = ' AND '.join(cons) + ' AND claimed_by IS NULL;'
        query = [
            'SELECT', 'COUNT(*)',
            'FROM', self.dbtable,
            'WHERE', where
        ]
        results = self.sqldb.execute(' '.join(query))
        if results is None:
            output = 0
        else:
            for result in results:
                output = result[0]
        return output

    def getRandomItem(self, dumptype=None):
        """
        This function is used to get a random unarchived item to work on.

        - dumptype (string): The specific dump type to work on.

        Returns: Dict with the parameters to the archiving scripts.
        """
        output = {}
        if dumptype is None:
            extraconds = ""
        else:
            # Replacement is to avoid SQL injection attacks
            extraconds = 'type="%s" AND ' % (dumptype.replace(" ", "_"))
        query = [
            'SELECT', 'type, subject, dumpdate',
            'FROM', self.dbtable,
            'WHERE', '%sprogress="done" AND is_archived="0"' % (extraconds),
            'AND claimed_by IS NULL AND can_archive=1',
            'ORDER BY', 'RAND()',
            'LIMIT', '1;'
        ]
        results = self.sqldb.execute(' '.join(query))
        for result in results:
            output = {
                'type': result[0],
                'subject': result[1],
                'dumpdate': result[2].strftime("%Y%m%d")
            }

        # Claim the item from the database server
        if self.debug:
            pass
        else:
            self.sqldb.claimItem(params=output)

        return output

    def getRandomArchived(self, dumptype=None):
        """
        This function is used to get a random unchecked item to check.

        - dumptype (string): The specific dump type to work on.

        Returns: Dict with the parameters to the checking scripts.
        """
        output = {}
        if dumptype is None:
            extraconds = ""
        else:
            # Replacement is to avoid SQL injection attacks
            extraconds = 'type="%s" AND ' % (dumptype.replace(" ", "_"))
        query = [
            'SELECT', 'type, subject, dumpdate',
            'FROM', self.dbtable,
            'WHERE', '%sis_archived="1" AND is_checked="0"' % (extraconds),
            'AND claimed_by IS NULL',
            'ORDER BY', 'RAND()',
            'LIMIT', '1;'
        ]
        results = self.sqldb.execute(' '.join(query))
        for result in results:
            output = {
                'type': result[0],
                'subject': result[1],
                'dumpdate': result[2].strftime("%Y%m%d")
            }

        # Claim the item from the database server
        if self.debug:
            pass
        else:
            self.sqldb.claimItem(params=output)

        return output

    def dispatch(self, dumptype, job, subject, dumpdate):
        """
        This function is used to dispatch the given item parameters.

        - dumptype (string): The type of dump.
        - job (string): The job to do for the dump.
        - subject (string): The subject of the dump.
        - dumpdate (string): The date of the dump.
        """
        status = False
        if dumptype == "main":
            # The main Wikimedia database dumps
            msg = "Running %s on the main Wikimedia database " % (job)
            msg += "dump of %s on %s" % (subject, dumpdate)
            self.printv(msg)
            Dumps = dumps.BALDumps(debug=self.debug, verbose=self.verbose)
            if job == "archive":
                status = Dumps.archive(wikidb=subject, dumpdate=dumpdate,
                                       dumpdir=self.dumpdir,
                                       resume=self.resume)
            elif job == "check":
                status = Dumps.check(subject, dumpdate)
        else:
            sys.stderr.write("An unknown type was given")
            return

        params = {
            "type": dumptype,
            "subject": subject,
            "dumpdate": dumpdate
        }
        if self.debug is False:
            if status:
                if job == "archive":
                    self.printv("Marking %s on %s as archived" %
                                (subject, dumpdate))
                    self.sqldb.markArchived(params)
                elif job == "check":
                    self.printv("Marking %s on %s as checked" %
                                (subject, dumpdate))
                    self.sqldb.markChecked(params)
            else:
                if job == "archive":
                    self.printv("Marking %s on %s as failed archive" %
                                (subject, dumpdate))
                    self.sqldb.markFailedArchive(params)
                elif job == "check":
                    self.printv("Marking %s on %s as failed check" %
                                (subject, dumpdate))
                    self.sqldb.markFailedCheck(params)
        else:
            return

    def usage(self, message=None):
        """
        This function is used to show the help message for using this script.

        - message (string): A message to show before the help text.
        """
        if message:
            sys.stderr.write("%s\n\n" % (message))
        text = """Usage: %s [--job archive|check|update]
    [--type main|incr] [--subject <subject>]
    [--date <dumpdate>] [--path <dirname>]
    [--resume] [--debug] [--help]

Options:
    --job (j):     archive -- Upload the dump files to the Internet Archive
                   check   -- Check if the uploaded dump is complete
                   update  -- Update the database for new dumps
                   Default: archive

    --type (t):    main -- Work on the main Wikimedia database dumps
                   Default: Randomly chosen

    --subject (s): The subject name to work on, --date must also be given.
                   E.g.: The wiki database name when working with wikis.
                   Default: Randomly chosen

    --wiki (w):    Alias for --subject.

    --date (d):    The date of the dump to work on for the wiki, --wiki must
                   also be given.
                   Default: Randomly chosen

    --path (p):    The path to the dump directory.
                   Default: Directory given in settings.conf for the --type

    --resume (r):  Resume uploading an item instead of uploading from the
                   start.

    --debug (D):   Don't modify anything, but output the progress.
    --verbose (v): Provide more verbosity.
    --help (h):    Display this help message and exit.

""" % (sys.argv[0])
        sys.stderr.write(text)
        sys.exit(1)

    def archiver(self, subject=None, dumpdate=None):
        """
        This function is used to archive wikis. If subject and dumpdate is
        given, only those will be archived and the script will exit.
        """
        while True:
            if subject is not None and dumpdate is not None:
                self.dispatch(self.dumptype, "archive", subject, dumpdate)
                break
            else:
                conds = {
                    'progress': 'done',
                    'is_archived': '0',
                    'can_archive': '1'
                }
                if self.getItemsLeft(conds) > 0:
                    item = self.getRandomItem(self.dumptype)
                    self.dispatch(item['type'], 'archive', item['subject'],
                                  item['dumpdate'])
                else:
                    if self.debug:
                        sys.stdout.write("Nothing to be done\n")
                        break
                    else:
                        # Allow the script to sleep for 6 hours
                        self.printv("Sleeping for 6 hours, %s" %
                                    (time.strftime("%Y-%m-%d %H:%M:%S",
                                                   time.localtime())))
                        time.sleep(60*60*6)

    def checker(self, subject=None, dumpdate=None):
        """
        This function is used to check wikis. If subject and dumpdate is
        given, only those will be checked and the script will exit.
        """
        while True:
            if subject is not None and dumpdate is not None:
                self.dispatch(self.dumptype, "check", subject, dumpdate)
                break
            else:
                conds = {
                    'progress': 'done',
                    'is_archived': '1',
                    'is_checked': '0'
                }
                if self.getItemsLeft(conds) > 0:
                    item = self.getRandomArchived(self.dumptype)
                    self.dispatch(item['type'], 'check', item['subject'],
                                  item['dumpdate'])
                else:
                    if self.debug:
                        sys.stdout.write("Nothing to be done\n")
                        break
                    else:
                        # Allow the script to sleep for 6 hours
                        self.printv("Sleeping for 6 hours, %s" %
                                    (time.strftime("%Y-%m-%d %H:%M:%S",
                                                   time.localtime())))
                        time.sleep(60*60*6)

    def updater(self):
        """
        This function is used to update the local database of new dumps.
        """
        if self.dumptype == "main":
            # The main Wikimedia database dumps
            Dumps = dumps.BALDumps(debug=self.debug, verbose=self.verbose)
            return Dumps.update()

    def execute(self):
        """
        This function is the main execution function for the archiving scripts.
        """
        shortopts = "d:j:p:t:s:w:Dhrv"
        longopts = [
            "date=",
            "job=",
            "path=",
            "type=",
            "subject=",
            "wiki=",
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
                self.dumpdate = val
            elif opt in ["-j", "--job"]:
                self.job = val
            elif opt in ["-p", "--path"]:
                self.dumpdir = val
            elif opt in ["-t", "--type"]:
                self.dumptype = val
            elif opt in ["-s", "--subject", "-w", "--wiki"]:
                self.subject = val
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

        if self.subject is None and self.dumpdate is not None:
            self.usage("Error: --date was given but not --subject")
        elif self.subject is not None and self.dumpdate is None:
            self.usage("Error: --subject was given but not --date")
        elif self.subject is not None and self.dumpdate is not None and \
                self.dumptype is None:
            self.usage("Error: --subject and --date given but --type wasn't")
        elif self.job == "update" and self.dumptype is None:
            self.usage("Error: --type needed for --job=update")

        if self.job == "archive":
            self.archiver(self.subject, self.dumpdate)
        elif self.job == "check":
            self.checker(self.subject, self.dumpdate)
        elif self.job == "update":
            self.updater()

if __name__ == '__main__':
    BALRunner = BALRunner()
    BALRunner.execute()
