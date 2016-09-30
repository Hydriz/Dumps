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

import datetime
import os
import socket
import time
import urllib

import balchivist


class BALMMediacounts(object):
    def __init__(self, params={}, argparse=False, sqldb=None):
        """
        This module is for archiving the statistics on visits to media files
        provided by the Wikimedia Foundation (available at
        <https://dumps.wikimedia.org/other/mediacounts/>) to the Internet
        Archive.

        - argparse (boolean): Whether or not the class was called during the
        argparse stage.
        - params (dict): Information about what is to be done about a given
        item. The "verbose" and "debug" parameters are necessary.
        - sqldb (object): A call to the BALSqlDb class with the required
        parameters.
        """
        self.title = "Wikimedia statistics files for media files visits on %s"
        self.desc = "This is the Wikimedia statistics files for visits to "
        self.desc += "media files on upload.wikimedia.org on %s."

        self.config = balchivist.BALConfig("mediacounts")
        self.sqldb = sqldb
        self.dbtable = "mediacounts"
        self.conv = balchivist.BALConverter()
        self.hostname = socket.gethostname()
        self.tempdir = self.config.get('dumpdir')
        self.filelist = [
            "mediacounts.%s.v00.tsv.bz2"
            "mediacounts.top1000.%s.v00.csv.zip"
        ]

        if (argparse):
            self.verbose = False
            self.debug = False
        else:
            self.verbose = params['verbose']
            self.debug = params['debug']

        self.jobs = [
            "archive",
            "check",
            "update"
        ]
        # A size hint for the Internet Archive, currently set at 100GB
        self.sizehint = "107374182400"
        self.common = balchivist.BALCommon(verbose=self.verbose,
                                           debug=self.debug)

    def argparse(self, parser=None):
        """
        This function is used for declaring the valid arguments specific to
        this module and should only be used during the argparse stage.

        - parser (object): The parser object.
        """
        group = parser.add_argument_group(
            title="Media files visits statistics",
            description="Statistics on visits to media files."
        )
        group.add_argument("--mediacounts-job", action="store",
                           choices=self.jobs, default="archive",
                           dest="mediacountsjob", help="The job to execute.")
        group.add_argument("--mediacounts-date", action="store",
                           dest="mediacountsdate",
                           help="The date of the dump to work on.")
        group.add_argument("--mediacounts-path", action="store",
                           dest="mediacountspath",
                           help="The path to the dump directory.")

    def getItemMetadata(self, dumpdate):
        """
        This function is used for obtaining the metadata for the item on the
        Internet Archive.

        - dumpdate (string in %Y%m%d format): The date of the dump to work on.

        Returns: Dict with the necessary item metadata.
        """
        datename = self.conv.getDateFromWiki(dumpdate)
        arcdate = self.conv.getDateFromWiki(dumpdate, archivedate=True)
        try:
            datetime.datetime.strptime(dumpdate, '%Y%m%d')
        except ValueError:
            self.common.giveMessage('The date was given in the wrong format!')
            return False

        metadata = {
            'collection': self.config.get('collection'),
            'contributor': self.config.get('contributor'),
            'mediatype': self.config.get('mediatype'),
            'rights': self.config.get('rights'),
            'licenseurl': self.config.get('licenseurl'),
            'date': arcdate,
            'subject': self.config.get('subject'),
            'title': self.title % (datename),
            'description': self.desc % (datename)
        }
        return metadata

    def getFiles(self, dumpdate):
        """
        This function is for getting a list of dump files available to be
        archived for the given dump date.

        - dumpdate (string in %Y%m%d format): The date of the dump to work on.

        Returns list of all files.
        """
        arcdate = self.conv.getDateFromWiki(dumpdate, archivedate=True)
        return self.filelist % (arcdate, arcdate)

    def downloadFiles(self, dumpdate, filelist):
        """
        This function is used for downloading the relevant files for a
        particular dump.

        - dumpdate (string in %Y%m%d format): The date of the dump to work on.
        - filelist (list): The list of files to work on.

        Returns: True if successful, False if an error has occured.
        """
        fileopener = urllib.URLopener()
        os.chdir(self.tempdir)
        for dumpfile in filelist:
            if (os.path.isfile(dumpfile)):
                continue
            else:
                self.common.giveMessage("Downloading file: %s" % (dumpfile))
                d = datetime.datetime.strptime(dumpdate, '%Y%m%d')
                fileurl = "%s/%s/%s" % (self.config.get('baseurl'),
                                        d.strftime('%Y'), dumpfile)
                fileopener.retrieve(fileurl, dumpfile)

    def removeFiles(self, filelist):
        """
        This function is used for removing all the downloaded files for a
        particular dump.

        - filelist (list): The list of dump files.

        Returns: True if the operation is successful, False if an error has
        occured.
        """
        for dumpfile in filelist:
            filepath = "%s/%s" % (self.tempdir, dumpfile)
            try:
                os.remove(filepath)
            except:
                return False
        return True

    def checkDumpDir(self, path, filelist):
        """
        This function is used to check if the given dump directory is complete.

        - path (string): The path to the dump directory.
        - filelist (list): A list of files generated from self.getFiles().

        Returns: True if the dump directory is complete, False if otherwise.
        """
        if (os.path.exists(path)):
            files = os.listdir(path)
        else:
            # The dump directory does not exist, something wrong probably
            # happened along the way.
            self.common.giveDebugMessage("The dump file directory does not "
                                         "exist!")
            return False

        for dumpfile in filelist:
            if (dumpfile in files):
                continue
            else:
                # The dump files on the local directory is incomplete.
                # Exit the rest of the function and leave it to another day.
                self.common.giveDebugMessage("The dump files in the local "
                                             "directory is incomplete!")
                return False
        return True

    def getDumpDates(self, can_archive="all"):
        """
        This function is for getting all the date of dumps presently stored in
        the database.

        - can_archive (string): Dumps with this can_archive status will be
        returned, "all" for all can_archive statuses.

        Returns: List of dump dates.
        """
        dumps = []
        if (can_archive == "all"):
            conds = ''
        else:
            conds = 'can_archive="%s"' % (can_archive)

        options = 'ORDER BY dumpdate DESC LIMIT 30'
        results = self.sqldb.select(dbtable=self.dbtable,
                                    columns=['dumpdate'],
                                    conds=conds, options=options)
        if results is not None:
            for result in results:
                dumps.append(result[0].strftime("%Y%m%d"))
        return dumps

    def getItemsLeft(self, job=None):
        """
        This function is used for getting the number of items left to be done
        for a specific job.

        Note: The "update" job should not be using this!

        - job (string): The job to obtain the count for.

        Returns: Int with the number of items left to work on.
        """
        conds = {}
        if (job is None or job == "archive"):
            conds['is_archived'] = "0"
            conds['can_archive'] = "1"
            return self.getNumberOfItems(params=conds)
        elif (job == "check"):
            conds['is_archived'] = "1"
            conds['is_checked'] = "0"
            return self.getNumberOfItems(params=conds)
        else:
            return 0

    def getNumberOfItems(self, params={}):
        """
        This function is used to get the number of items left to work with.

        - params (dict): The conditions to put in the WHERE clause.

        Returns: Int with number of items left to work with.
        """
        conds = ['claimed_by IS NULL']
        for key, val in params.iteritems():
            conds.append('%s="%s"' % (key, val))
        return self.sqldb.count(dbtable=self.dbtable,
                                conds=' AND '.join(conds))

    def getRandomItem(self, job=None):
        """
        This function is used for getting a random item to work on for a
        specific job.

        Returns: String in %Y%m%d format for the dump date of the item to work
        on, None if otherwise.
        """
        if (job is None or job == "archive"):
            return self.getRandomItemSql(archived=False)
        elif (job == "check"):
            return self.getRandomItemSql(archived=True)
        else:
            return None

    def getRandomItemSql(self, archived=False):
        """
        This function is used to get a random item to work on.

        - archived (boolean): Whether or not to obtain a random item that is
        already archived.

        Returns: Dict with the parameters to the archiving scripts.
        """
        output = {}
        columns = ['dumpdate']
        options = 'ORDER BY RAND() LIMIT 1'
        conds = ['claimed_by IS NULL']

        if (archived):
            extra = [
                'is_archived="1"',
                'is_checked="0"'
            ]
        else:
            extra = [
                'is_archived="0"',
                'can_archive="1"'
            ]
        conds.extend(extra)

        results = self.sqldb.select(dbtable=self.dbtable, columns=columns,
                                    conds=' AND '.join(conds), options=options)
        if results is None:
            # This should not be triggered at all. Use self.getItemsLeft()
            # to verify first before running this function.
            output = None
        else:
            for result in results:
                output = result[0].strftime("%Y%m%d")

        return output

    def addNewItem(self, dumpdate):
        """
        This function is used for adding new dumps into the database.

        - dumpdate (string in %Y%m%d format): The date of the dump to add.

        Returns: True if the update is successful, False if an error occured.
        """
        try:
            arcdate = self.conv.getDateFromWiki(dumpdate, archivedate=True)
        except ValueError:
            return False

        values = {
            'dumpdate': '"%s"' % (arcdate),
            'claimed_by': 'NULL',
            'can_archive': '"0"',
            'is_archived': '"0"',
            'is_checked': '"0"',
            'comments': 'NULL'
        }
        return self.sqldb.insert(dbtable=self.dbtable, values=values)

    def updateCanArchive(self, dumpdate, can_archive):
        """
        This function is used to update the status of whether a dump can be
        archived.

        - dumpdate (string in %Y%m%d format): The date of the dump to work on.
        - can_archive (string): The can_archive status of the dump.

        Returns: True if update is successful, False if an error occurred.
        """
        vals = {
            'can_archive': '"%s"' % (can_archive)
        }
        arcdate = self.conv.getDateFromWiki(dumpdate, archivedate=True)
        return self.sqldb.update(dbtable=self.dbtable, values=vals,
                                 conds="dumpdate=%s" % (arcdate))

    def markArchived(self, dumpdate):
        """
        This function is used to mark an item as archived after doing so.

        - dumpdate (string in %Y%m%d format): The date of the dump to work on.

        Returns: True if update is successful, False if an error occurred.
        """
        vals = {
            'is_archived': '"1"',
            'claimed_by': 'NULL'
        }
        arcdate = self.conv.getDateFromWiki(dumpdate, archivedate=True)
        return self.sqldb.update(dbtable=self.dbtable, values=vals,
                                 conds="dumpdate=%s" % (arcdate))

    def markChecked(self, dumpdate):
        """
        This function is used to mark an item as checked after doing so.

        - dumpdate (string in %Y%m%d format): The date of the dump to work on.

        Returns: True if update is successful, False if an error occurred.
        """
        vals = {
            'is_checked': '"1"',
            'claimed_by': 'NULL'
        }
        arcdate = self.conv.getDateFromWiki(dumpdate, archivedate=True)
        return self.sqldb.update(dbtable=self.dbtable, values=vals,
                                 conds="dumpdate=%s" % (arcdate))

    def markFailedArchive(self, dumpdate):
        """
        This function is used to mark an item as failed when archiving it.

        - dumpdate (string in %Y%m%d format): The date of the dump to work on.

        Returns: True if update is successful, False if an error occurred.
        """
        vals = {
            'is_archived': '"2"',
            'claimed_by': 'NULL'
        }
        arcdate = self.conv.getDateFromWiki(dumpdate, archivedate=True)
        return self.sqldb.update(dbtable=self.dbtable, values=vals,
                                 conds="dumpdate=%s" % (arcdate))

    def markFailedCheck(self, dumpdate):
        """
        This function is used to mark an item as failed when checking it.

        - dumpdate (string in %Y%m%d format): The date of the dump to work on.

        Returns: True if update is successful, False if an error occurred.
        """
        vals = {
            'is_checked': '"2"',
            'claimed_by': 'NULL'
        }
        arcdate = self.conv.getDateFromWiki(dumpdate, archivedate=True)
        return self.sqldb.update(dbtable=self.dbtable, values=vals,
                                 conds="dumpdate=%s" % (arcdate))

    def claimItem(self, dumpdate):
        """
        This function is used to claim an item from the server.

        - dumpdate (string in %Y%m%d format): The date of the dump to work on.

        Returns: True if update is successful, False if an error occurred.
        """
        if (self.debug):
            return True
        else:
            vals = {
                'claimed_by': '"%s"' % (self.hostname)
            }
            arcdate = self.conv.getDateFromWiki(dumpdate, archivedate=True)
            return self.sqldb.update(dbtable=self.dbtable, values=vals,
                                     conds="dumpdate=%s" % (arcdate))

    def archive(self, dumpdate, path=None, verbose=False, debug=False):
        """
        This function is for doing the actual archiving process.

        - dumpdate (string): The dumpdate of the dump in %Y%m%d format.
        - path (string): The path to the dump directory.
        - verbose (boolean): Whether or not to increase verbosity.
        - debug (boolean): Whether or not to run in debug mode.

        Returns: True if process is successful, False if otherwise.
        """
        count = 0
        identifier = "mediacounts-%s" % (dumpdate)
        iaitem = balchivist.BALArchiver(identifier)
        allfiles = self.getFiles(dumpdate)

        if (path is None):
            dumps = self.tempdir
            self.downloadFiles(dumpdate=dumpdate, filelist=allfiles)
        else:
            dumps = path

        if (self.checkDumpDir(path=dumps, filelist=allfiles)):
            pass
        else:
            # The dump directory is not suitable to be used, exit the function
            return False

        os.chdir(dumps)
        for dumpfile in allfiles:
            self.common.giveMessage("Uploading file: %s" % (dumpfile))
            time.sleep(1)  # For Ctrl+C
            if count == 0:
                metadata = self.getItemMetadata(dumpdate)
                headers = {
                    'x-archive-size-hint': self.sizehint
                }
                upload = iaitem.uploadFile(dumpfile, metadata=metadata,
                                           headers=headers, debug=debug)
                # Allow the Internet Archive to process the item creation
                if debug:
                    pass
                else:
                    timenow = time.strftime("%Y-%m-%d %H:%M:%S",
                                            time.localtime())
                    self.common.giveMessage("Sleeping for 30 seconds, %s" %
                                            (timenow))
                    time.sleep(30)
            else:
                upload = iaitem.uploadFile(dumpfile, debug=debug)

            if upload:
                count += 1
            else:
                return False

        if (path is None):
            self.removeFiles(allfiles)
        else:
            pass

        return True

    def check(self, dumpdate):
        """
        This function checks if the uploaded dump is really complete.

        - dumpdate (string in %Y%m%d format): The date of the dump to check.

        Returns: True if complete, False if it isn't or errors have occurred.
        """
        complete = True
        allfiles = self.getFiles(dumpdate)
        identifier = "mediacounts-%s" % (dumpdate)
        iaitem = balchivist.BALArchiver(identifier)
        iafiles = iaitem.getFileList()
        self.common.giveMessage("Checking if all files are uploaded for the "
                                "%s dump" % (dumpdate))
        for dumpfile in allfiles:
            if (dumpfile in iafiles):
                continue
            else:
                # The Internet Archive have got incomplete items
                complete = False
        return complete

    def update(self):
        """
        This function checks for new dumps and adds new entries into the
        database.

        Returns: True if complete, raises an Exception if an error has
        occurred.
        """
        # Variables for getting latest 3 days for updating/checking
        today = datetime.datetime.now().strftime("%Y%m%d")
        oneday = datetime.datetime.now() - datetime.timedelta(days=1)
        twoday = datetime.datetime.now() - datetime.timedelta(days=2)
        yesterday = oneday.strftime("%Y%m%d")
        daybefore = twoday.strftime("%Y%m%d")

        alldumps = self.getDumpDates()

        # Add today's date into the database
        if (today in alldumps):
            self.common.giveMessage("Dump on %s already in the database, "
                                    "skipping" % (today))
        else:
            self.addNewItem(dumpdate=today)

        # Allow yesterday's dump to be archived
        if (yesterday in alldumps):
            self.common.giveMessage("Updating can_archive for dump on %s" %
                                    (yesterday))
            self.updateCanArchive(dumpdate=yesterday, can_archive=1)
        else:
            # Yesterday's date was not in the database, which should not happen
            self.common.giveMessage("Adding dump on %s and updating its "
                                    "can_archive status" % (yesterday))
            self.addNewItem(dumpdate=yesterday)
            self.updateCanArchive(dumpdate=yesterday, can_archive=1)

        # Double-check dump for the day before
        if (daybefore in alldumps):
            # Ensure that it really can be archived
            self.updateCanArchive(dumpdate=daybefore, can_archive=1)
        else:
            # The day before yesterday's date was not in the database, which
            # really should not happen
            self.common.giveMessage("Adding dump on %s and updating its "
                                    "can_archive status" % (daybefore))
            self.addNewItem(dumpdate=daybefore)
            self.updateCanArchive(dumpdate=daybefore, can_archive=1)

        return True

    def dispatch(self, job, date, path):
        """
        This function is for dispatching an item to the various functions.
        """
        self.claimItem(dumpdate=date)
        msg = "Running %s on the Wikimedia media files visit " % (job)
        msg += "statistics on %s" % (date)
        self.common.giveMessage(msg)
        if (job == "archive"):
            status = self.archive(dumpdate=date, path=path,
                                  verbose=self.verbose, debug=self.debug)
            if (self.debug):
                return status
            elif (self.debug is False and status):
                self.common.giveMessage("Marking %s as archived" % (date))
                self.markArchived(dumpdate=date)
            else:
                self.common.giveMessage("Marking %s as failed archive" %
                                        (date))
                self.markFailedArchive(dumpdate=date)
        elif (job == "check"):
            status = self.check(dumpdate=date)
            if (self.debug):
                return status
            elif (self.debug is False and status):
                self.common.giveMessage("Marking %s as checked" % (date))
                self.markChecked(dumpdate=date)
            else:
                self.common.giveMessage("Marking %s as failed check" % (date))
                self.markFailedCheck(dumpdate=date)

    def execute(self, args=None):
        """
        This function is for the main execution of the module.

        - args (namespace): A namespace for all arguments from argparse.

        Returns: True if all processing has completed successfully, False if an
        error has occurred.
        """
        continuous = False
        if (args is None):
            continuous = True
        elif (args.mediacountsjob == "update"):
            return self.update()
        elif (args.mediacountsdate is None):
            continuous = True
        else:
            pass

        if (continuous):
            if (args is None):
                # Default to performing the archive job
                mediacountsjob = "archive"
                mediacountspath = None
            else:
                mediacountsjob = args.mediacountsjob
                mediacountspath = args.mediacountspath

            while self.getItemsLeft(job=mediacountsjob) > 0:
                itemdetails = self.getRandomItem(job=mediacountsjob)
                date = itemdetails['date']
                self.dispatch(job=mediacountsjob, date=date,
                              path=mediacountspath)
        else:
            self.dispatch(job=args.mediacountsjob, date=args.mediacountsdate,
                          path=args.mediacountspath)

        return True


if __name__ == '__main__':
    BALMessage = balchivist.BALMessage()
    IncorrectUsage = balchivist.exception.IncorrectUsage
    raise IncorrectUsage(BALMessage.getMessage('exception-incorrectusage'))
