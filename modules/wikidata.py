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
import re
import shutil
import socket
import urllib

import balchivist


class BALMWikidata(object):
    """
    This module is for archiving the Wikidata JSON dumps provided by the
    Wikimedia Foundation (available at
    <https://dumps.wikimedia.org/other/wikibase/>) to the Internet Archive.
    """
    title = "JSON dump of all Wikibase entries for %s generated on %s"
    desc = "This is a JSON dump of all Wikibase entries for %s that "
    desc += "was generated by the Wikimedia Foundation on %s."
    subject = "%s;wikibase;wikidata;json;dumps;data dumps"

    config = balchivist.BALConfig('wikidata')
    dbtable = "wikidata"
    conv = balchivist.BALConverter()
    resume = False
    hostname = socket.gethostname()

    jobs = [
        "archive",
        "check",
        "update"
    ]
    # A size hint for the Internet Archive, currently set at 100GB
    sizehint = "107374182400"

    def __init__(self, params={}, sqldb=None):
        """
        This module is for archiving the Wikidata JSON dumps provided by the
        Wikimedia Foundation (available at
        <https://dumps.wikimedia.org/other/wikibase/>) to the Internet Archive.

        - params (dict): Information about what is to be done about a given
        item. The "verbose" and "debug" parameters are necessary.
        - sqldb (object): A call to the BALSqlDb class with the required
        parameters.
        """
        self.sqldb = sqldb
        self.verbose = params['verbose']
        self.debug = params['debug']
        self.common = balchivist.BALCommon(verbose=self.verbose,
                                           debug=self.debug)

    @classmethod
    def argparse(cls, parser=None):
        """
        This function is used for declaring the valid arguments specific to
        this module and should only be used during the argparse stage.

        - parser (object): The parser object.
        """
        group = parser.add_argument_group(
            title="Wikidata JSON dumps",
            description="The JSON dumps of all Wikibase entries on Wikidata."
        )
        group.add_argument("--wikidata-job", action="store", choices=cls.jobs,
                           default="archive", dest="wikidatajob",
                           help="The job to execute.")
        group.add_argument("--wikidata-wiki", action="store",
                           dest="wikidatawiki", help="The wiki to work on.")
        group.add_argument("--wikidata-date", action="store",
                           dest="wikidatadate",
                           help="The date of the wiki dump to work on.")
        group.add_argument("--wikidata-path", action="store",
                           dest="wikidatapath",
                           help="The path to the wiki dump directory.")
        group.add_argument("--wikidata-resume", action="store_true",
                           default=False, dest="wikidataresume",
                           help="Resume uploading a wiki dump instead of "
                           "restarting all over.")

    def extractLinks(self, url):
        """
        This function is for getting a list of links for the given URL.

        - url (string): The URL to work on.

        Returns list of links without the trailing slash and the parent
        directory.
        """
        links = []
        page = urllib.urlopen(url)
        raw = page.read()
        page.close()

        regex = r'<a href="(?P<link>[^>]+)">'
        m = re.compile(regex).finditer(raw)
        for i in m:
            database = i.group('link')
            if (database == "../"):
                # Skip the parent directory
                continue
            elif (database.endswith('/')):
                links.append(database[:-1])
            else:
                links.append(database)
        return sorted(links)

    def getDatabases(self):
        """
        This function is for getting a list of all wiki databases with dumps.

        Returns list of all databases.
        """
        return self.extractLinks(self.config.get('baseurl'))

    def getDumpDates(self, database):
        """
        This function is for getting a list of all available dump dates for a
        given database.

        - database (string): The database to get all dump dates for.

        Returns list of all dump dates (in %Y%m%d format).
        """
        url = "%s/%s/" % (self.config.get('baseurl'), database)
        return self.extractLinks(url)

    def getFiles(self, database, dumpdate):
        """
        This function is for getting a list of dump files available to be
        archived for the given wiki.

        - database (string): The database to get the dump files for.

        Returns list of all files.
        """
        url = "%s/%s/%s/" % (self.config.get('baseurl'), database, dumpdate)
        return self.extractLinks(url)

    def getStoredDumps(self, database, can_archive="all"):
        """
        This function is for getting a list of dumps that are currently stored
        in the database.

        - database (string): The database to get a list of dumps for.
        - can_archive (string): Dumps with this can_archive status will be
        returned, "all" for all can_archive statuses.

        Returns list of all dump dates.
        """
        dumps = []
        conds = ['wiki="%s"' % database]
        if (can_archive == "all"):
            pass
        else:
            conds.append('can_archive="%s"' % (can_archive))

        options = 'ORDER BY dumpdate DESC LIMIT 30'
        results = self.sqldb.select(dbtable=self.dbtable,
                                    columns=['dumpdate'],
                                    conds=' AND '.join(conds), options=options)
        if results is not None:
            for result in results:
                dumps.append(result[0].strftime("%Y%m%d"))
        return dumps

    def getItemMetadata(self, database, dumpdate):
        """
        This function is for obtaining the metadata for the item on the
        Internet Archive.

        - database (string): The database name of the wiki.
        - dumpdate (string in %Y%m%d format): The date of the current dump.

        Returns dict with all the item metadata or False if an error occurred.
        """
        try:
            datetime.datetime.strptime(dumpdate, '%Y%m%d')
        except ValueError:
            self.common.giveMessage('The date was given in the wrong format!')
            return False
        sitename = self.conv.getNameFromDB(database, pretext=True)
        datename = self.conv.getDateFromWiki(dumpdate)
        arcdate = self.conv.getDateFromWiki(dumpdate, archivedate=True)

        metadata = {
            'collection': self.config.get('collection'),
            'creator': self.config.get('creator'),
            'contributor': self.config.get('contributor'),
            'mediatype': self.config.get('mediatype'),
            'rights': self.config.get('rights'),
            'licenseurl': self.config.get('licenseurl'),
            'date': arcdate,
            'subject': self.subject % (database),
            'title': self.title % (sitename, datename),
            'description': self.desc % (sitename, datename)
        }
        return metadata

    def downloadFiles(self, database, dumpdate, filelist):
        """
        This function is used for downloading all the available dump files for
        a given wiki and saves them into the temporary directory.

        - database (string): The database for the dump files.
        - dumpdate (string in %Y%m%d format): The date of the current dump.
        - filelist (list): A list of files generated from self.getFiles().
        """
        fileopener = urllib.URLopener()
        dumps = "%s/%s/%s" % (self.config.get('dumpdir'), database, dumpdate)

        if (os.path.exists(dumps)):
            pass
        else:
            os.makedirs(dumps)

        os.chdir(dumps)
        for dumpfile in filelist:
            if (os.path.isfile(dumpfile)):
                continue
            else:
                self.common.giveMessage("Downloading file: %s" % (dumpfile))
                fileurl = "%s/%s/%s/%s" % (self.config.get('baseurl'),
                                           database, dumpdate, dumpfile)
                fileopener.retrieve(fileurl, dumpfile)

    def checkDumpDir(self, path, filelist):
        """
        This function is used to check if the given dump directory is complete.

        - path (string): The path to the dump directory.
        - filelist (list): A list of files generated from self.getFiles().
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

    def getSqlConds(self, params):
        """
        This function is used for getting the conditions necessary for the
        SQL query to work.

        - params (dict): Information about the item with the keys "wiki"
        and "dumpdate".

        Returns: String with the SQL-like conditions.
        """
        arcdate = self.conv.getDateFromWiki(params['dumpdate'],
                                            archivedate=True)
        conds = [
            'wiki="%s"' % (params['wiki']),
            'dumpdate="%s"' % (arcdate)
        ]
        return ' AND '.join(conds)

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

        Returns: Dict with the information about the item to work on.
        """
        if (job is None or job == "archive"):
            itemdetails = self.getRandomItemSql(archived=False)
            output = {
                'wiki': itemdetails['wiki'],
                'date': itemdetails['date']
            }
        elif (job == "check"):
            itemdetails = self.getRandomItemSql(archived=True)
            output = {
                'wiki': itemdetails['wiki'],
                'date': itemdetails['date']
            }
        elif (job == "update"):
            output = {
                'wiki': None,
                'date': None
            }
        else:
            output = {}
        return output

    def getRandomItemSql(self, archived=False):
        """
        This function is used to get a random item to work on.

        - archived (boolean): Whether or not to obtain a random item that is
        already archived.

        Returns: Dict with the parameters to the archiving scripts.
        """
        output = {}
        columns = ['wiki', 'dumpdate']
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
            output = {
                'wiki': None,
                'date': None
            }
        else:
            for result in results:
                output = {
                    'wiki': result[0],
                    'date': result[1].strftime("%Y%m%d")
                }

        return output

    def updateCanArchive(self, params):
        """
        This function is used to update the status of whether a dump can be
        archived.

        - params (dict): Information about the item with the keys "wiki",
        "dumpdate" and "can_archive".

        Returns: True if update is successful, False if an error occurred.
        """
        vals = {
            'can_archive': '"%s"' % (params['can_archive'])
        }
        return self.sqldb.update(dbtable=self.dbtable, values=vals,
                                 conds=self.getSqlConds(params=params))

    def markArchived(self, params):
        """
        This function is used to mark an item as archived after doing so.

        - params (dict): Information about the item with the keys "wiki"
        and "dumpdate".

        Returns: True if update is successful, False if an error occurred.
        """
        vals = {
            'is_archived': '"1"',
            'claimed_by': 'NULL'
        }
        return self.sqldb.update(dbtable=self.dbtable, values=vals,
                                 conds=self.getSqlConds(params=params))

    def markChecked(self, params):
        """
        This function is used to mark an item as checked after doing so.

        - params (dict): Information about the item with the keys "wiki"
        and "dumpdate".

        Returns: True if update is successful, False if an error occurred.
        """
        vals = {
            'is_checked': '"1"',
            'claimed_by': 'NULL'
        }
        return self.sqldb.update(dbtable=self.dbtable, values=vals,
                                 conds=self.getSqlConds(params=params))

    def markFailedArchive(self, params):
        """
        This function is used to mark an item as failed when archiving it.

        - params (dict): Information about the item with the keys "wiki"
        and "dumpdate".

        Returns: True if update is successful, False if an error occurred.
        """
        vals = {
            'is_archived': '"2"',
            'claimed_by': 'NULL'
        }
        return self.sqldb.update(dbtable=self.dbtable, values=vals,
                                 conds=self.getSqlConds(params=params))

    def markFailedCheck(self, params):
        """
        This function is used to mark an item as failed when checking it.

        - params (dict): Information about the item with the keys "wiki"
        and "dumpdate".

        Returns: True if update is successful, False if an error occurred.
        """
        vals = {
            'is_checked': '"2"',
            'claimed_by': 'NULL'
        }
        return self.sqldb.update(dbtable=self.dbtable, values=vals,
                                 conds=self.getSqlConds(params=params))

    def claimItem(self, params):
        """
        This function is used to claim an item from the server.

        - params (dict): Information about the item with the keys "wiki"
        and "dumpdate".

        Returns: True if update is successful, False if an error occurred.
        """
        vals = {
            'claimed_by': '"%s"' % (self.hostname)
        }
        return self.sqldb.update(dbtable=self.dbtable, values=vals,
                                 conds=self.getSqlConds(params=params))

    def addNewItem(self, params):
        """
        This function is used to insert a new item into the database.

        - params (dict): Information about the item with the keys "wiki" and
        "dumpdate".

        Returns: True if update is successful, False if an error occurred.
        """
        try:
            arcdate = self.conv.getDateFromWiki(params['dumpdate'],
                                                archivedate=True)
        except ValueError:
            # This case occurs when the "dumpdate" parameter is not in the
            # %Y%m%d format (usually for files like "dcatap.rdf")
            return False

        values = {
            'wiki': '"%s"' % (params['wiki']),
            'dumpdate': '"%s"' % (arcdate),
            'claimed_by': 'NULL',
            'can_archive': '"0"',
            'is_archived': '"0"',
            'is_checked': '"0"',
            'comments': 'NULL'
        }
        return self.sqldb.insert(dbtable=self.dbtable, values=values)

    def updateNewDumps(self, db, alldumps):
        """
        This function is used to check if all new dumps have been registered
        and update the database accordingly for new dumps. This function is
        called during the "update" job.

        - db (string): The database to work on.
        - alldumps (list): A list of all dumps.
        """
        storeddumps = self.getStoredDumps(database=db)
        for dump in alldumps:
            if (dump in storeddumps):
                self.common.giveMessage("Dump of %s on %s already in the "
                                        "database, skipping" % (db, dump))
                continue
            else:
                self.common.giveMessage("Adding new item %s on "
                                        "%s" % (db, dump))
                params = {
                    'wiki': db,
                    'dumpdate': dump
                }
                self.addNewItem(params=params)

    def updateCanArchiveStatus(self, db, alldumps):
        """
        This function is used for checking existing dumps that have been
        completed and updates the database if these dumps are ready to be
        archived. This function is called during the "update" job.

        - db (string): The database to work on.
        - alldumps (list): A list of all dumps.
        """
        cannotarc = self.getStoredDumps(database=db, can_archive=0)
        lastweek = datetime.datetime.now()
        lastweek -= datetime.timedelta(days=7)
        for dump in cannotarc:
            if (dump <= lastweek.strftime("%Y%m%d") and dump in alldumps):
                # The dump is now suitable to be archived
                self.common.giveMessage("Updating can_archive for %s "
                                        "on %s" % (db, dump))
                params = {
                    'wiki': db,
                    'dumpdate': dump,
                    'can_archive': 1
                }
                self.updateCanArchive(params=params)
            else:
                continue

    def updateOldCanArchiveStatus(self, db, alldumps):
        """
        This function is used for checking whether the dumps marked as "can
        archive" is really able to be archived or has been deleted. This
        function is called during the "update" job.

        - db (string): The database to work on.
        """
        canarc = self.getStoredDumps(database=db, can_archive=1)
        for dump in canarc:
            if (dump in alldumps):
                continue
            else:
                # The dump is now unable to be archived
                self.common.giveMessage("Updating can_archive for %s on "
                                        "%s" % (db, dump))
                params = {
                    'wiki': db,
                    'dumpdate': dump,
                    'can_archive': 0
                }
                self.updateCanArchive(params=params)

    def getFilesToUpload(self, database, dumpdate):
        """
        This function is used to generate the list of files to upload given
        the circumstances.

        - database (string): The wiki database to work on.
        - dumpdate (string): The date of the dump in %Y%m%d format.

        Returns: List of files to upload.
        """
        identifier = "wikibase-%s-%s" % (database, dumpdate)
        iaitem = balchivist.BALArchiver(identifier)
        allfiles = self.getFiles(database, dumpdate)
        if self.resume:
            items = []
            iafiles = iaitem.getFileList()
            for dumpfile in allfiles:
                if dumpfile in iafiles:
                    continue
                else:
                    # The file does not exist in the Internet Archive item
                    items.append(dumpfile)
            if items == []:
                self.common.giveMessage("All files have already been uploaded")
                return items
        else:
            items = allfiles

    def archive(self, database, dumpdate, path=None):
        """
        This function is for doing the actual archiving process.

        - database (string): The wiki database to archive.
        - dumpdate (string): The dumpdate of the dump in %Y%m%d format.
        - path (string): The path to the dump directory.

        Returns: True if process is successful, False if otherwise.
        """
        identifier = "wikibase-%s-%s" % (database, dumpdate)
        iaitem = balchivist.BALArchiver(identifier)
        md = self.getItemMetadata(database, dumpdate)
        items = self.getFilesToUpload(database, dumpdate)
        headers = {
            'x-archive-size-hint': self.sizehint
        }

        if (path is None):
            dumps = "%s/%s/%s" % (self.config.get('dumpdir'), database,
                                  dumpdate)
            self.downloadFiles(database=database, dumpdate=dumpdate,
                               filelist=items)
        else:
            dumps = path

        if (self.checkDumpDir(path=dumps, filelist=items)):
            pass
        else:
            # The dump directory is not suitable to be used, exit the function
            return False

        if (items == []):
            return True
        else:
            os.chdir(dumps)
            upload = iaitem.upload(body=items, metadata=md, headers=headers)

        if (upload and path is None):
            shutil.rmtree(dumps)
        else:
            return upload

    def check(self, database, dumpdate):
        """
        This function checks if the uploaded dump is really complete.

        - database (string): The wiki database to check.
        - dumpdate (string in %Y%m%d format): The date of the dump to check.

        Returns: True if complete, False if it isn't or errors have occurred.
        """
        complete = True
        allfiles = self.getFiles(database, dumpdate)
        identifier = "wikibase-%s-%s" % (database, dumpdate)
        iaitem = balchivist.BALArchiver(identifier)
        iafiles = iaitem.getFileList()
        self.common.giveMessage("Checking if all files are uploaded for %s "
                                "on %s" % (database, dumpdate))
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
        databases = self.getDatabases()
        for db in databases:
            alldumps = self.getDumpDates(database=db)
            # Step 1: Ensure that all new dumps are registered
            self.updateNewDumps(db, alldumps=alldumps)
            # Step 2: Check if the dump is suitable for archiving
            self.updateCanArchiveStatus(db, alldumps=alldumps)
            # Step 3: Reset the can_archive statuses of old dumps
            self.updateOldCanArchiveStatus(db, alldumps=alldumps)

        return True

    def dispatch(self, job, wiki, date, path):
        """
        This function is for dispatching an item to the various functions.
        """
        updatedetails = {
            'wiki': wiki,
            'dumpdate': date
        }

        # Claim the item from the database server if not in debug mode
        if self.debug:
            pass
        else:
            self.claimItem(params=updatedetails)

        msg = "Running %s on the JSON dumps of all Wikibase entries " % (job)
        msg += "for %s on %s" % (wiki, date)
        self.common.giveMessage(msg)
        if (job == "archive"):
            status = self.archive(database=wiki, dumpdate=date, path=path)
            if (self.debug):
                return status
            elif (self.debug is False and status):
                self.common.giveMessage("Marking %s on %s as archived" %
                                        (wiki, date))
                self.markArchived(updatedetails)
            else:
                self.common.giveMessage("Marking %s on %s as failed"
                                        " archive" % (wiki, date))
                self.markFailedArchive(updatedetails)
        elif (job == "check"):
            status = self.check(database=wiki, dumpdate=date)
            if (self.debug):
                return status
            elif (self.debug is False and status):
                self.common.giveMessage("Marking %s on %s as checked" %
                                        (wiki, date))
                self.markChecked(updatedetails)
            else:
                self.common.giveMessage("Marking %s on %s as failed"
                                        " check" % (wiki, date))
                self.markFailedCheck(updatedetails)

    def execute(self, args=None):
        """
        This function is for the main execution of the module and is directly
        called by runner.py.

        - args (namespace): A namespace of all the arguments from argparse.

        Returns True if all required processing is successful, False if an
        error has occurred.
        """
        continuous = False
        if (args is None):
            continuous = True
        elif (args.wikidatajob == "update"):
            return self.update()
        elif (args.wikidatawiki is None and args.wikidatadate is None):
            continuous = True
        elif (args.wikidatawiki is None and args.wikidatadate is not None):
            self.common.giveError("Error: Date was given but not the wiki!")
            return False
        elif (args.wikidatawiki is not None and args.wikidatadate is None):
            self.common.giveError("Error: Wiki was given but not the date!")
            return False
        else:
            pass

        if (continuous):
            if (args is None):
                # Default to performing the archive job
                wikidatajob = "archive"
                wikidatapath = None
            else:
                wikidatajob = args.wikidatajob
                wikidatapath = args.wikidatapath

            while self.getItemsLeft(job=wikidatajob) > 0:
                itemdetails = self.getRandomItem(job=wikidatajob)
                wiki = itemdetails['wiki']
                date = itemdetails['date']
                self.dispatch(job=wikidatajob, wiki=wiki, date=date,
                              path=wikidatapath)
        else:
            self.resume = args.wikidataresume
            self.dispatch(job=args.wikidatajob, wiki=args.wikidatawiki,
                          date=args.wikidatadate, path=args.wikidatapath)

        return True


if __name__ == '__main__':
    BALMessage = balchivist.BALMessage()
    IncorrectUsage = balchivist.exception.IncorrectUsage
    raise IncorrectUsage(BALMessage.getMessage('exception-incorrectusage'))
