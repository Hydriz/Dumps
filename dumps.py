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

import datetime
import os
import re
import sys
import time
import urllib

import balchivist


class IncorrectUsage(Exception):
    pass


class BALDumps(object):
    def __init__(self, debug=False, verbose=False):
        """
        This script is for archiving the main Wikimedia database dumps.

        - debug (boolean): Whether or not to run in debug mode.
        - verbose (boolean): Whether or not to increase verbosity.
        """
        self.config = balchivist.BALConfig('dumps')
        self.debug = debug
        self.verbose = verbose
        self.title = "Wikimedia database dump of %s on %s"
        self.desc = "This is the full database dump of %s that is "
        self.desc += "generated by the Wikimedia Foundation on %s."
        self.subject = "wiki;dumps;data dumps;%s;%s;%s"
        # A size hint for the Internet Archive, currently set at 100GB
        self.sizehint = "107374182400"
        # Additional files in each dump
        self.additional = [
            'dumpruninfo.txt',
            'status.html'
        ]
        config = balchivist.BALConfig('main')
        self.sqldb = balchivist.BALSqlDb(database=config.get('database'),
                                         host=config.get('host'),
                                         default=config.get('defaults_file'))

    def printv(self, message):
        """
        This function is used to write output into stdout if verbose == True.

        - message (string): The message to output.
        """
        if self.verbose:
            sys.stdout.write("%s\n" % (message))
        else:
            pass

    def getDumpProgress(self, wikidb, dumpdate):
        """
        This function is used to get the progress of a dump.

        - wikidb (string): The wiki database to check.
        - dumpdate (string): The date of the dump in %Y%m%d format.

        Returns: String of either "progress", "done", "error" or "unknown"
        - "progress": The dump is still in progress. There are jobs that are
        still running or waiting to run.
        - "done": The dump is completed successfully.
        - "error": The dump has stopped and have errors in them.
        - "unknown": Unknown status. It is likely that such a dump does not
        exist.
        """
        output = "unknown"
        progress = 0
        done = 0
        statusurl = "%s/%s/%s/dumpruninfo.txt" % (self.config.get('dumps'),
                                                  wikidb, dumpdate)
        f = urllib.urlopen(statusurl)
        raw = f.read()
        f.close()

        regex = r'name:[^;]+; status:(?P<status>[^;]+); updated:'
        m = re.compile(regex).finditer(raw)
        for i in m:
            status = i.group('status')
            if status == "failed":
                output = "error"
                return output
            elif status == "in-progress" or status == "waiting":
                progress += 1
            elif status == "done":
                done += 1
        if progress > 0:
            output = "progress"
        elif progress == 0 and done > 0:
            output = "done"
        else:
            output = "unknown"
        return output

    def getDBList(self, dblist):
        """
        This function is used for getting an updated copy of the database
        list from the configuration files website.

        Returns: True if process is successful, False if otherwise.
        """
        dblisturl = self.config.get(dblist.replace(".", ""))
        try:
            urllib.urlretrieve(dblisturl, dblist)
            return True
        except:
            return False

    def getDatabases(self, dblist):
        """
        This function is used to get a list of databases from a dblist file.
        It will also check if the local cache of the dblist file is up-to-date.

        Returns: True if process is successful, False if otherwise.
        """
        if not os.path.exists(dblist):
            self.getDBList(dblist)
        else:
            lastchange = os.path.getctime(dblist)
            now = time.time()
            dayago = now - 60*60*24*1
            if (lastchange < dayago):
                # The dblist cache is more than a day old, update it
                self.getDBList(dblist)
            else:
                pass
        databases = open(dblist, 'r').read().splitlines()
        return sorted(databases)

    def getDumpFiles(self, wikidb, dumpdate):
        """
        This function is used to get a list of dump files from the dumps server
        by using regular expressions.

        Returns: List of files.
        """
        dumpfiles = []
        url = "%s/%s/%s/index.html" % (self.config.get('dumps'), wikidb,
                                       dumpdate)
        f = urllib.urlopen(url)
        raw = f.read()
        f.close()

        regex = r'<li class=\'file\'><a href="/%s/%s/(?P<dumpfile>[^>]+)">' % (
                wikidb, dumpdate)
        m = re.compile(regex).finditer(raw)
        for i in m:
            dumpfiles.append(i.group('dumpfile'))
        return sorted(dumpfiles + self.additional)

    def getAllDumps(self, wikidb):
        """
        This function is used to get all dumps in a directory from the dumps
        server by using regular expressions.

        Returns: List of all dumps.
        """
        dumps = []
        url = "%s/%s" % (self.config.get('dumps'), wikidb)
        f = urllib.urlopen(url)
        raw = f.read()
        f.close()

        regex = r'<a href="(?P<dump>[^>]+)/">'
        m = re.compile(regex).finditer(raw)
        for i in m:
            try:
                datetime.datetime.strptime(i.group('dump'), '%Y%m%d')
            except ValueError:
                continue
            dumps.append(i.group('dump'))
        return sorted(dumps)

    def checkDumpDir(self, dumpdir, wikidb, dumpdate):
        """
        This function is used to check if the given dump directory is complete.

        - dumpdir (string): The path to the dump directory.
        - wikidb (string): The wiki database to check.
        - dumpdate (string): The date of the dump in %Y%m%d format.

        Returns: True if dump directory is complete, False if otherwise.
        """
        if os.path.exists(dumpdir):
            files = os.listdir(dumpdir)
        else:
            # The dump directory does not exist.
            # Exit the rest of the function and leave it to another day.
            if self.debug:
                sys.stderr.write("The dump file directory does not exist!")
            else:
                pass
            return False
        allfiles = self.getDumpFiles(wikidb, dumpdate)
        for dumpfile in allfiles:
            if dumpfile in files:
                continue
            else:
                # The dump files on the local directory is incomplete.
                # Exit the rest of the function and leave it to another day.
                if self.debug:
                    sys.stderr.write("The dump files in the local directory "
                                     "is incomplete")
                else:
                    pass
                return False
        return True

    def archive(self, wikidb, dumpdate, dumpdir=None, resume=False):
        """
        This function is for doing the actual archiving process.

        - wikidb (string): The wiki database to check.
        - dumpdate (string): The date of the dump in %Y%m%d format.
        - dumpdir (string): The path to the dump directory.
        - resume (boolean): Whether or not to resume archiving an item.

        Returns: True if process is successful, False if otherwise.
        """
        converter = balchivist.BALConverter()
        wikiname = converter.getNameFromDB(wikidb)
        sitename = converter.getNameFromDB(wikidb, pretext=True)
        langname = converter.getNameFromDB(wikidb, format='language')
        project = converter.getNameFromDB(wikidb, format='project')
        datename = converter.getDateFromWiki(dumpdate)
        arcdate = converter.getDateFromWiki(dumpdate, archivedate=True)

        if dumpdir is None:
            dumps = "%s/%s/%s" % (self.config.get('dumpdir'), wikidb, dumpdate)
        else:
            dumps = dumpdir
        if self.checkDumpDir(dumps, wikidb, dumpdate):
            pass
        else:
            # The dump directory is not suitable to be used, exit the function
            return False
        count = 0
        iaitem = balchivist.BALArchiver('%s-%s' % (wikidb, dumpdate))
        if resume:
            items = []
            iafiles = iaitem.getFileList()
            for dumpfile in allfiles:
                if dumpfile in iafiles:
                    continue
                else:
                    # The file does not exist in the Internet Archive item
                    items.append(dumpfile)
            if items == []:
                self.printv("All files have already been uploaded")
                return True
        else:
            items = allfiles
        os.chdir(dumps)
        for dumpfile in items:
            self.printv("Uploading file: %s" % (dumpfile))
            time.sleep(1)  # For Ctrl+C
            if count == 0:
                metadata = {
                    "collection": self.config.get('collection'),
                    "creator": self.config.get('creator'),
                    "contributor": self.config.get('contributor'),
                    "mediatype": self.config.get('mediatype'),
                    "rights": self.config.get('rights'),
                    "subject": self.subject % (wikidb, langname, project),
                    "date": arcdate,
                    "licenseurl": self.config.get('licenseurl'),
                    "title": self.title % (sitename, datename),
                    "description": self.desc % (sitename, datename)
                }
                headers = {
                    'x-archive-size-hint': self.sizehint
                }
                upload = iaitem.uploadFile(dumpfile, metadata=metadata,
                                           headers=headers, debug=self.debug)
                # Allow the Internet Archive to process the item creation
                if self.debug:
                    pass
                else:
                    self.printv("Sleeping for 30 seconds, %s" %
                                (time.strftime("%Y-%m-%d %H:%M:%S",
                                               time.localtime())))
                    time.sleep(30)
            else:
                upload = iaitem.uploadFile(dumpfile, debug=self.debug)
            if upload:
                if self.debug:
                    sys.stderr.write(upload)
                else:
                    pass
                count += 1
            else:
                return False
        return True

    def check(self, wikidb, dumpdate):
        """
        This function checks if the uploaded dump is really complete.

        - wikidb (string): The wiki database to check.
        - dumpdate (string): The date of the dump in %Y%m%d format.

        Returns: True if complete, False if errors have occurred.
        """
        complete = True
        allfiles = self.getDumpFiles(wikidb, dumpdate)
        iaitem = balchivist.BALArchiver('%s-%s' % (wikidb, dumpdate))
        iafiles = iaitem.getFileList()
        self.printv("Checking if all files are uploaded for %s on %s" %
                    (wikidb, dumpdate))
        params = {
            'type': 'main',
            'subject': wikidb,
            'dumpdate': dumpdate
        }
        for dumpfile in allfiles:
            if dumpfile in iafiles:
                continue
            else:
                # The Internet Archive have got incomplete items
                complete = False
        if complete:
            self.printv("All the files have been uploaded, marking as "
                        "checked")
            self.sqldb.markChecked(params=params)
            return True
        else:
            self.printv("Internet Archive item is incomplete!")
            self.sqldb.markFailedCheck(params=params)
            return False

    def update(self):
        """
        This function checks for new dumps and add new entries into the
        database.

        Returns: True if complete, Exception if an error occurred.
        """
        alldb = self.getDatabases('all.dblist')
        privatedb = self.getDatabases('private.dblist')
        converter = balchivist.BALConverter()
        # Remove all instances of private wikis
        for private in privatedb:
            alldb.remove(private)
        for db in alldb:
            dumps = self.getAllDumps(db)
            stored = self.sqldb.getAllDumps(db)
            inprogress = self.sqldb.getAllDumps(db, progress="progress")
            cannotarc = self.sqldb.getAllDumps(db, can_archive=0)
            # Step 1: Check if all new dumps are registered
            for dump in dumps:
                if dump in stored:
                    self.printv("Dump of %s on %s already in the database, "
                                "skipping" % (db, dump))
                    continue
                else:
                    self.printv("Adding new item %s on %s" % (db, dump))
                    arcdate = converter.getDateFromWiki(dump, archivedate=True)
                    progress = self.getDumpProgress(db, dump)
                    params = {
                        'type': 'main',
                        'subject': db,
                        'dumpdate': arcdate,
                        'progress': progress
                    }
                    self.sqldb.addNewItem(params=params)
            # Step 2: Check if the status of dumps in progress have changed
            for dump in inprogress:
                progress = self.getDumpProgress(db, dump)
                if progress != 'progress':
                    self.printv("Updating dump progress for %s on %s" % (db,
                                                                         dump))
                    arcdate = converter.getDateFromWiki(dump, archivedate=True)
                    params = {
                        'type': 'main',
                        'subject': db,
                        'dumpdate': arcdate,
                        'progress': progress
                    }
                    self.sqldb.updateProgress(params=params)
                else:
                    continue
            # Step 3: Check if the dump is available for archiving
            for dump in cannotarc:
                dumpdir = "%s/%s/%s" % (self.config.get('dumpdir'), db, dump)
                if self.checkDumpDir(dumpdir, db, dump):
                    # The dump is now suitable to be archived
                    self.printv("Updating can_archive for %s on %s" % (db,
                                                                       dump))
                    arcdate = converter.getDateFromWiki(dump, archivedate=True)
                    params = {
                        'type': 'main',
                        'subject': db,
                        'dumpdate': arcdate
                    }
                    self.sqldb.markCanArchive(params=params)
                else:
                    continue
        return True

if __name__ == '__main__':
    raise IncorrectUsage("Script cannot be used directly, use runner.py "
                         "instead")
