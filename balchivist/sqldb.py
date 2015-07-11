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
#
# TODO:
# - is_claimed: Column to indicate whether the item has been claimed for
# archiving or checking

import socket
import time

import MySQLdb

from converter import BALConverter


class IncorrectUsage(Exception):
    pass


class BALSqlDb(object):
    def __init__(self, database='balchivist', host='localhost',
                 default='~/.my.cnf'):
        """
        This module is used to provide an interface for interacting with an
        SQL database and the regular functions specific to Balchivist.

        - database (string): The database name to work with.
        - host (string): The MySQL server hosting the database
        - default (string): The path to the file with the MySQL credentials.
        """
        self.default = default
        self.host = host
        self.database = database
        self.dbtable = 'archive'
        self.hostname = socket.gethostname()

    def execute(self, query, params=()):
        """
        This function is used to execute a query on the database given when
        initializing the module.

        - query (string): The query to execute on the database.
        - params (tuple): Parameters to substitute in query to prevent SQL
        injection attacks.

        Returns: Tuple with the MySQL query results, else None if empty set.
        """
        result = ()
        conn = MySQLdb.connect(host=self.host, db=self.database,
                               read_default_file=self.default)
        cursor = conn.cursor()
        cursor.execute(query, params)
        result = cursor.fetchall()
        conn.commit()
        cursor.close()
        conn.close()
        if result is None or result is ():
            return None
        else:
            return result

    def getAllDumps(self, wikidb, progress="all", can_archive="all"):
        """
        This function is used to get all dumps of a specific wiki.

        - progress (string): Dumps with this progress will be returned, "all"
        for all progress statuses.
        - can_archive (string): Dumps with this can_archive status will be
        returned, "all" for all can_archive statuses.

        Returns: Dict with all dumps of a wiki.
        """
        dumps = []
        conds = ""
        if progress == "all":
            pass
        else:
            conds += ' AND progress="%s"' % (progress)
        if can_archive == "all":
            pass
        else:
            conds += ' AND can_archive="%s"' % (can_archive)
        query = [
            'SELECT', 'dumpdate',
            'FROM', self.dbtable,
            'WHERE', 'subject="%s"%s' % (wikidb, conds),
            'ORDER BY', 'dumpdate DESC',
            'LIMIT', '20;'
        ]
        results = self.execute(' '.join(query))
        if results is not None:
            for result in results:
                dumps.append(result[0].strftime("%Y%m%d"))
        return dumps

    def claimItem(self, params):
        """
        This function is used to claim an item from the server.

        - params (dict): Information about the item with the keys "type",
        "subject" and "dumpdate".

        Returns: True if update is successful, False if an error occurred.
        """
        conv = BALConverter()
        arcdate = conv.getDateFromWiki(params['dumpdate'], archivedate=True)
        conds = (self.hostname, params['type'], params['subject'], arcdate)
        query = [
            'UPDATE', self.dbtable,
            'SET', 'claimed_by=%s',
            'WHERE', 'type=%s AND subject=%s AND dumpdate=%s;'
        ]
        try:
            self.execute(' '.join(query), conds)
            return True
        except:
            return False

    def markCanArchive(self, params):
        """
        This function is used to update the status of whether a dump can be
        archived.

        - params (dict): Information about the item with the keys "type",
        "subject" and "dumpdate".

        Returns: True if update is successful, False if an error occurred.
        """
        conv = BALConverter()
        arcdate = conv.getDateFromWiki(params['dumpdate'], archivedate=True)
        conds = (params['type'], params['subject'], arcdate)
        query = [
            'UPDATE', self.dbtable,
            'SET', 'can_archive=1',
            'WHERE', 'type=%s AND subject=%s AND dumpdate=%s;'
        ]
        try:
            self.execute(' '.join(query), conds)
            return True
        except:
            return False

    def markArchived(self, params):
        """
        This function is used to mark an item as archived after doing so.

        - params (dict): Information about the item with the keys "type",
        "subject" and "dumpdate".

        Returns: True if update is successful, False if an error occurred.
        """
        conv = BALConverter()
        arcdate = conv.getDateFromWiki(params['dumpdate'], archivedate=True)
        conds = (params['type'], params['subject'], arcdate)
        query = [
            'UPDATE', self.dbtable,
            'SET', 'is_archived=1, claimed_by=NULL',
            'WHERE', 'type=%s AND subject=%s AND dumpdate=%s;'
        ]
        try:
            self.execute(' '.join(query), conds)
            return True
        except:
            return False

    def markChecked(self, params):
        """
        This function is used to mark an item as checked after doing so.

        - params (dict): Information about the item with the keys "type",
        "subject" and "dumpdate".

        Returns: True if update is successful, False if an error occurred.
        """
        conv = BALConverter()
        arcdate = conv.getDateFromWiki(params['dumpdate'], archivedate=True)
        conds = (params['type'], params['subject'], arcdate)
        query = [
            'UPDATE', self.dbtable,
            'SET', 'is_checked=1, claimed_by=NULL',
            'WHERE', 'type=%s AND subject=%s AND dumpdate=%s;'
        ]
        try:
            self.execute(' '.join(query), conds)
            return True
        except:
            return False

    def markFailedArchive(self, params):
        """
        This function is used to mark an item as failed when archiving it.

        - params (dict): Information about the item with the keys "type",
        "subject" and "dumpdate".

        Returns: True if update is successful, False if an error occurred.
        """
        conv = BALConverter()
        arcdate = conv.getDateFromWiki(params['dumpdate'], archivedate=True)
        conds = (params['type'], params['subject'], arcdate)
        query = [
            'UPDATE', self.dbtable,
            'SET', 'is_archived=2, claimed_by=NULL',
            'WHERE', 'type=%s AND subject=%s AND dumpdate=%s;'
        ]
        try:
            self.execute(' '.join(query), conds)
            return True
        except:
            return False

    def markFailedCheck(self, params):
        """
        This function is used to mark an item as failed when checking it.

        - params (dict): Information about the item with the keys "type",
        "subject" and "dumpdate".

        Returns: True if update is successful, False if an error occurred.
        """
        conv = BALConverter()
        arcdate = conv.getDateFromWiki(params['dumpdate'], archivedate=True)
        conds = (params['type'], params['subject'], arcdate)
        query = [
            'UPDATE', self.dbtable,
            'SET', 'is_checked=2, claimed_by=NULL',
            'WHERE', 'type=%s AND subject=%s AND dumpdate=%s;'
        ]
        try:
            self.execute(' '.join(query), conds)
            return True
        except:
            return False

    def updateProgress(self, params):
        """
        This function is used to update the progress of a dump.

        - params (dict): Information about the item with the keys "type",
        "subject", "dumpdate" and "progress".

        Returns: True if update is successful, False if an error occurred.
        """
        conds = (params['progress'], params['type'], params['subject'],
                 params['dumpdate'])
        query = [
            'UPDATE', self.dbtable,
            'SET', 'progress=%s',
            'WHERE', 'type=%s AND subject=%s AND dumpdate=%s;'
        ]
        try:
            self.execute(' '.join(query), conds)
            return True
        except:
            return False

    def addNewItem(self, params):
        """
        This function is used to insert a new item into the database.

        - params (dict): Information about the item with the keys "type",
        "subject", "dumpdate" and "progress".

        Returns: True if update is successful, False if an error occurred.
        """
        conds = (
            params['type'],
            params['subject'],
            params['dumpdate'],
            params['progress']
        )
        query = [
            'INSERT INTO', self.dbtable,
            '(type, subject, dumpdate, progress, claimed_by, can_archive,',
            'is_archived, is_checked, comments)',
            'VALUES', '(%s, %s, %s, %s, NULL, 0, 0, 0, NULL);'
        ]
        try:
            self.execute(' '.join(query), conds)
            return True
        except:
            return False

if __name__ == '__main__':
    raise IncorrectUsage("Script cannot be called directly")
