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

import MySQLdb

from converter import BALConverter
from exception import IncorrectUsage
import message


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
        self.conv = BALConverter()

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

    def count(self, conds='', options='', params=()):
        """
        This function is used to get a count of the number of rows in the
        database depending on the given conditions.

        - conds (string): Conditions (WHERE clauses) to add.
        - options (string): Query options.
        - params (tuple): Parameters to substitute in the query.

        Returns: Int with the number of rows for a given query.
        """
        output = 0
        query = [
            'SELECT', 'COUNT(*)',
            'FROM', self.dbtable
        ]
        if (conds != ''):
            extra = ['WHERE', conds, options]
        else:
            extra = [options]
        query.extend(extra)
        execute = ' '.join(query) + ';'
        results = self.execute(execute, params)
        if results is None:
            output = 0
        else:
            for result in results:
                output = result[0]
        return output

    def insert(self, values={}, params=()):
        """
        This function is used for inserting new rows into the database.

        - values (dict): A dictionary with key and value pairs to insert.
        - params (tuple): Parameters to substitute in the query.

        Returns: True if insert is successful, False if an error occurred.
        """
        keys = []
        vals = []
        for key, val in values.iteritems():
            keys.append(key)
            vals.append(val)

        query = [
            'INSERT INTO', self.dbtable,
            '(' + ', '.join(keys) + ')',
            'VALUES', '(' + ', '.join(vals) + ')'
        ]
        execute = ' '.join(query) + ';'
        try:
            self.execute(execute, params)
            return True
        except:
            return False

    def select(self, columns=[], conds='', options='', params=()):
        """
        This function is used for obtaining information from the database.

        - columns (dict): The column(s) to retrieve from the database.
        - conds (string): Conditions (WHERE clauses) to add.
        - options (string): Query options.
        - params (tuple): Parameters to substitute in the query.

        Note: options should contain a LIMIT clause to prevent retrieving an
        excessive amount of data from the database.

        Returns: Tuple with the SQL query results, None if empty set.
        """
        query = [
            'SELECT', ', '.join(columns),
            'FROM', self.dbtable
        ]
        if (conds != ''):
            extra = ['WHERE', conds, options]
        else:
            extra = [options]
        query.extend(extra)
        execute = ' '.join(query) + ';'
        return self.execute(execute, params)

    def update(self, values={}, conds='', params=()):
        """
        This function is used for updating information in the database. This
        is different from INSERT (see self.insert instead).

        - values (dict): A dictionary of columns and values to update.
        - conds (string): Conditions (WHERE clauses) to add.
        - params (tuple): Parameters to substitute in the query.

        Returns: True if update is successful, False if an error occurred.
        """
        vals = []
        for key, val in values.iteritems():
            vals.append('%s=%s' % (key, val))
        query = [
            'UPDATE', self.dbtable,
            'SET', ', '.join(vals)
        ]
        if (conds != ''):
            extra = ['WHERE', conds]
        else:
            extra = []
        query.extend(extra)
        execute = ' '.join(query) + ';'
        try:
            self.execute(execute, params)
            return True
        except:
            return False

    def getAllDumps(self, wikidb, progress="all", can_archive="all",
                    is_archived="all", is_checked="all"):
        """
        This function is used to get all dumps of a specific wiki (up to 30).

        - progress (string): Dumps with this progress will be returned, "all"
        for all progress statuses.
        - can_archive (string): Dumps with this can_archive status will be
        returned, "all" for all can_archive statuses.
        - is_archived (string): Dumps with this is_archived status will be
        returned, "all" for all is_archived statuses.
        - is_checked (string): Dumps with this is_checked status will be
        returned, "all" for all is_checked statuses.

        Returns: Dict with all dumps of a wiki.
        """
        dumps = []
        conds = ['subject="%s"' % wikidb]

        if progress == "all":
            pass
        else:
            conds.append('progress="%s"' % (progress))

        if can_archive == "all":
            pass
        else:
            conds.append('can_archive="%s"' % (can_archive))

        if is_archived == "all":
            pass
        else:
            conds.append('is_archived="%s"' % (is_archived))

        if is_checked == "all":
            pass
        else:
            conds.append('is_checked="%s"' % (is_checked))

        options = 'ORDER BY dumpdate DESC LIMIT 30'
        results = self.select(['dumpdate'], ' AND '.join(conds), options)
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
        arcdate = self.conv.getDateFromWiki(params['date'],
                                            archivedate=True)
        values = {
            'claimed_by': '"%s"' % (self.hostname)
        }
        conds = [
            'type="%s"' % (params['type']),
            'subject="%s"' % (params['subject']),
            'dumpdate="%s"' % (arcdate)
        ]
        results = self.update(values=values, conds=' AND '.join(conds))
        if results:
            return True
        else:
            return False

    def updateCanArchive(self, params):
        """
        This function is used to update the status of whether a dump can be
        archived.

        - params (dict): Information about the item with the keys "type",
        "subject" and "dumpdate".

        Returns: True if update is successful, False if an error occurred.
        """
        arcdate = self.conv.getDateFromWiki(params['date'],
                                            archivedate=True)
        values = {
            'can_archive': '"%s"' % (params['can_archive'])
        }
        conds = [
            'type="%s"' % (params['type']),
            'subject="%s"' % (params['subject']),
            'dumpdate="%s"' % (arcdate)
        ]
        results = self.update(values=values, conds=' AND '.join(conds))
        if results:
            return True
        else:
            return False

    def markArchived(self, params):
        """
        This function is used to mark an item as archived after doing so.

        - params (dict): Information about the item with the keys "type",
        "subject" and "dumpdate".

        Returns: True if update is successful, False if an error occurred.
        """
        arcdate = self.conv.getDateFromWiki(params['date'],
                                            archivedate=True)
        values = {
            'is_archived': '"1"',
            'claimed_by': 'NULL'
        }
        conds = [
            'type="%s"' % (params['type']),
            'subject="%s"' % (params['subject']),
            'dumpdate="%s"' % (arcdate)
        ]
        results = self.update(values=values, conds=' AND '.join(conds))
        if results:
            return True
        else:
            return False

    def markChecked(self, params):
        """
        This function is used to mark an item as checked after doing so.

        - params (dict): Information about the item with the keys "type",
        "subject" and "dumpdate".

        Returns: True if update is successful, False if an error occurred.
        """
        arcdate = self.conv.getDateFromWiki(params['date'],
                                            archivedate=True)
        values = {
            'is_checked': '"1"',
            'claimed_by': 'NULL'
        }
        conds = [
            'type="%s"' % (params['type']),
            'subject="%s"' % (params['subject']),
            'dumpdate="%s"' % (arcdate)
        ]
        results = self.update(values=values, conds=' AND '.join(conds))
        if results:
            return True
        else:
            return False

    def markFailedArchive(self, params):
        """
        This function is used to mark an item as failed when archiving it.

        - params (dict): Information about the item with the keys "type",
        "subject" and "dumpdate".

        Returns: True if update is successful, False if an error occurred.
        """
        arcdate = self.conv.getDateFromWiki(params['date'],
                                            archivedate=True)
        values = {
            'is_archived': '"2"',
            'claimed_by': 'NULL'
        }
        conds = [
            'type="%s"' % (params['type']),
            'subject="%s"' % (params['subject']),
            'dumpdate="%s"' % (arcdate)
        ]
        results = self.update(values=values, conds=' AND '.join(conds))
        if results:
            return True
        else:
            return False

    def markFailedCheck(self, params):
        """
        This function is used to mark an item as failed when checking it.

        - params (dict): Information about the item with the keys "type",
        "subject" and "dumpdate".

        Returns: True if update is successful, False if an error occurred.
        """
        arcdate = self.conv.getDateFromWiki(params['date'],
                                            archivedate=True)
        values = {
            'is_checked': '"2"',
            'claimed_by': 'NULL'
        }
        conds = [
            'type="%s"' % (params['type']),
            'subject="%s"' % (params['subject']),
            'dumpdate="%s"' % (arcdate)
        ]
        results = self.update(values=values, conds=' AND '.join(conds))
        if results:
            return True
        else:
            return False

    def updateProgress(self, params):
        """
        This function is used to update the progress of a dump.

        - params (dict): Information about the item with the keys "type",
        "subject", "dumpdate" and "progress".

        Returns: True if update is successful, False if an error occurred.
        """
        arcdate = self.conv.getDateFromWiki(params['date'],
                                            archivedate=True)
        values = {
            'progress': '"%s"' % (params['progress'])
        }
        conds = [
            'type="%s"' % (params['type']),
            'subject="%s"' % (params['subject']),
            'dumpdate="%s"' % (arcdate)
        ]
        results = self.update(values=values, conds=' AND '.join(conds))
        if results:
            return True
        else:
            return False

    def addNewItem(self, params):
        """
        This function is used to insert a new item into the database.

        - params (dict): Information about the item with the keys "type",
        "subject", "dumpdate" and "progress".

        Returns: True if update is successful, False if an error occurred.
        """
        arcdate = self.conv.getDateFromWiki(params['date'],
                                            archivedate=True)
        values = {
            'type': '"%s"' % (params['type']),
            'subject': '"%s"' % (params['subject']),
            'dumpdate': '"%s"' % (arcdate),
            'progress': '"%s"' % (params['progress']),
            'claimed_by': 'NULL',
            'can_archive': '"0"',
            'is_archived': '"0"',
            'is_checked': '"0"',
            'comments': 'NULL'
        }
        results = self.insert(values=values)
        if results:
            return True
        else:
            return False

    def getItemsLeft(self, params={}):
        """
        This function is used to get the number of items left to work with.

        - params (dict): The conditions to put in the WHERE clause.

        Returns: Int with number of items left to work with.
        """
        conds = ['claimed_by IS NULL']
        for key, val in params.iteritems():
            conds.append('%s="%s"' % (key, val))
        return self.count(' AND '.join(conds))

    def getRandomItem(self, dumptype=None, archived=False, debug=False):
        """
        This function is used to get a random item to work on.

        - dumptype (string): The specific dump type to work on.
        - archived (boolean): Whether or not to obtain a random item that is
        already archived.
        - debug (boolean): Whether or not to run this function in debug mode.

        Returns: Dict with the parameters to the archiving scripts.
        """
        output = {}
        columns = ['type', 'subject', 'dumpdate']
        options = 'ORDER BY RAND() LIMIT 1'
        conds = ['claimed_by IS NULL']

        if (archived):
            extra = [
                'is_archived="1"',
                'is_checked="0"'
            ]
        else:
            extra = [
                'progress="done"',
                'is_archived="0"',
                'can_archive="1"'
            ]
        conds.extend(extra)

        if dumptype is None:
            pass
        else:
            # Replacement is to avoid SQL injection attacks
            conds.append('type="%s"' % (dumptype.replace(" ", "_")))

        results = self.select(columns, ' AND '.join(conds), options)
        if results is None:
            ou
        for result in results:
            output = {
                'type': result[0],
                'subject': result[1],
                'date': result[2].strftime("%Y%m%d")
            }

        # Claim the item from the database server if not in debug mode
        if debug:
            pass
        else:
            self.claimItem(params=output)

        return output

if __name__ == "__main__":
    BALMessage = message.BALMessage()
    raise IncorrectUsage(BALMessage.getMessage('exception-incorrectusage'))
