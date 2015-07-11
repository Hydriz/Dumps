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

import os
import unittest
import pep8

ignore_patterns = ('.git', 'bin', 'lib' + os.sep + 'python')


def ignore(dir):
    """Should the directory be ignored?"""
    for pattern in ignore_patterns:
        if pattern in dir:
            return True
    return False


class TestBalchivist(unittest.TestCase):
    def test_pep8(self):
        style = pep8.StyleGuide()
        # Ignore E402: Module level import not at top of file
        style.options.ignore += ('E402',)
        python_files = []
        errors = 0
        for root, _, files in os.walk('.'):
            if ignore(root):
                continue
            for f in files:
                if f.endswith('.py'):
                    python_files.append(os.path.join(root, f))
                else:
                    continue
            errors += style.check_files(python_files).total_errors
            self.assertEqual(errors, 0, 'PEP8 style errors: %d' % errors)
