#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
import sqlite3
import sys

from datetime import datetime
from os import path


if len(sys.argv) < 2:
    print 'Please provide a log filename'
    sys.exit(2)

if not path.isfile(sys.argv[1]):
    print 'Cannot find the file "%s"' % sys.argv[1]
    sys.exit(2)


filename = sys.argv[1]
db_name = 'logs.sqlite3' if (len(sys.argv) < 3) else sys.argv[2]
conn = sqlite3.connect(db_name)
pattern = re.compile(r"""(?P<ipaddress>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) - - \[(?P<datetime>\d{2}\/[a-z]{3}\/\d{4}:\d{2}:\d{2}:\d{2} (\+|\-)\d{4})\] ((\"(?P<verb>GET|POST|PATCH|PUT|DELETE|OPTIONS|HEAD) )(?P<url>.+)(http\/1\.1")) (?P<status>\d{3}) (?P<size>\d+) (["](?P<referrer>(\-)|(.+))["]) (["](?P<useragent>.+)["])""",
          re.IGNORECASE)
logs = []


with open(filename) as f:
    for line in f:
        m = pattern.match(line)

        if m is None:
            continue

        dt = m.group('datetime').split(' ')
        tz = dt[1]
        dt = datetime.strptime(dt[0], "%d/%b/%Y:%H:%M:%S")

        log = (None, m.group('ipaddress'), dt.strftime('%Y-%m-%d %H:%M:%S'),
               m.group('verb'), m.group('status'), m.group('url'),
               m.group('referrer'), m.group('useragent'))
        logs.append(log)


with conn:
    curr = conn.cursor()
    curr.execute("DROP TABLE IF EXISTS Logs")
    curr.execute("CREATE TABLE  Logs (id INTEGER PRIMARY KEY AUTOINCREMENT, ip_address TEXT, dt DATETIME,verb TEXT, status TEXT, url TEXT, referrer TEXT, useragent TEXT);")
    curr.executemany("INSERT INTO Logs VALUES(?, ?, ?, ?, ?, ?, ?, ?)", logs)
