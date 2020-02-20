#!/usr/bin/env python
import load_util
import psycopg2 as pg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import sys

# Get the target database name
plat = sys.argv[1]
print('./... <plat>')
arch = 'snap_%s'%plat
scale = '1'
database = 'arch-%s-scale-%s'%(arch, scale)

# Read from files and load into the target database
conn = pg2.connect('user=duke host=localhost password=duke port=5422 dbname=%s'%database)
load_util.create_combined(conn)

filename = '../%s/%s_combined.txt'%(plat, plat)
load_util.insert_combined(filename, conn)

conn.close()

