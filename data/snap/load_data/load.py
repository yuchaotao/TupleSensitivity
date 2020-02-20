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

# Create the target database
print('connecting')
conn = pg2.connect('user=duke host=localhost password=duke port=5422')
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cursor = conn.cursor()
cursor.execute('drop database if exists \"%s\"'%database)
cursor.execute('create database \"%s\"'%database)
conn.close()

# Read from files and load into the target database
conn = pg2.connect('user=duke host=localhost password=duke port=5422 dbname=%s'%database)
load_util.create_circles(conn)
load_util.create_edges(conn)

dirname = '../%s/circles'%plat
source_set = load_util.retrieve_source_set(dirname)
for source in source_set:
    print("loading %s"%source)
    load_util.insert_circles(dirname, source, conn)
    load_util.insert_edges(dirname, source, conn)

conn.close()
