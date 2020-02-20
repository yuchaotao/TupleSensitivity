#!/usr/bin/env python
import sys
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import psycopg2 as pg2

sql='''
CREATE MATERIALIZED VIEW same_circle_edges (source, circle, fnode, tnode)
AS (
    SELECT
        edges.source, c1.circle as circle, fnode, tnode
    FROM
        edges, circles c1, circles c2
    WHERE edges.source = c1.source AND edges.source = c2.source AND edges.fnode = c1.node AND edges.tnode = c2.node
        AND c1.circle = c2.circle
)
'''


# Get the target database name
plat = sys.argv[1]
print('./... <plat>')
arch = 'snap_%s'%plat
scale = '1'
database = 'arch-%s-scale-%s'%(arch, scale)

# Create the target view
print('connecting')
conn = pg2.connect('user=duke host=localhost password=duke port=5422 dbname=%s'%database)
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cursor = conn.cursor()
cursor.execute(sql)
conn.close()
