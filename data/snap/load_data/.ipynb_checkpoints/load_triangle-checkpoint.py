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

sql='''
CREATE MATERIALIZED VIEW same_circle_triangles (source, circle, node1, node2, node3)
AS (
    SELECT
        e1.source as source, e1.circle as circle, e1.fnode as node1, e2.fnode as node2, e3.fnode as node3
    FROM
        same_circle_edges e1, same_circle_edges e2, same_circle_edges e3
    WHERE
        e1.source = e2.source AND e2.source = e3.source
        AND e1.circle = e2.circle AND e2.circle = e3.circle
        AND e1.tnode = e2.fnode AND e2.tnode = e3.fnode AND e3.tnode = e1.fnode
        -- AND e1.fnode < e2.fnode AND e2.fnode < e3.fnode
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
cursor.execute('drop materialized view if exists same_circle_triangles')
cursor.execute(sql)
conn.close()
