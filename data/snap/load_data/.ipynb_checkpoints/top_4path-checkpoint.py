#!/usr/bin/env python
import pandas as pd
import psycopg2 as pg2
import sys

plat = sys.argv[1]
threshold = sys.argv[2]
print('./... <plat> <threshold>')

database = 'arch-snap_%s-scale-1'%plat
conn = pg2.connect("host='localhost'  port=5422 user='duke' password='duke' dbname=%s"%database)

# Count how many triangles such that all individuals share the common friend, and each of edge from different circle
# Output:
# source, circle 1, circle 2, circle 3, triangles
sql = '''
WITH full_info AS (
    SELECT
        edges.source, fnode, c1.circle as fcircle, tnode, c2.circle as tcircle
    FROM
        edges, circles c1, circles c2
    where edges.source = c1.source AND edges.source = c2.source AND edges.fnode = c1.node AND edges.tnode = c2.node
        AND c1.circle = c2.circle
)
select
    e1.source as source, e1.fcircle as circle1, e2.fcircle as circle2, e3.fcircle as circle3, e4.fcircle as circle4, count(*) as cnt
from
    full_info e1, full_info e2, full_info e3, full_info e4
WHERE
    True
    AND e1.fcircle < e2.fcircle
    AND e2.fcircle < e3.fcircle
    AND e3.fcircle < e4.fcircle
    AND e1.source = e2.source
    AND e2.source = e3.source
    AND e3.source = e4.source
    AND e1.tnode = e2.fnode AND e2.tnode = e3.fnode AND e3.tnode = e4.fnode 
GROUP BY
    e1.source, e1.fcircle, e2.fcircle, e3.fcircle, e4.fcircle
HAVING count(*) > %s
ORDER BY cnt DESC
;
'''%threshold
triangles = pd.read_sql(sql, conn)
top_triangles = triangles.query('cnt > %s'%threshold)
top_triangles.to_csv('../%s/top_4path.csv'%plat, index = False)
