#!/usr/bin/env python
import pandas as pd
import psycopg2 as pg2
import sys
import time

plat = sys.argv[1]
threshold = sys.argv[2]
print('./... <plat> <threshold>')

database = 'arch-snap_%s-scale-1'%plat
conn = pg2.connect("host='localhost'  port=5422 user='duke' password='duke' dbname=%s"%database)

# Count how many triangles such that all individuals share the common friend, and each of edge from different circle
# Output:
# source, circle 1, circle 2, circle 3, triangles
sql = '''
select
    tri.source as source, e1.circle as circle1, e2.circle as circle2, e3.circle as circle3, count(*) as cnt
from
    same_circle_edges e1,  same_circle_edges e2,  same_circle_edges e3, same_circle_triangles tri
WHERE
    True
    AND tri.circle <> e1.circle
    AND tri.circle <> e2.circle
    AND tri.circle <> e3.circle
    AND e1.circle < e2.circle
    AND e2.circle < e3.circle
    AND e1.source = tri.source
    AND e2.source = tri.source
    AND e3.source = tri.source
    AND e1.fnode = tri.node1
    AND e1.tnode = tri.node2
    AND e2.fnode = tri.node2
    AND e2.tnode = tri.node3
    AND e3.fnode = tri.node3
    AND e3.tnode = tri.node1
GROUP BY
    tri.source, e1.circle, e2.circle, e3.circle
HAVING count(*) > %s
ORDER BY cnt DESC
;
'''%threshold
time_start = time.time()
results = pd.read_sql(sql, conn)
time_end = time.time()
print('time elapsed: %.3f'%(time_end - time_start))
results.to_csv('../%s/top_4stars.csv'%plat, index = False)
