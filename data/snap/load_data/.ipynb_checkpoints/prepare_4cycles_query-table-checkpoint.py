#!/usr/bin/env python
import pandas as pd
import psycopg2 as pg2
import numpy as np
import sys
import extract_table

plat = sys.argv[1]

arch = 'snap_%s'%plat

# I separate the function into files before.
#    top_triangles_csv = '../%s/top_triangles.csv'%plat
#    top_triangles = pd.read_csv(top_triangles_csv)

qprefix = '4cycle'
query_folder = '../../../queries/snap_%s/'%plat
schema_folder = '../../../schema/snap_%s/'%plat
query_metadata_folder = '../../../data/snap/%s/query_metadata/'%plat

def get_index(s):
    import hashlib
    m = hashlib.md5()
    m.update(s.encode('utf-8'))
    return m.hexdigest()[:6]

def fetch_table(source, circle, rename):
    sql = '''
    WITH 
        edges AS (SELECT fnode, tnode FROM edges WHERE source = '{source}'),
        circles AS (SELECT node FROM circles WHERE source = '{source}' AND circle = '{circle}')
    SELECT fnode, tnode FROM edges WHERE fnode in (select * from circles) AND tnode in (select * from circles);
    '''.format(source=source, circle=circle)
    sql = ''
    return sql

def add_query(info):
    info['qprefix'] = qprefix
    info['qfullname'] = 'q_{qprefix}_{source}_{circle1}_{circle2}_{circle3}_{circle4}_{cnt}}'.format(**info)
    info['index'] = get_index(info['qfullname'])
    info['qname'] = 'q_{qprefix}_{index}'.format(**info)
    query = query_template.format(**info)
    schema = schema_template.format(**info)
    qfile = open(query_folder + info['qname'] +'.hypertree', 'w')
    sfile = open(schema_folder + info['qname'] +'.schema', 'w')
    qfile.write(query)
    sfile.write(schema)
    qfile.close()
    sfile.close()

def add_table(table):
    source, circle = table
    extract_table.extract_edges(plat, source, circle)
    
def find_queries():
    database = 'arch-snap_%s-scale-1'%plat
    conn = pg2.connect("host='localhost'  port=5422 user='duke' password='duke' dbname=%s"%database)

    source_limit = 10
    sql = 'select source from circles group by source limit %d;'%source_limit
    sources = pd.read_sql(sql, conn)['source'].tolist()

    queries_limit = 20
    queries = []
    tables = set()
    unique = set()
    while True:
        source = np.random.choice(sources)
        sql = "select circle from circles where source='{source}' group by circle;".format(source=source)
        circles = pd.read_sql(sql, conn)['circle'].tolist()
        if len(circles) < 4:
            continue
        target_circles = np.random.choice(circles, 4, replace=False)
        index = tuple([source] +  list(target_circles))
        if index in unique:
            continue
        else:
            unique.add(index)
        info = dict()
        info['qprefix'] = qprefix
        info['source'] = source
        for i, circle in enumerate(target_circles):
            info['circle%d'%(i+1)] = circle
            tables.add((source, circle))
        queries.append(info)
        if len(queries) >= queries_limit:
            break
            
    return queries, tables
    
def main():
    #queries, tables = find_queries()
    queries = []
    tables = set()
    #top_triangles = get_top_triangles(plat, threshold)
    query_metas = pd.read_csv(query_metadata_folder + 'q_{qprefix}.csv'.format(qprefix=qprefix))
    for _, query_meta in query_metas.iterrows():
        for i in [1,2,3]:
            tables.add((query_meta['source'],  query_meta['circle%d'%i]))
        queries.append(query_meta.to_dict())
    
    for query in queries:
        add_query(query)
    for table in tables:
        add_table(table)
        
query_template = '''c {qfullname}
c This is a hypertree for {qname} (from snap_facebook)
c
c Genral format: FLAG INDEX NAME OTHER
c Flag meaning:
c 'c': comment
c 'r': relation
c 'a': attribute
c 'n': node
c 'e': edge (directed)
c 'd': start defining double tree. 'd <relation index> [double nodes]'
c 'de': double edges.
c 'dn': double nodes. the index list after the name is its nodes/relations
c       Each vertex name is made from a character and a number.
c       For instance:    'c1' means cohort relation 1,
c                        'b2' means botjoin node 2,
c                        't3' means topjoin node 3.
c For each node, the index list after the node name is its relations
c For each edge, the first node index is parent, the second is its child
c For each attribute, we project the second name to the first name
c
r 1 source_{source}_circle_{circle1}
a 1 A fnode
a 2 B tnode
c
r 2 source_{source}_circle_{circle2}
a 2 B fnode
a 3 C tnode
c
r 3 source_{source}_circle_{circle3}
a 3 C fnode
a 4 D tnode
c
r 4 source_{source}_circle_{circle4}
a 4 D fnode
a 1 A tnode
c
n 1 {qname}_node_half_12 1 2
n 2 {qname}_node_half_34 3 4
e 1 2
c
d 1
dn c2 {qname}_dnode_c2 c2
dn b2 {qname}_dnode_b2 b2
de c2 b2
c
d 2
dn c1 {qname}_dnode_c1 c1
dn b2 {qname}_dnode_b2 b2
de c1 b2
c
d 3
dn c4 {qname}_dnode_c4 c4
dn t2 {qname}_dnode_t2 t2
de c4 t2
c
d 4
dn c3 {qname}_dnode_c3 c3
dn t2 {qname}_dnode_t2 t2
de c3 t2
c
c
'''

schema_template = '''c This is a schema file for snap_facebook
c We don't include specific foreignkey-primarykey pairs here
c We only track the relationship among relations
c
c Denote 'r <i1> <r1>' as a relation with index <i1> and name <r1>
c Denote 'e <i1> <i2> [a :list]' as a directed edge from r1(primary) to r2(foreign). So r2 is the parent of r1.
c        a :list is a list of attributes that has the primary-foreign key relation.
c
r 1 source_{source}_circle_{circle1}
a 1 A fnode
a 2 B tnode
c
r 2 source_{source}_circle_{circle2}
a 2 B fnode
a 3 C tnode
c
r 3 source_{source}_circle_{circle3}
a 3 C fnode
a 4 D tnode
c
r 4 source_{source}_circle_{circle4}
a 4 D fnode
a 1 A tnode
c
'''

if __name__ == '__main__':
    main()
