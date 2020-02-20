#!/usr/bin/env python
import pandas as pd
import sys
import extract_table

plat = sys.argv[1]
#threshold = sys.argv[2]

arch = 'snap_%s'%plat

# I separate the function into files before.
#    top_triangles_csv = '../%s/top_triangles.csv'%plat
#    top_triangles = pd.read_csv(top_triangles_csv)

qprefix = '4star'
query_folder = '../../../queries/snap_%s/'%plat
schema_folder = '../../../schema/snap_%s/'%plat
query_metadata_folder = '../../../data/snap/%s/query_metadata/'%plat

def get_index(s):
    import hashlib
    m = hashlib.md5()
    m.update(s.encode('utf-8'))
    return m.hexdigest()[:6]

def main():
    edge_tables = set()
    tri_tables = set()
    #top_triangles = get_top_triangles(plat, threshold)
    query_metas = pd.read_csv(query_metadata_folder + 'q_{qprefix}.csv'.format(qprefix=qprefix))
    for _, query_meta in query_metas.iterrows():
        for i in [1,2,3]:
            edge_tables.add((query_meta['source'],  query_meta['circle%d'%i]))
        tri_tables.add((query_meta['source'],  query_meta['circle_tri']))
        add_query(query_meta.to_dict())
    add_edge_tables(edge_tables)
    add_tri_tables(tri_tables)

def add_query(info):
    info['qprefix'] = qprefix
    info['qfullname'] = 'q_{qprefix}_{source}_{circle1}_{circle2}_{circle3}_{circle_tri}_{cnt}'.format(**info)
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

def add_edge_tables(tables):
    for table in tables:
        source, circle = table
        extract_table.extract_edges(plat, source, circle)
        
def add_tri_tables(tables):
    for table in tables:
        source, circle = table
        extract_table.extract_triangles(plat, source, circle)

def get_top_triangles(plat, threshold):
    import pandas as pd
    import psycopg2 as pg2

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
        e1.source as source, e1.fcircle as circle1, e2.fcircle as circle2, e3.fcircle as circle3, count(*) as cnt
    from
        full_info e1, full_info e2, full_info e3
    WHERE
        True
        AND e1.fcircle < e2.fcircle
        AND e2.fcircle < e3.fcircle
        AND e1.source = e2.source
        AND e2.source = e3.source
        AND e1.tnode = e2.fnode AND e2.tnode = e3.fnode AND e3.tnode = e1.fnode
    GROUP BY
        e1.source, e1.fcircle, e2.fcircle, e3.fcircle
    ;
    '''
    triangles = pd.read_sql(sql, conn)
    top_triangles = triangles.query('cnt > %s'%threshold)
    return top_triangles
        
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
a 1 A tnode
c
r 4 tri_source_{source}_circle_{circle_tri}
a 1 A node1
a 2 B node2
a 3 C node3
c
n 1 {qname}_node_1 1
n 2 {qname}_node_2 2
n 3 {qname}_node_3 3
n 4 {qname}_node_4 4
e 4 1
e 4 2
e 4 3
c
d 1
dn t1 {qname}_dnode_t1 t1
c
d 2
dn t2 {qname}_dnode_t2 t2
c
d 3
dn t3 {qname}_dnode_t3 t3
c
d 4
dn b1 {qname}_dnode_b1 b1
dn b2 {qname}_dnode_b2 b2
dn b3 {qname}_dnode_b3 b3
de b1 b2
de b2 b3
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
a 1 A tnode
c
r 4 tri_source_{source}_circle_{circle_tri}
a 1 A node1
a 2 B node2
a 3 C node3
c
'''

if __name__ == '__main__':
    main()
