#!/usr/bin/env python
import pandas as pd
import sys
import extract_table

plat = sys.argv[1]
arch = 'snap_%s'%plat
top_triangles_csv = '../%s/top_triangles.csv'%plat
top_triangles = pd.read_csv(top_triangles_csv)
query_folder = '../../../queries/snap_%s/'%plat
schema_folder = '../../../schema/snap_%s/'%plat

def main():
    tables = set()
    for _, triangle in top_triangles.iterrows():
        for i in [1,2,3]:
            tables.add((triangle['source'],  triangle['circle%d'%i]))
        add_query(triangle.to_dict())
    add_tables(tables)

def add_schema(tables):
    global schema_template
    i = 1
    schema = schema_template
    for table in tables:
        source, circle = table
        table_name = 'source_{source}_circle_{circle}'.format(source=source, circle=circle)
        schema += 'r {i} {table}\na {i} X fnode\na {i} Y tnode\nc\n'.format(i=i, table=table_name)
        i += 1
    sfile = open(schema_folder + 'snap_%s.schema'%plat, 'w')
    sfile.write(schema)
    sfile.close()

def add_query(triangle):
    triangle['qname'] = 'q{source}_{circle1}_{circle2}_{circle3}_{cnt}'.format(**triangle)
    query = query_template.format(**triangle)
    schema = schema_template.format(**triangle)
    qfile = open(query_folder + triangle['qname'] +'.hypertree', 'w')
    sfile = open(schema_folder + triangle['qname'] +'.schema', 'w')
    qfile.write(query)
    sfile.write(schema)
    qfile.close()
    sfile.close()

def add_tables(tables):
    for table in tables:
        source, circle = table
        extract_table.main(plat, source, circle)

query_template = '''
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
a 1 A fnode
a 3 C tnode
c
n 1 {qname}_node_tri 1 2 3
c
d 1
dn 2 {qname}_dnode_1_c2 c2
dn 3 {qname}_dnode_1_c3 c3
de 2 3
c
d 2
dn 1 {qname}_dnode_1_c1 c1
dn 3 {qname}_dnode_1_c3 c3
de 1 3
c
d 3
dn 1 {qname}_dnode_1_c1 c1
dn 2 {qname}_dnode_1_c2 c2
de 1 2
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
a 1 A fnode
a 3 C tnode
c
'''

if __name__ == '__main__':
    main()
