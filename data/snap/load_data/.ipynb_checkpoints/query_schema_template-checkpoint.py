queries = {
'triangle' : '''c {qfullname}
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
n 1 {qname}_node_12 1 2
n 3 {qname}_node_3 3
c
e 1 3
c
d 1
dn 2 {qname}_dnode_c2 c2
dn 3 {qname}_dnode_b3 b3
de 2 3
c
d 2
dn 1 {qname}_dnode_c1 c1
dn 3 {qname}_dnode_b3 b3
de 1 3
c
d 3
dn 4 {qname}_dnode_t3 t3
c
c
''',
    
'4path' : '''c {qfullname}
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
a 5 E tnode
c
n 1 {qname}_node_1 1
n 2 {qname}_node_2 2
n 3 {qname}_node_3 3
n 4 {qname}_node_4 4
c
e 1 2
e 2 3
e 3 4
c
d 1
dn b2 {qname}_dnode_b2 b2
c
d 2
dn t2 {qname}_dnode_t2 t2
dn b3 {qname}_dnode_b3 b3
c
d 3
dn t3 {qname}_dnode_t3 t3
dn b4 {qname}_dnode_b4 b4
c
d 4
dn t4 {qname}_dnode_t4 t4
c
''',

'4cycle' : '''c {qfullname}
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
''',
'4star' : '''c {qfullname}
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
}

schemas = {
'triangle' : '''c This is a schema file for snap_facebook
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
''',
'4path' : '''c This is a schema file for snap_facebook
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
a 5 E tnode
c
''',
'4cycle' : '''c This is a schema file for snap_facebook
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
''',
'4star' : '''c This is a schema file for snap_facebook
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
}