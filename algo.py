#!/usr/bin/python3

import psycopg2 as pg2
import psycopg2.extras
import time
from typing import List, Set, Tuple
from copy import copy

DEBUG = True
#DEBUG = False

#RECREATE_TABLE = True
RECREATE_TABLE = False

class Attribute:
    def __init__(self, index, join_name, orig_name):
        self.index = index
        self.join_name = join_name
        self.orig_name = orig_name

    def __eq__(self, other):
        return other and self.join_name == other.join_name

    def __hash__(self):
        return hash(self.join_name)

    def __str__(self):
        return self.join_name

    __repr__ = __str__

class Relation:
    def __init__(self, index, name, attributes: Set[Attribute]):
        self.index = index
        self.name = name
        self.attributes = attributes

    def rename(self):
        new_attrs = ','.join('%s AS %s'%(attr.orig_name, attr.join_name) for attr in self.attributes)
        new_reln  = '(SELECT {new_attrs} FROM {reln}) AS {reln}'.format(new_attrs=new_attrs, reln=self.name)
        return new_reln

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.__str__()

def _Relation(index, name, attributes: List[str]):
    attributes = set(Attribute(0, attr, attr) for attr in attributes)
    return Relation(index, name, attributes)

class Node:
    def __init__(self, index, name, relations: List[Relation]):
        self.index = index
        self.name = name
        self.relations = relations

        self.parent = None
        self.children = []
        self.botjoin = None
        self.topjoin = None
        self.attributes = union_attributes(relations)

    def get_cohorts(self, relation):
        return [rel for rel in self.relations if rel != relation]

    def isRoot(self):
        return self.parent is None

    def isLeaf(self):
        return len(self.children) == 0

    def __str__(self):
        return self.name

    __repr__ = __str__

class Tree:
    def __init__(self, nodes: List[Node]):
        self.nodes = nodes
        self.node_map = {}
        self.demap_nodes(nodes)

    def demap_nodes(self, nodes):
        for node in nodes:
            self.demap_node(node)

    def demap_node(self, node):
        for relation in node.relations:
            self.node_map[relation] = node

def union_attributes(relation_list: List[Relation]):
    attribute_set = copy(relation_list[0].attributes)
    for relation in relation_list:
        attribute_set |= relation.attributes
    return attribute_set

def common_attributes(node_list: List[Node]):
    attribute_set = copy(node_list[0].attributes)
    for node in node_list:
        attribute_set &= node.attributes
    return attribute_set

def get_neighbours(node):
    return [child for child in node.parent.children if child != node]

def gen_sqlstr_padding(sqlstr):
    return " " + sqlstr + " "

def gen_sqlstr_childmutli(node: Node):
    sql = " * ".join(["C_{i}_{pi}".format(i=child.index, pi=node.index) for child in node.children])
    sql = gen_sqlstr_padding(sql)
    return sql

def gen_sqlstr_neighmutli(node: Node):
    sql = " * ".join(["C_{i}_{pi}".format(i=neigh.index, pi=node.parent.index) for neigh in get_neighbours(node)])
    sql = gen_sqlstr_padding(sql)
    return sql

def gen_sqlstr_freqmulti(center_node:Node, node_flag_list: List[Tuple[Node, str]]):
    freq_strs = []
    for node, flag in node_flag_list:
        if flag == 'cohort':
            continue
        elif flag == 'top':
            freq_str = 'C_{i}_{pi}'.format(i=center_node.index, pi=node.parent.index)
        elif flag == 'bot':
            freq_str = 'C_{i}_{pi}'.format(i=node.index, pi=center_node.index)
        freq_strs.append(freq_str)
    return ' * '.join(freq_strs)

def gen_sqlstr_attributes(attributes):
    sql = ", ".join(map(str, attributes))
    sql = gen_sqlstr_padding(sql)
    return sql

def gen_sqlstr_joins(node_names):
    sql = " NATURAL JOIN ".join(node_names)
    sql = gen_sqlstr_padding(sql)
    return sql

def _prepare_botjoin(node: Node):
    if node.botjoin is not None:
        return node

    name    =   get_botjoin_tablename(node)

    if node.isRoot():
        pass
    elif node.isLeaf():
        parent  =   node.parent

        attrs   =   gen_sqlstr_attributes(common_attributes([node, parent]))

        sql = "SELECT {attrs}, COUNT(*) AS C_{i}_{pi} FROM {relation} GROUP BY {attrs}"
        sql = sql.format(attrs=attrs, i=node.index, pi=parent.index, relation=node.name)
        create_table(sql, name)
    else:
        for child in node.children:
            _prepare_botjoin(child)

        parent  =   node.parent
        children    =   node.children
        join_relations  =   [node.name] + [get_botjoin_tablename(child) for child in children]

        attrs   =   gen_sqlstr_attributes(common_attributes([node, parent]))
        childmulti  =   gen_sqlstr_childmutli(node)
        joins   =   gen_sqlstr_joins(join_relations)

        sql = "SELECT {attrs}, SUM({childmulti}) AS C_{i}_{pi} FROM {joins} GROUP BY {attrs}"
        sql = sql.format(attrs=attrs, i=node.index, pi=parent.index, childmulti=childmulti, joins=joins)
        create_table(sql, name)

    node.botjoin = name
    return node

def _prepare_topjoin(node: Node):
    if node.topjoin is not None:
        return node
    name    =   get_topjoin_tablename(node)
    parent  =   node.parent
    children    =   node.children

    if node.isRoot():
        return node
    elif node.parent.isRoot():
        if len(get_neighbours(node)) == 0:
            join_relations = [parent.name]

            attrs   =   gen_sqlstr_attributes(common_attributes([node, parent]))
            joins   =   gen_sqlstr_joins(join_relations)

            sql = "SELECT {attrs}, COUNT(*) AS C_{i}_{pi} FROM {joins} GROUP BY {attrs}"
            sql = sql.format(attrs=attrs, i=node.index, pi=parent.index, joins=joins)
        else:

            join_relations = [parent.name] + [get_botjoin_tablename(neigh) for neigh in get_neighbours(node)]

            attrs   =   gen_sqlstr_attributes(common_attributes([node, parent]))
            neighmulti  =   gen_sqlstr_neighmutli(node)
            joins   =   gen_sqlstr_joins(join_relations)

            sql = "SELECT {attrs}, SUM({neighmulti}) AS C_{i}_{pi} FROM {joins} GROUP BY {attrs}"
            sql = sql.format(attrs=attrs, i=node.index, pi=parent.index, neighmulti=neighmulti, joins=joins)
    else:
        _prepare_topjoin(node.parent)

        join_relations = [parent.name] + [get_topjoin_tablename(parent)] + [get_botjoin_tablename(neigh) for neigh in get_neighbours(node)]

        attrs   =   gen_sqlstr_attributes(common_attributes([node, parent]))
        neighmulti  =   gen_sqlstr_neighmutli(node)
        joins   =   gen_sqlstr_joins(join_relations)

        if len(get_neighbours(node)) == 0:
            sql = "SELECT {attrs}, SUM(C_{pi}_{ppi})  AS C_{i}_{pi} FROM {joins} GROUP BY {attrs}"
            sql = sql.format(attrs=attrs, i=node.index, pi=parent.index, ppi=parent.parent.index, joins=joins)
        else:
            sql = "SELECT {attrs}, SUM(C_{pi}_{ppi} * {neighmulti}) AS C_{i}_{pi} FROM {joins} GROUP BY {attrs}"
            sql = sql.format(attrs=attrs, i=node.index, pi=parent.index, ppi=parent.parent.index, neighmulti=neighmulti, joins=joins)

    create_table(sql, name)
    node.topjoin = name
    return node

def _prepare_freqtable(node: Node):
    name    =   get_nodefreq_tablename(node)
    parent  =   node.parent
    children    =   node.children

    if node.isRoot():
        join_relations  =   [get_botjoin_tablename(child) for child in children]

        attrs   =   gen_sqlstr_attributes(node.attributes)
        childmulti  =   gen_sqlstr_childmutli(node)
        joins   =   gen_sqlstr_joins(join_relations)

        sql = "SELECT {attrs}, SUM({childmulti}) AS freq FROM {joins} GROUP BY {attrs}"
        sql = sql.format(attrs=attrs, childmulti=childmulti, joins=joins)
        create_table(sql, name)
    elif node.isLeaf():
        join_relations  =   [get_topjoin_tablename(node)]

        attrs   =   gen_sqlstr_attributes(node.attributes)
        joins   =   gen_sqlstr_joins(join_relations)

        sql = "SELECT {attrs}, SUM(C_{i}_{pi}) AS freq FROM {joins} GROUP BY {attrs}"
        sql = sql.format(attrs=attrs, i=node.index, pi=parent.index, joins=joins)
        create_table(sql, name)
    else:
        join_relations  =   [get_topjoin_tablename(node)] + [get_botjoin_tablename(child) for child in children]

        attrs   =   gen_sqlstr_attributes(node.attributes)
        childmulti  =   gen_sqlstr_childmutli(node)
        joins   =   gen_sqlstr_joins(join_relations)

        sql = "SELECT {attrs}, SUM(C_{i}_{pi} * {childmulti}) AS freq FROM {joins} GROUP BY {attrs}"
        sql = sql.format(attrs=attrs, i=node.index, pi=parent.index, childmulti=childmulti, joins=joins)
        create_table(sql, name)

def _prepare_tuplesens(reln: Relation, node: Node):
    name = get_relnsens_tablename(reln)

    attrs   =   gen_sqlstr_attributes(reln.attributes)

    #subquery = "(SELECT {attrs}, SUM(freq) AS freq FROM {nodefreq} GROUP BY {attrs}) AS {new_name}"
    #subquery = subquery.format(attrs=attrs, nodefreq=get_nodefreq_tablename(node), new_name=get_grouped_nodefreq_tablename(node, reln))
    #join_relations  =   [subquery] + [cohort.name for cohort in node.get_cohorts(reln)]

    join_relations  =   [get_nodefreq_tablename(node)] + [cohort.name for cohort in node.get_cohorts(reln)]

    joins   =   gen_sqlstr_joins(join_relations)

    sql = "SELECT {attrs}, SUM(freq) AS tsens FROM {joins} GROUP BY {attrs}"
    sql = sql.format(attrs=attrs, joins=joins)
    create_table(sql, name)

def _select_most_sensitive_tuple(reln: Relation, node: Node):

    name    =   get_nodefreq_tablename(node)
    parent  =   node.parent
    children    =   node.children

    join_relations_attrs = [((cohort, 'cohort'), cohort.attributes) for cohort in node.get_cohorts(reln)]
    dprint(join_relations_attrs)

    if node.isRoot():
        join_relations_attrs  +=   [((child, 'bot'), common_attributes([child, node])) for child in children]
    elif node.isLeaf():
        join_relations_attrs  +=   [((node, 'top'), common_attributes([parent, node]))]
    else:
        join_relations_attrs  +=   [((child, 'bot'), common_attributes([child, node])) for child in children]
        join_relations_attrs  +=   [((node, 'top'), common_attributes([parent, node]))]

    join_clusters = get_joinclusters(join_relations_attrs)
    tstar_candidates = []
    for join_cluster, join_attributes in join_clusters:
        print('clsuter:', join_cluster)
        attrs = gen_sqlstr_attributes(join_attributes)
        join_relations = [join_relation for join_relation, join_attributes in join_cluster]
        join_names = get_joinnames(join_relations)
        joins = gen_sqlstr_joins(join_names)
        freq_str = gen_sqlstr_freqmulti(node, join_relations)
        if freq_str != '':
            sql = 'SELECT {attrs}, SUM({freq}) AS tsens FROM {joins} GROUP BY {attrs} ORDER BY tsens DESC LIMIT 1'
            sql = sql.format(attrs=attrs, freq=freq_str, joins=joins)
        else:
            sql = 'SELECT {attrs}, COUNT(*) AS tsens FROM {joins} GROUP BY {attrs} ORDER BY tsens DESC LIMIT 1'
            sql = sql.format(attrs=attrs, joins=joins)
        dprint(reln, 'partial sens')
        dprint(' '*4, sql)
        dprint()
        cur = run_sql(sql)
        res = cur.fetchone()
        tstar_candidate = decompose_tsens_row(res)
        tstar_candidates.append(tstar_candidate)

    tstar = merge_tstar_candidates(tstar_candidates)
    return tstar

def prepare_botjoin(hypertree: Tree):
    for node in hypertree.nodes:
        _prepare_botjoin(node)

def prepare_topjoin(hypertree: Tree):
    for node in hypertree.nodes:
        _prepare_topjoin(node)

def prepare_freqtable(hypertree: Tree):
    for node in hypertree.nodes:
        _prepare_freqtable(node)

def prepare_node(node: Node):
    name = node.name
    join_relations = [rel.rename() for rel in node.relations]
    joins   =   gen_sqlstr_joins(join_relations)

    sql = "SELECT * FROM {joins}"
    sql = sql.format(joins=joins)
    create_table(sql, name)

def prepare_tree(hypertree: Tree):
    for node in hypertree.nodes:
        prepare_node(node)

def prepare_tuplesens(hypertree: Tree):
    for reln, node in hypertree.node_map.items():
        _prepare_tuplesens(reln, node)

def select_most_sensitive_tuple(hypertree: Tree):
    tsens_list = []
    for reln, node in hypertree.node_map.items():
        tupl, sens = _select_most_sensitive_tuple(reln, node)
        if DEBUG:
            print_tuplesens(reln, tupl, sens)
        tsens_list.append((reln, tupl, sens))
    tsens_list.sort(key=lambda x:x[2])
    return tsens_list[-1], tsens_list[::-1]

def decompose_tsens_row(res):
    if res is None:
        sens = 0
        tupl = [('any', 'any')]
    else:
        sens = int(res['tsens'])
        tupl = [(k, v) for k,v in res.items() if k != 'tsens']
    return tupl, sens

def get_joinclusters(join_relations_attrs):
    join_clusters = []
    while join_relations_attrs:
        join_cluster = [join_relations_attrs[0]]
        del join_relations_attrs[0]
        candidate_attributes = copy(join_cluster[0][1])
        i = 0
        while i < len(join_relations_attrs):
            _, join_attributes = join_relations_attrs[i]
            if candidate_attributes & join_attributes:
                join_cluster.append(join_relations_attrs[i])
                candidate_attributes |= join_attributes
                del join_relations_attrs[i]
            else:
                i += 1
        join_cluster = (join_cluster, candidate_attributes)
        join_clusters.append(join_cluster)
    return join_clusters

def get_joinnames(join_relations: List[Tuple[Node, str]]):
    join_names = []
    for node, flag in join_relations:
        if flag == 'cohort':
            join_name = node.rename()
        elif flag == 'top':
            join_name = get_topjoin_tablename(node)
        elif flag == 'bot':
            join_name = get_botjoin_tablename(node)
        join_names.append(join_name)
    return join_names

def merge_tstar_candidates(tstar_candidates):
    tstar = ([], 1)
    for tstar_candidate in tstar_candidates:
        tupl, sens = tstar_candidate
        tstar = (tstar[0] + [tupl], tstar[1] * sens)
    return tstar

def create_table(sql, name):
    if DEBUG:
        print(name)
        print('    ' + sql)
        print()
    if RECREATE_TABLE:
        sml = "DROP TABLE IF EXISTS {name}; CREATE TABLE IF NOT EXISTS {name} AS ({sql})"
    else:
        sml = "CREATE TABLE IF NOT EXISTS {name} AS ({sql})"
    sml = sml.format(sql=sql, name=name)
    run_sql(sml)

def run_sql(sql):
    global conn
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(sql)

    conn.commit()
    return cur

def get_botjoin_tablename(node):
    return "BOTJOIN_" + node.name

def get_topjoin_tablename(node):
    return "TOPJOIN_" + node.name

def get_nodefreq_tablename(node):
    return "NODEFREQ_" + node.name

def get_relnsens_tablename(reln):
    return "RELNSENS_" + reln.name

def get_grouped_nodefreq_tablename(node, reln):
    return "GROUPED_NODEFREQ_" + node.name + "_" +reln.name

def groud_truth(relations: List[Relation], reln, tupl):
    single_tuple_reln = "(SELECT %s) AS %s"%(', '.join("'%s' AS %s"%(v, k) for k,v in tupl), reln.name)
    join_relations = [single_tuple_reln] + [other.name for other in relations if other != reln]
    joins   =   gen_sqlstr_joins(join_relations)

    sql = "SELECT COUNT(*) FROM {joins}"
    sql = sql.format(joins=joins)
    cur = run_sql(sql)
    res = cur.fetchone()
    if DEBUG:
        print(sql)
        print(res)
    return int(res[0])

def test_groud(relations):
    for reln in relations:
        name = get_relnsens_tablename(reln)
        sql = "SELECT * FROM %s"%name
        cur = run_sql(sql)
        for res in cur.fetchall():
            tupl, sens = decompose_tsens_row(res)
            gans = groud_truth(relations, reln, tupl)
            if DEBUG:
                print_tuplesens(reln, tupl, sens)
            assert(sens == gans)

def run_algo(T: Tree, _conn):
    global conn
    conn = _conn

    time_start = time.time()

    prepare_tree(T)
    prepare_botjoin(T)
    prepare_topjoin(T)
    #prepare_freqtable(T)
    #prepare_tuplesens(T)
    tstar, local_tstar_list = select_most_sensitive_tuple(T)

    time_finsh = time.time()
    elapsed = time_finsh - time_start

    return tstar, local_tstar_list, elapsed

def use_db(arch: str, scale: str):
    global conn
    dbname = arch + "_" + scale
    conn = pg2.connect(dbname=dbname, user='yuchao', password='')

def dprint(*args, **argv):
    if DEBUG:
        print(*args, **argv)

def gen_arch_hw2td3():
    R1 = _Relation(1, 'R1', ['A', 'B'])
    R2 = _Relation(2, 'R2', ['B', 'C'])
    R3 = _Relation(3, 'R3', ['C'])
    R4 = _Relation(4, 'R4', ['A'])
    R5 = _Relation(5, 'R5', ['B'])
    R6 = _Relation(6, 'R6', ['B'])
    relations = [R1, R2, R3, R4, R5, R6]

    N1 = Node(1, 'N1', [R1, R6])
    N2 = Node(2, 'N2', [R2])
    N3 = Node(3, 'N3', [R3])
    N4 = Node(4, 'N4', [R4])
    N5 = Node(5, 'N5', [R5])
    nodes = [N1, N2, N3, N4, N5]

    N1.parent = N2
    N3.parent = N2
    N4.parent = N1
    N5.parent = N1

    N2.children = [N1, N3]
    N1.children = [N4, N5]

    T = Tree(nodes)
    return T, nodes, relations

def gen_relations():
    R1 = _Relation(1, 'R1', ['A'])
    R2 = _Relation(2, 'R2', ['A', 'C'])
    R3 = _Relation(3, 'R3', ['A', 'B'])
    R4 = _Relation(4, 'R4', ['B', 'C'])
    R5 = _Relation(5, 'R5', ['C', 'D'])
    R6 = _Relation(6, 'R6', ['A', 'B', 'D'])
    R7 = _Relation(7, 'R7', ['C'])
    R8 = _Relation(8, 'R8', ['A', 'D'])
    R9 = _Relation(9, 'R9', ['D'])
    relations = [R1, R2, R3, R4, R5, R6, R7, R8, R9]
    return relations

def gen_arch_hw1td2():
    relations = gen_relations()
    R1, R2, R3, R4, R5, R6, R7, R8, R9 = relations
    relations = [R1, R3, R4, R5, R9]

    N1 = Node(1, 'N1', [R1])
    N2 = Node(2, 'N2', [R3])
    N3 = Node(3, 'N3', [R4])
    N4 = Node(4, 'N4', [R5])
    N5 = Node(5, 'N5', [R9])
    nodes = [N1, N2, N3, N4, N5]

    N2.parent = N1
    N3.parent = N2
    N4.parent = N3
    N5.parent = N4

    N1.children = [N2]
    N2.children = [N3]
    N3.children = [N4]
    N4.children = [N5]

    T = Tree(nodes)
    return T, nodes, relations

def gen_arch_hw1td3():
    relations = gen_relations()
    R1, R2, R3, R4, R5, R6, R7, R8, R9 = relations
    relations = [R1, R3, R4, R7, R5, R9]

    N1 = Node(1, 'N1', [R1])
    N2 = Node(2, 'N2', [R3])
    N3 = Node(3, 'N3', [R4])
    N4 = Node(4, 'N4', [R7])
    N5 = Node(5, 'N5', [R5])
    N6 = Node(6, 'N6', [R9])
    nodes = [N1, N2, N3, N4, N5, N6]

    N1.parent = N2
    N3.parent = N2
    N4.parent = N3
    N5.parent = N3
    N6.parent = N5

    N2.children = [N1, N3]
    N3.children = [N4, N5]
    N5.children = [N6]

    T = Tree(nodes)
    return T, nodes, relations

def gen_arch_hw2td2():
    relations = gen_relations()
    R1, R2, R3, R4, R5, R6, R7, R8, R9 = relations
    relations = [R1, R3, R4, R5, R6, R8]

    N1 = Node(1, 'N1', [R1])
    N2 = Node(2, 'N2', [R3, R4])
    N3 = Node(3, 'N3', [R5, R6])
    N4 = Node(4, 'N4', [R8])
    nodes = [N1, N2, N3, N4]

    N1.parent = N2
    N3.parent = N2
    N4.parent = N3

    N2.children = [N1, N3]
    N3.children = [N4]

    T = Tree(nodes)
    return T, nodes, relations

def gen_arch_hw3td3():
    relations = gen_relations()
    R1, R2, R3, R4, R5, R6, R7, R8, R9 = relations
    relations = [R1, R2, R3, R4, R5, R6, R7, R8]

    N1 = Node(1, 'N1', [R1])
    N2 = Node(2, 'N2', [R2, R3, R4])
    N3 = Node(3, 'N3', [R5, R6])
    N4 = Node(4, 'N4', [R7])
    N5 = Node(5, 'N5', [R8])
    nodes = [N1, N2, N3, N4, N5]

    N1.parent = N2
    N3.parent = N2
    N4.parent = N3
    N5.parent = N3


    N2.children = [N1, N3]
    N3.children = [N4, N5]

    T = Tree(nodes)
    return T, nodes, relations

def gen_report(arch, scale, query, tstar, local_tstar_list, elapsed, test_pass):
    reln, tupl, sens = tstar
    elapsed = '%.3f'%elapsed
    print(arch, scale, query, elapsed, reln, tupl, sens, local_tstar_list, test_pass, sep=' | ')

def gen_report_title():
    print('arch', 'scale', 'query', 'time', 'relation', 'tuple', 'sensitivity', 'all_table_tstar' ,'test pass', sep=' | ')

def print_tuplesens(reln, tupl, sens):
    print('- Relation: ', reln)
    print('- Tuple: ', tupl)
    print('- sensitivity: ', sens)
    print()

def print_humanreadable_report(arch, scale, query, tstar, local_tstar_list, elapsed, test_pass):
    print('arch-%s-scale-%s'%(arch, scale))
    print('query: ', query)
    print('Time Elapsed: %.3f'%(elapsed))
    print('-'*20)
    print("The most sensitive tuple is")
    print_tuplesens(*tstar)
    print("The most sensitive tuple for each table")
    print('\n'.join(str(local_tstar) for local_tstar in local_tstar_list))
    print("- Test Result: ", test_pass)
    print()

def _test(arch, scale):
    global conn
    use_db(arch, scale)
    T, nodes, relations = globals()['gen_arch_'+arch]()

    tstar, local_tstar_list, elapsed = run_algo(T, conn)

    test_pass = 'Unknown'
    if scale == 'small' or scale == 'toy':
        try:
            test_groud(relations)
        except:
            test_pass = 'Failed'
        else:
            test_pass = 'Succeeded'

    print_humanreadable_report(arch, scale, arch, tstar, local_tstar_list, elapsed, test_pass)
    #gen_report(arch, scale, arch, tstar, local_tstar_list, elapsed, test_pass)

def test_toy():
    arch = 'hw2td3'
    scale = 'toy'
    _test(arch, scale)

def test():
    gen_report_title()
    for arch in ['hw1td2', 'hw1td3', 'hw2td2', 'hw3td3']:
         for scale in ['small', 'medium', 'large']:
             _test(arch, scale)

if __name__ == '__main__':
    test()
    #test_toy()
