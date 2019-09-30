#!/usr/bin/python3

from typing import List

class Relation:
    def __init__(self, index, name, attributes: List[str]):
        self.index = index
        self.name = name
        self.attributes = set(attributes)

class Node:
    def __init__(self, index, name, relations: List[Relation]):
        self.index = index
        self.name = name
        self.relations = relations

        self.parent = None
        self.children = None
        self.botjoin = None
        self.topjoin = None
        self.attributes = union_attributes(relations)

    def isRoot(self):
        return self.parent is None

    def isLeaf(self):
        return self.children is None

class Tree:
    def __init__(self, nodes: List[Node]):
        self.nodes = nodes

def union_attributes(relation_list: List[Relation]):
    attribute_set = set(relation_list[0].attributes)
    for relation in relation_list:
        attribute_set |= relation.attributes
    return attribute_set

def common_attributes(node_list: List[Node]) -> List[str]:
    attribute_set = set(node_list[0].attributes)
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

def gen_sqlstr_attributes(attributes):
    sql = ", ".join(attributes)
    sql = gen_sqlstr_padding(sql)
    return sql

def gen_sqlstr_joins(node_names):
    sql = " NATURAL JOIN ".join(node_names)
    sql = gen_sqlstr_padding(sql)
    return sql

def get_botjoin_tablename(node):
    return "BOTJOIN_" + node.name

def get_topjoin_tablename(node):
    return "TOPJOIN_" + node.name

def prepare_node(node: Node):
    name = node.name
    join_relations = [rel.name for rel in node.relations]
    joins   =   gen_sqlstr_joins(join_relations)

    sql = "SELECT * FROM {joins}"
    sql = sql.format(joins=joins)
    create_table(sql, name)

def prepare_tree(hypertree: Tree):
    for node in hypertree.nodes:
        prepare_node(node)

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

        sql = "SELECT {attrs}, SUM({childmulti}) AS C_{i}_{pi} FROM {joins}"
        sql = sql.format(attrs=attrs, i=node.index, pi=parent.index, childmulti=childmulti, joins=joins)
        create_table(sql, name)

    node.botjoin = name
    return node

def prepare_botjoin(hypertree: Tree):
    for node in hypertree.nodes:
        _prepare_botjoin(node)

def _prepare_topjoin(node: Node):
    if node.topjoin is not None:
        return node
    name    =   get_topjoin_tablename(node)
    parent  =   node.parent
    children    =   node.children

    if node.isRoot():
        return node
    elif node.parent.isRoot():
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

        sql = "SELECT {attrs}, SUM(C_{pi}_{ppi} * {neighmulti}) AS C_{i}_{pi} FROM {joins} GROUP BY {attrs}"
        sql = sql.format(attrs=attrs, i=node.index, pi=parent.index, ppi=parent.parent.index, neighmulti=neighmulti, joins=joins)

    create_table(sql, name)
    node.topjoin = name
    return node

def prepare_topjoin(hypertree: Tree):
    for node in hypertree.nodes:
        _prepare_topjoin(node)

def create_table(sql, name):
    sml = "CREATE TABLE IF NOT EXISTS {name} AS ({sql})"
    sml = sml.format(sql=sql, name=name)
    run_sql(sml)

def run_sql(sql):
    print(sql)

def test():
    R1 = Relation(1, 'R1', ['A', 'B'])
    R2 = Relation(2, 'R2', ['B', 'C'])
    R3 = Relation(3, 'R3', ['C', 'D'])
    R4 = Relation(4, 'R4', ['A'])
    R5 = Relation(5, 'R5', ['B'])
    R6 = Relation(6, 'R6', ['B'])

    N1 = Node(1, 'N1', [R1, R6])
    N2 = Node(2, 'N2', [R2])
    N3 = Node(3, 'N3', [R3])
    N4 = Node(4, 'N4', [R4])
    N5 = Node(5, 'N5', [R5])

    N1.parent = N2
    N3.parent = N2
    N4.parent = N1
    N5.parent = N1

    N2.children = [N1, N3]
    N1.children = [N4, N5]

    T = Tree([N1, N2, N3, N4, N5])

    prepare_tree(T)
    prepare_botjoin(T)
    prepare_topjoin(T)

if __name__ == '__main__':
    test()
