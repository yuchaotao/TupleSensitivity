#!/usr/bin/python3

from algo import Attribute, Relation, Node, Tree

def read_hypertree_from_file(hypertree_file):
    relations = {}
    nodes = {}
    for line in open(hypertree_file, 'r').readlines():
        info = line.strip().split(' ')
        flag = info[0]
        if flag == 'c':
            pass
        elif flag == 'r':
            index   = info[1]
            name    = info[2]
            relation    = Relation(index, name, set())
            relations[index]    = relation
        elif flag == 'a':
            index = info[1]
            join_name = info[2]
            orig_name = info[3]
            attribute = Attribute(index, join_name, orig_name)
            relation.attributes.add(attribute)
        elif flag == 'n':
            index = info[1]
            name  = info[2]
            relns = [relations[index] for index in info[3:]]
            node = Node(index, name, relns)
            nodes[index] = node
        elif flag == 'e':
            parent = nodes[info[1]]
            child  = nodes[info[2]]
            parent.children.append(child)
            child.parent = parent
    relations = relations.values()
    nodes = nodes.values()
    T = Tree(nodes)
    return T, nodes, relations
