#!/usr/bin/python3

from algo import Attribute, Relation, Node, BotNode, TopNode, Tree

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
        elif flag == 'd':
            index = info[1]
            relation = relations[index]
        elif flag == 'dn':
            dnode_name = info[1]
            vertex_type = info[]
    relations = relations.values()
    nodes = nodes.values()
    T = Tree(nodes)
    return T, nodes, relations

def dnode_parser(info, relations, nodes):
    dnode_name = info[1]
    rlnds = [dparse(vertex, relations, nodes) for vertex in info[2:]]

def dparse(vertex, relations, nodes):
    flag = vertex[0]
    index = vertex[1:]
    if flag == 'c':
        return relations[index]
    elif flag == 't':
        return TopNode(nodes[index])
    elif flag == 'b':
        return BotNode(nodes[index])
