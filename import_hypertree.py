#!/usr/bin/python3

from copy import deepcopy as copy

from objects import Attribute, Relation, Node, Tree
from objects import BotNode, TopNode, DNode, DForest
from utils import dprint

def read_hypertree_from_file(hypertree_name, hypertree_file):
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
            dnodes = {}
            dforest = DForest()
            relations[index].dforest = dforest
        elif flag == 'dn':
            dnode = dnode_parser(info, relations, nodes)
            dnodes[dnode.index] = dnode
            dforest.dnodes.append(dnode)
        elif flag == 'de':
            parent = dnodes[info[1]]
            child = dnodes[info[2]]
            parent.children.append(child)
            child.parent = parent
    relations = relations.values()
    nodes = nodes.values()
    T = Tree(hypertree_name, nodes)
    return T, nodes, relations

def dnode_parser(info, relations, nodes):
    index = info[1]
    name = info[2]
    rlnds = [dparse(vertex, relations, nodes) for vertex in info[3:]]
    return DNode(index, name, rlnds)

def dparse(vertex, relations, nodes):
    flag = vertex[0]
    index = vertex[1:]
    if flag == 'c':
        return relations[index]
    elif flag == 't':
        return TopNode(nodes[index])
    elif flag == 'b':
        return BotNode(nodes[index])
    else:
        raise Exception('Unknown Flag: %s'%flag)
