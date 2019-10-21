from typing import List, Set, Tuple
from copy import copy
from deprecated import deprecated

from utils import INS

class Attribute:
    def __init__(self, index, join_name, orig_name):
        self.index = index
        self.join_name = join_name
        self.orig_name = orig_name

        self.mf = None
        self.actual_mf = None

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

        self.dforest = None
        self.sens = None
        self.size = None

        self.foreignkey_sources = []
        self.primarykey_targets = []

    def rename(self):
        new_reln = '({rename_attributes}) AS {reln}'.format(rename_attributes=self.rename_attributes(), reln=self.name)
        return new_reln

    def rename_attributes(self):
        new_attrs = ','.join('%s AS %s'%(attr.orig_name, attr.join_name) for attr in self.attributes)
        new_reln  = 'SELECT {new_attrs} FROM {reln}'.format(new_attrs=new_attrs, reln=self.name)
        return new_reln

    def gen_sqlstr(self):
        new_attrs = ','.join('%s AS %s'%(attr.orig_name, attr.join_name) for attr in self.attributes)
        new_reln  = '(SELECT {new_attrs}, 1 as C_{i} FROM {reln}) AS {reln}'.format(new_attrs=new_attrs, i=self.index, reln=self.name)
        return new_reln

    def __eq__(self, other):
        return other and self.name == other.name

    def __hash__(self):
        return hash(self.name)

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.__str__()

class Schema:
    def __init__(self, name, relations):
        self.name = name
        self.relations = relations

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

        self.sensreln = None
        self.botsensreln = None

    def get_cohorts(self, relation):
        return [rel for rel in self.relations if rel != relation]

    def isRoot(self):
        return self.parent is None

    def isLeaf(self):
        return len(self.children) == 0

    @property
    def project_attrs(self):
        if self.parent is not None:
            attrs = common_attributes([self, self.parent])
        else:
            attrs = self.attributes
        return attrs

    def __str__(self):
        return self.name

    __repr__ = __str__

class Tree:
    def __init__(self, name, nodes: List[Node]):
        self.nodes = nodes
        self.node_map = {}
        self.demap_nodes(nodes)

        self.name = name

    def demap_nodes(self, nodes):
        for node in nodes:
            self.demap_node(node)

    def demap_node(self, node):
        for relation in node.relations:
            self.node_map[relation] = node

class TupleSens:
    def __init__(self, reln, attr, sens):
        self.reln = reln
        self.attr = attr
        self.sens = sens

    def asTuple(self):
        return (self.reln, self.attr, self.sens)

    def __str__(self):
        return str(self.asTuple())

    __repr__ = __str__

class ElasticTupleSens:
    def __init__(self, reln, attr, e_sens, t_sens):
        self.reln = reln
        self.attr = attr
        self.e_sens = e_sens
        self.t_sens = t_sens

    def asTuple(self):
        return (self.reln, self.attr, self.e_sens, self.t_sens)

    def __str__(self):
        return str(self.asTuple())

    __repr__ = __str__

def max_ltstars(ltstars):
    ltstars.sort(key=lambda tsens: tsens.sens, reverse=True)
    tstar = ltstars[0]
    return tstar, ltstars

class DNode:
    def __init__(self, index, name, rlnds):
        self.index = index
        self.name = name
        self.rlnds = rlnds

        self.parent = None
        self.children = []
        self.attributes = self.union_rlnd_attributes(rlnds)
        self.project_attrs = None

        self.dbotjoin = None

    def union_rlnd_attributes(self, rlnds):
        attributes = set()
        for rlnd in rlnds:
            if rlnd |INS| Relation:
                next_attributes = rlnd.attributes
            elif rlnd |INS| Node:
                next_attributes = rlnd.attributes
            else:
                print(type(rlnd))
                raise Exception
            attributes |= next_attributes
        return attributes

    def update_project_attrs(self, reln):
        attrs = (self.attributes) | set().union(*[child.project_attrs for child in self.children])
        parent_attrs = self.parent.attributes if self.parent else set()
        self.project_attrs = attrs & (parent_attrs | reln.attributes)

    def isRoot(self):
        return self.parent is None

    def isLeaf(self):
        return len(self.children) == 0

    def gen_sqlstr(self):
        name = self.name
        attrs = gen_sqlstr_attributes(self.attributes)
        joins = [rlnd.gen_sqlstr() for rlnd in self.rlnds]
        joins = gen_sqlstr_joins(joins)
        freqs = gen_sqlstr_freqmulti(self.rlnds)
        if len(self.rlnds) > 1:
            sql = "(SELECT {attrs}, SUM({freqs}) AS C_{i} FROM {joins} GROUP BY {attrs}) AS {name}"
        else:
            sql = "(SELECT {attrs}, {freqs} AS C_{i} FROM {joins}) AS {name}"
        sql = sql.format(attrs=attrs, freqs=freqs, joins=joins, name=name, i=self.index)
        return sql

    def __str__(self):
        return self.name

    __repr__ = __str__

class BotNode(Node):
    def __init__(self, node):
        self.node = node

    @property
    def index(self):
        return self.node.index

    @property
    def parent(self):
        return self.node.parent

    @property
    def attributes(self):
        return self.node.project_attrs

    @property
    def name(self):
        return '<%s>%s'%('bot', self.node.name)

    def gen_sqlstr(self):
        return get_botjoin_tablename(self.node)

class TopNode(Node):
    def __init__(self, node):
        self.node = node

    @property
    def index(self):
        return self.node.index

    @property
    def parent(self):
        return self.node.parent

    @property
    def attributes(self):
        return self.node.project_attrs

    @property
    def name(self):
        return '<%s>%s'%('top', self.node.name)

    def gen_sqlstr(self):
        return get_topjoin_tablename(self.node)

class DForest:
    def __init__(self, dnodes: List[DNode] = None):
        if dnodes:
            self.dnodes = dnodes
        else:
            self.dnodes = []

    def __str__(self):
        return str(self.dnodes)

    __repr__ = __str__

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
    #sql = gen_sqlstr_padding(sql)
    return sql

def gen_sqlstr_neighmutli(node: Node):
    sql = " * ".join(["C_{i}_{pi}".format(i=neigh.index, pi=node.parent.index) for neigh in get_neighbours(node)])
    #sql = gen_sqlstr_padding(sql)
    return sql

@deprecated
def deprecated_gen_sqlstr_freqmulti(center_node:Node, node_flag_list: List[Tuple[Node, str]]):
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

def gen_sqlstr_freqmulti(rlnds_or_dnode):
    freq_strs = []
    if rlnds_or_dnode |INS| DNode:
        dnode = rlnds_or_dnode
        freq_strs.append('C_{i}'.format(i=dnode.index))
        freq_strs += ['C_{i}_{pi}'.format(i=dchild.index, pi=dnode.index) for dchild in dnode.children]
    else:
        rlnds = rlnds_or_dnode
        for rlnd in rlnds:
            if rlnd |INS| Relation:
                freq_str = 'C_{i}'.format(i=rlnd.index)
            elif rlnd |INS| Node:
                freq_str = 'C_{i}_{pi}'.format(i=rlnd.index, pi=rlnd.parent.index)
            freq_strs.append(freq_str)
    return ' * '.join(freq_strs)

def gen_sqlstr_attributes(attributes):
    sql = ", ".join(map(str, attributes))
    #sql = gen_sqlstr_padding(sql)
    return sql

def gen_sqlstr_joins(node_names):
    sql = " NATURAL JOIN ".join(node_names)
    #sql = gen_sqlstr_padding(sql)
    return sql

def get_botjoin_tablename(node):
    return "BOTJOIN_" + node.name

def get_topjoin_tablename(node):
    return "TOPJOIN_" + node.name

def get_nodefreq_tablename(node):
    return "NODEFREQ_" + node.name

def get_relnsens_tablename(reln):
    return "RELNSENS_" + reln.name

def get_dbotjoin_tablename(dnode):
    return "DBOTJOIN_" + dnode.name

def get_grouped_nodefreq_tablename(node, reln):
    return "GROUPED_NODEFREQ_" + node.name + "_" +reln.name
