#!/usr/bin/python3

import psycopg2 as pg2
import psycopg2.extras
import psycopg2.sql
import time
from typing import List, Set, Tuple
from copy import copy
from deprecated import deprecated

SQL = psycopg2.sql.SQL
LIT = psycopg2.sql.Literal
IDN = psycopg2.sql.Identifier

DEBUG = True
#DEBUG = False

RECREATE_TABLE = True
#RECREATE_TABLE = False

class Infix:
    def __init__(self, function):
        self.function = function
    def __ror__(self, other):
        return Infix(lambda x, self=self, other=other: self.function(other, x))
    def __or__(self, other):
        return self.function(other)
    def __rlshift__(self, other):
        return Infix(lambda x, self=self, other=other: self.function(other, x))
    def __rshift__(self, other):
        return self.function(other)
    def __call__(self, value1, value2):
        return self.function(value1, value2)

INS = Infix(lambda x,y: isinstance(x, y))

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

        self.dtree = None

    def rename(self):
        new_attrs = ','.join('%s AS %s'%(attr.orig_name, attr.join_name) for attr in self.attributes)
        new_reln  = '(SELECT {new_attrs} FROM {reln}) AS {reln}'.format(new_attrs=new_attrs, reln=self.name)
        return new_reln

    def gen_sqlstr(self):
        new_attrs = ','.join('%s AS %s'%(attr.orig_name, attr.join_name) for attr in self.attributes)
        new_reln  = '(SELECT {new_attrs}, 1 as C_{i} FROM {reln}) AS {reln}'.format(new_attrs=new_attrs, i=self.index, reln=self.name)
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

@deprecated
def deprecated_select_most_sensitive_tuple(reln: Relation, node: Node):

    name    =   get_nodefreq_tablename(node)
    parent  =   node.parent
    children    =   node.children

    join_relations_attrs = [((cohort, 'cohort'), cohort.attributes) for cohort in node.get_cohorts(reln)]
    dprint('[SEL TSENS] ', 'join_rels_attrs', join_relations_attrs)

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
        dprint('clsuter:', join_cluster)
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

class BotjoinING:
    pass

def prepare_dbotjoin(reln: Relation, dnode: DNode, view_queue: List[Tuple[str, str]], ptstar_candidates):
    if dnode.dbotjoin is not None or dnode.dbotjoin |INS| BotjoinING:
        return

    dnode.dbotjoin = BotjoinING()
    dprint('dnode: ', dnode, ', children', dnode.children)
    for dchild in dnode.children:
        prepare_dbotjoin(reln, dchild, view_queue, ptstar_candidates)

    dnode.update_project_attrs(reln)
    attrs = dnode.project_attrs
    dprint('dnode.project_attrs: ', attrs)
    attrs = gen_sqlstr_attributes(attrs)
    joins = [dnode.gen_sqlstr()] + [get_dbotjoin_tablename(dchild) for dchild in dnode.children]
    joins = gen_sqlstr_joins(joins)
    freqs = gen_sqlstr_freqmulti(dnode)
    name = get_dbotjoin_tablename(dnode)

    if not dnode.isRoot():
        sql = 'SELECT {attrs}, SUM({freqs}) AS C_{i}_{pi} FROM {joins} GROUP BY {attrs}'
        sql = sql.format(attrs=attrs, freqs=freqs, joins=joins, i=dnode.index, pi=dnode.parent.index)
        view_queue.append((sql, name))
        dnode.dbotjoin = name
        #dprint(name, dnode.parent, [name for sql, name in view_queue])
        prepare_dbotjoin(reln, dnode.parent, view_queue, ptstar_candidates)
    else:
        dprint('DNode Attrs: ', dnode.attributes, 'Reln Attrs: ', reln.attributes)
        if view_queue:
            sql = 'WITH \n' + ', \n'.join('    {name} AS ({view})'.format(view=view, name=name) for view, name in view_queue) + '\n'
        else:
            sql = ''
        sql = sql + '    SELECT {attrs}, SUM({freqs}) AS ptsens FROM {joins} GROUP BY {attrs} ORDER BY ptsens DESC LIMIT 1'
        sql = sql.format(attrs=attrs, freqs=freqs, joins=joins)
        dprint(name)
        dprint([name for sql, name in view_queue])
        dprint(' '*4 +  sql)
        dprint()
        cur = run_sql(sql)
        res = cur.fetchone()
        ptstar_candidate = decompose_ptsens_row(res)
        ptstar_candidates.append(ptstar_candidate)
        dnode.dbotjoin = name

def learn_ptstar_candidates(reln: Relation):
    ptstar_candidates = []
    for dnode in reln.dforest.dnodes:
        view_queue = []
        prepare_dbotjoin(reln, dnode, view_queue, ptstar_candidates)
    return ptstar_candidates

def select_ltstar(reln: Relation, node: Node):
    dprint('Relation: ', reln, ', DForest', reln.dforest)
    ptstar_candidates = learn_ptstar_candidates(reln)
    ltstar = merge_ptstar_candidates(ptstar_candidates)
    return ltstar

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
    ltstars = []
    for reln, node in hypertree.node_map.items():
        tupl, sens = select_ltstar(reln, node)
        if DEBUG:
            print_tuplesens(reln, tupl, sens)
        ltstars.append((reln, tupl, sens))
    ltstars.sort(key=lambda x:x[2], reverse=True)
    tstar = ltstars[0]
    return tstar, ltstars

def decompose_tsens_row(res):
    if res is None:
        sens = 0
        tupl = [('any', 'any')]
    else:
        sens = int(res['tsens'])
        tupl = [(k, v) for k,v in res.items() if k != 'tsens']
    return tupl, sens

def decompose_ptsens_row(res):
    if res is None:
        sens = 0
        tupl = [('any', 'any')]
    else:
        sens = int(res['ptsens'])
        tupl = [(k, v) for k,v in res.items() if k != 'ptsens']
    return tupl, sens

def merge_ptstar_candidates(ptstar_candidates):
    ptstar = ([], 1)
    for ptstar_candidate in ptstar_candidates:
        tupl, sens = ptstar_candidate
        ptstar = (ptstar[0] + tupl, ptstar[1] * sens)
    return ptstar

@deprecated
def get_joinclusters(join_relations_attrs):
    join_clusters = []
    while join_relations_attrs:
        join_cluster = [join_relations_attrs[0]]
        del join_relations_attrs[0]
        candidate_attributes = copy(join_cluster[0][1])
        no_change = False
        while not no_change:
            no_change = True
            i = 0
            while i < len(join_relations_attrs):
                _, join_attributes = join_relations_attrs[i]
                if candidate_attributes & join_attributes:
                    join_cluster.append(join_relations_attrs[i])
                    candidate_attributes |= join_attributes
                    del join_relations_attrs[i]
                    no_change = False
                else:
                    i += 1
        join_cluster = (join_cluster, candidate_attributes)
        join_clusters.append(join_cluster)
    return join_clusters

@deprecated
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

def create_table(sql, name):
    dprint(name)
    dprint('    ' + sql)
    dprint()
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

def get_dbotjoin_tablename(dnode):
    return "DBOTJOIN_" + dnode.name

def get_grouped_nodefreq_tablename(node, reln):
    return "GROUPED_NODEFREQ_" + node.name + "_" +reln.name

def ground_truth(relations: List[Relation], reln, tupl):
    sql = SQL('(SELECT {0}) AS {1}').format(
            SQL(', ').join(
                SQL('{0} AS {1}').format(
                    LIT(v), IDN(k)
                ) for k,v in tupl
            ),
            IDN(reln.name))
    single_tuple_reln = sql.as_string(conn)

    join_relations = [single_tuple_reln] + [other.rename() for other in relations if other != reln]
    joins   =   gen_sqlstr_joins(join_relations)

    sql = "SELECT COUNT(*) FROM {joins}"
    sql = sql.format(joins=joins)
    dprint('[GROUND TRUTH]', 'SQL: ', sql)
    cur = run_sql(sql)
    res = cur.fetchone()
    res = int(res[0])
    dprint('[GROUND TRUTH]', 'SENS: ', res)
    return res

def test_ground(relations, tstars=None):
    try:
        if tstars:
            for tstar in tstars:
                reln, tupl, sens = tstar
                dprint('[TEST GROUND]', 'tstar', tstar)
                gans = ground_truth(relations, reln, tupl)
                dprint()
                assert(sens == gans)
        else:
            for reln in relations:
                name = get_relnsens_tablename(reln)
                sql = "SELECT * FROM %s"%name
                cur = run_sql(sql)
                for res in cur.fetchall():
                    tupl, sens = decompose_tsens_row(res)
                    gans = ground_truth(relations, reln, tupl)
                    if DEBUG:
                        print_tuplesens(reln, tupl, sens)
                    assert(sens == gans)
    except:
        raise Exception(tstar, 'Sens: %d'%sens, 'Gans: %d'%gans)

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
    if scale in ['small', 'toy'] or (arch == 'tpch' and scale in ['0.0001', '0.01']):
        try:
            test_ground(relations)
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
