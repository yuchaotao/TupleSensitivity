#!/usr/bin/python3

import psycopg2 as pg2
import psycopg2.extras
import psycopg2.sql
import time
from typing import List, Set, Tuple
from deprecated import deprecated

from objects import Attribute, Relation, Node, Tree
from objects import TupleSens
from objects import BotNode, TopNode, DNode, DForest
from objects import union_attributes, common_attributes
from objects import get_neighbours
from objects import gen_sqlstr_attributes, gen_sqlstr_childmutli, gen_sqlstr_joins, gen_sqlstr_neighmutli, gen_sqlstr_freqmulti, gen_sqlstr_padding
from objects import get_botjoin_tablename, get_topjoin_tablename, get_nodefreq_tablename, get_relnsens_tablename, get_dbotjoin_tablename, get_grouped_nodefreq_tablename
from objects import max_ltstars
from utils import INS, dprint, DEBUG, timeout, TimeoutError

import db

SQL = psycopg2.sql.SQL
LIT = psycopg2.sql.Literal
IDN = psycopg2.sql.Identifier

RECREATE_TABLE = True
#RECREATE_TABLE = False

def _prepare_botjoin(node: Node, calc_root=False):
    if node.botjoin is not None:
        return node

    name    =   get_botjoin_tablename(node)

    if node.isRoot():
        if calc_root:
            for child in node.children:
                _prepare_botjoin(child)

            childmulti  =   gen_sqlstr_childmutli(node)
            join_relations = [node.name] + [get_botjoin_tablename(child) for child in node.children]
            joins   =   gen_sqlstr_joins(join_relations)

            sql = "SELECT SUM({childmulti}) AS total_cnt FROM {joins}"
            sql = sql.format(childmulti=childmulti, joins=joins)
            create_table(sql, name)
        else:
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

def _prepare_freqtable(reln: Relation):
    for dnode in reln.dforest.dnodes:
        if dnode.isRoot():
            name, view_queue = dnode.dbotjoin
        else:
            continue
        attrs = dnode.project_attrs
        attrs = gen_sqlstr_attributes(attrs)
        joins = [dnode.gen_sqlstr()] + [get_dbotjoin_tablename(dchild) for dchild in dnode.children]
        joins = gen_sqlstr_joins(joins)
        freqs = gen_sqlstr_freqmulti(dnode)
        sql = 'SELECT {attrs}, SUM({freqs}) AS ptsens_{ri}_{dni} FROM {joins} GROUP BY {attrs}'
        sql = sql.format(attrs=attrs, freqs=freqs, joins=joins, ri=reln.index, dni=dnode.index)
        view_queue.append((sql, name))

def _prepare_tuplesens(reln: Relation):
    view_queue = []
    joins = [reln.rename()]
    freqs = []
    for dnode in reln.dforest.dnodes:
        if dnode.isRoot():
            name, part_view_queue = dnode.dbotjoin
        else:
            continue
        view_queue += part_view_queue
        joins.append(name)
        freqs.append('ptsens_{ri}_{dni}'.format(ri=reln.index, dni=dnode.index))

    freqs = ' * '.join(freqs)
    joins = gen_sqlstr_joins(joins)
    attrs = ', '.join('{join_name} AS {orig_name}'.format(join_name=attr.join_name, orig_name = attr.orig_name) for attr in reln.attributes)

    sql = 'WITH \n' + ', \n'.join('    {name} AS ({view})'.format(view=view, name=name) for view, name in view_queue) + '\n'
    sql += 'SELECT {attrs}, {freqs} as tsens FROM {joins}'.format(attrs=attrs, freqs=freqs, joins=joins)

    name = get_relnsens_tablename(reln)

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

def _prepare_dbotjoin(reln: Relation, dnode: DNode, view_queue: List[Tuple[str, str]]):
    if dnode.dbotjoin is not None or dnode.dbotjoin |INS| BotjoinING:
        return

    dnode.dbotjoin = BotjoinING()
    dprint('dnode: ', dnode, ', children', dnode.children)
    for dchild in dnode.children:
        _prepare_dbotjoin(reln, dchild, view_queue)

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
        dnode.dbotjoin = (name, view_queue)
        #dprint(name, dnode.parent, [name for sql, name in view_queue])
        _prepare_dbotjoin(reln, dnode.parent, view_queue)
    else:
        dprint('DNode Attrs: ', dnode.attributes, 'Reln Attrs: ', reln.attributes)
        dnode.dbotjoin = (name, view_queue)

def prepare_dbotjoin(hypertree: Tree):
    for reln in hypertree.node_map:
        for dnode in reln.dforest.dnodes:
            view_queue = []
            _prepare_dbotjoin(reln, dnode, view_queue)

def learn_ptstar_candidates(reln: Relation):
    ptstar_candidates = []
    for dnode in reln.dforest.dnodes:
        if dnode.isRoot():
            attrs = dnode.project_attrs
            attrs = gen_sqlstr_attributes(attrs)
            joins = [dnode.gen_sqlstr()] + [get_dbotjoin_tablename(dchild) for dchild in dnode.children]
            joins = gen_sqlstr_joins(joins)
            freqs = gen_sqlstr_freqmulti(dnode)
            _, view_queue = dnode.dbotjoin
            if view_queue:
                sql = 'WITH \n' + ', \n'.join('    {name} AS ({view})'.format(view=view, name=name) for view, name in view_queue) + '\n'
            else:
                sql = ''
            sql = sql + '    SELECT {attrs}, SUM({freqs}) AS ptsens FROM {joins} GROUP BY {attrs} ORDER BY ptsens DESC LIMIT 1'
            sql = sql.format(attrs=attrs, freqs=freqs, joins=joins)

            cur = run_sql(sql)
            res = cur.fetchone()
            ptstar_candidate = decompose_ptsens_row(res)
            ptstar_candidates.append(ptstar_candidate)
    return ptstar_candidates

def select_ltstar(reln: Relation, node: Node):
    dprint('Relation: ', reln, ', DForest', reln.dforest)
    ptstar_candidates = learn_ptstar_candidates(reln)
    tupl, sens = merge_ptstar_candidates(ptstar_candidates)
    ltstar = TupleSens(reln, tupl, sens)
    return ltstar

def dummy_ltstar(reln: Relation):
    ltstar = TupleSens(reln, [], 1)
    return ltstar

def prepare_botjoin(hypertree: Tree, calc_root=False):
    for node in hypertree.nodes:
        _prepare_botjoin(node, calc_root)

def prepare_topjoin(hypertree: Tree):
    for node in hypertree.nodes:
        _prepare_topjoin(node)

def prepare_node(node: Node):
    name = node.name
    if len(node.relations) == 1:
        reln = node.relations[0]
        sql = reln.rename_attributes()
        create_view(sql, name)
    else:
        join_relations = [reln.rename() for reln in node.relations]
        joins   =   gen_sqlstr_joins(join_relations)

        sql = "SELECT * FROM {joins}"
        sql = sql.format(joins=joins)
        create_table(sql, name)

def prepare_tree(hypertree: Tree):
    for node in hypertree.nodes:
        prepare_node(node)

def prepare_freqtable(hypertree: Tree):
    for reln, node in hypertree.node_map.items():
        _prepare_freqtable(reln)

def prepare_tuplesens(hypertree: Tree, exclusion):
    for reln, node in hypertree.node_map.items():
        if reln.name in exclusion:
            continue
        _prepare_tuplesens(reln)

def select_most_sensitive_tuple(hypertree: Tree, exclusion: List[str] = []):
    ltstars = []
    for reln, node in hypertree.node_map.items():
        if reln.name in exclusion:
            ltstar = dummy_ltstar(reln)
        else:
            ltstar = select_ltstar(reln, node)
        if DEBUG:
            print_tuplesens(ltstar)
        ltstars.append(ltstar)
    tstar, ltstars = max_ltstars(ltstars)
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
        drop_table_or_view(name)
    sml = "CREATE TABLE IF NOT EXISTS {name} AS ({sql})"
    sml = sml.format(sql=sql, name=name)
    run_sql(sml)

def create_view(sql, name):
    dprint(name)
    dprint('    ' + sql)
    dprint()
    if RECREATE_TABLE:
        drop_table_or_view(name)
    sml = "CREATE VIEW {name} AS ({sql})"
    sml = sml.format(sql=sql, name=name)
    run_sql_silent(sml)

def drop_table_or_view(name):
    sml = "DROP TABLE IF EXISTS {name};".format(name=name)
    run_sql_silent(sml)
    sml = "DROP VIEW IF EXISTS {name};".format(name=name)
    run_sql(sml)

def run_sql(sql):
    global conn
    return db.run_sql(sql, conn)

def run_sql_silent(sql):
    global conn
    return db.run_sql_silent(sql, conn)

def ground_truth(relations: List[Relation], reln, tupl, _conn=None):
    global conn
    if _conn:
        conn = _conn
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

def ground_query_count(relations, _conn=None):
    global conn
    if _conn:
        conn = _conn

    join_relations = [reln.rename() for reln in relations]
    joins   =   gen_sqlstr_joins(join_relations)

    sql = "SELECT COUNT(*) AS cnt FROM {joins}"
    sql = sql.format(joins=joins)
    cnt = int((run_sql(sql).fetchone())[0])

    dprint('[GROUND QUERY TRUTH]', 'SQL: ', sql)
    dprint('[GROUND QUERY TRUTH]', 'CNT: ', cnt)

    return cnt

class QueryCntError(Exception):
    pass

@timeout(3)
def test_query_ground(relations, my_cnt):
    try:
        true_cnt = ground_query_count(relations)
        assert(true_cnt == my_cnt)
    except TimeoutError:
        raise TimeoutError
    except AssertionError:
        dprint(true_cnt)
        dprint(my_cnt)
        raise QueryCntError('My Cnt: %d'%my_cnt, 'True Cnt: %d'%true_cnt)

class TStarError(Exception):
    pass

@timeout(3)
def test_ground(relations, tstars=None):
    try:
        if tstars:
            for tstar in tstars:
                reln, tupl, sens = tstar.asTuple()
                if not tupl:
                    continue
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
                    tsens = TupleSens(reln, tupl, sens)
                    if DEBUG:
                        print_tuplesens(tsens)
                    assert(sens == gans)
    except TimeoutError:
        raise TimeoutError
    except AssertionError:
        raise TStarError(tstar, 'Sens: %d'%sens, 'Gans: %d'%gans)

def tuple_sens(T: Tree, _conn, exclusion=[]):
    global conn
    conn = _conn

    time_start = time.time()

    prepare_tree(T)
    prepare_botjoin(T)
    prepare_topjoin(T)
    prepare_dbotjoin(T)
    tstar, local_tstar_list = select_most_sensitive_tuple(T, exclusion)

    time_finsh = time.time()
    elapsed = time_finsh - time_start

    return tstar, local_tstar_list, elapsed

def local_tuple_sens(T: Tree, _conn, exclusion=[]):
    global conn
    conn = _conn

    time_start = time.time()

    prepare_tree(T)
    prepare_botjoin(T)
    prepare_topjoin(T)
    prepare_dbotjoin(T)
    prepare_freqtable(T)
    prepare_tuplesens(T, exclusion)

    time_finsh = time.time()
    elapsed = time_finsh - time_start

    return elapsed

def evaluate_query(T: Tree, _conn):
    global conn
    conn = _conn

    time_start = time.time()
    dprint(time_start)
    prepare_tree(T)
    prepare_botjoin(T, calc_root=True)

    for node in T.nodes:
        if node.isRoot():
            sql = 'SELECT total_cnt from {name}'.format(name=get_botjoin_tablename(node))
            cur = run_sql(sql)
            res = cur.fetchone()['total_cnt']
    total_cnt = res if res else 0

    time_finsh = time.time()
    dprint(time_finsh)
    elapsed = time_finsh - time_start

    return total_cnt, elapsed

def gen_report(algo_name, arch, scale, query, tstar, local_tstar_list, avg_time, time_list, test_pass):
    reln, tupl, sens = tstar.asTuple()
    elapsed = '%.3f'%avg_time
    print(algo_name, arch, scale, query, elapsed, reln, tupl, sens, local_tstar_list, time_list, test_pass, sep=' | ')

def gen_report_title():
    print('algo', 'arch', 'scale', 'query', 'time', 'relation', 'tuple', 'sensitivity', 'all_table_tstar', 'time_list' ,'test pass', sep=' | ')

def gen_query_report(arch, scale, query, total_cnt, avg_time, time_list, test_pass):
    avg_time = '%.3f'%avg_time
    print(arch, scale, query, total_cnt, avg_time, time_list, test_pass, sep=' | ')

def gen_query_report_title():
    print('arch', 'scale', 'query', 'toal_cnt', 'avg_time', 'time_list', 'test_pass', sep=' | ')

def print_tuplesens(tsens):
    if tsens:
        reln, tupl, sens = tsens.asTuple()
    else:
        reln, tupl, sens = None, None, None
    print('- Relation: ', reln)
    print('- Tuple: ', tupl)
    print('- sensitivity: ', sens)
    print()


def print_humanreadable_report(algo_name, arch, scale, query, tstar, local_tstar_list, elapsed, test_pass):
    print('algorithm:', algo_name)
    print('arch-%s-scale-%s'%(arch, scale))
    print('query: ', query)
    print('Time Elapsed: %.3f'%(elapsed))
    print('-'*20)
    print("The most sensitive tuple is")
    print_tuplesens(tstar)
    print("The most sensitive tuple for each table")
    print('\n'.join(str(local_tstar) for local_tstar in local_tstar_list))
    print("- Test Result: ", test_pass)
    print()

