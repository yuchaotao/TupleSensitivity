from copy import deepcopy as copy
import time
import random

from objects import Attribute, Relation, Node, Tree, TupleSens
from objects import gen_sqlstr_attributes
from objects import max_ltstars
from db import run_sql
from utils import dprint

def elastic_sens_ltstars(hypertree: Tree, _conn):
    global conn
    conn = _conn

    time_start = time.time()

    for reln in hypertree.node_map:
        init_mf(reln)
        init_size(reln)

    relations = hypertree.node_map.keys()

    ltstars = []
    for primary_private_reln in relations:
        elasticsens = 0
        for reln in hypertree.node_map:
            if reln == primary_private_reln:
                init_sens(reln, 1)
            else:
                init_sens(reln, 0)
        for node in hypertree.nodes:
            init_node_sensreln(node)
            init_node_botsensreln(node)
        for node in hypertree.nodes:
            nodesens = calc_botsens(node)
            if node.isRoot():
                elasticsens = nodesens.sens
        ltstar = select_ltstar(primary_private_reln, relations)
        ltstars.append(TupleSens(primary_private_reln, ltstar, elasticsens))
    tstar, ltstars = max_ltstars(ltstars)

    time_finsh = time.time()
    elapsed = time_finsh - time_start
    return tstar, ltstars, elapsed

def select_ltstar(primary_private_reln, relations):
    ltstar = []
    for attr in primary_private_reln.attributes:
        mf_candidates = []
        for reln in relations:
            for other_attr in reln.attributes:
                if attr == other_attr:
                    mf_candidates.append(other_attr.mf_value)
        attr.ptstar = (attr.join_name, random.choice(mf_candidates))
        ltstar.append(attr.ptstar)
    return ltstar


def elastic_sens(hypertree: Tree, _conn):
    global conn
    conn = _conn
    elasticsens = 0
    for reln in hypertree.node_map:
        init_mf(reln)
        init_size(reln)
        init_sens(reln)
    for node in hypertree.nodes:
        nodesens = calc_botsens(node)
        if node.isRoot():
            elasticsens = nodesens.sens
    return TupleSens('any', 'any', elasticsens)

def elastic_sens_w_policy(hypertree: Tree, _conn, init_mf, init_size, init_sens):
    global conn
    conn = _conn
    elasticsens = 0
    for reln in hypertree.node_map:
        init_mf(reln)
        init_size(reln)
        init_sens(reln)
    for node in hypertree.nodes:
        nodesens = calc_botsens(node)
        if node.isRoot():
            elasticsens = nodesens.sens
    return TupleSens('any', 'any', elasticsens)

def calc_botsens(node: Node) -> Relation:
    if node.botsensreln != None:
        return node.botsensreln
    #dprint('[+ calc_botsens]: ', node.name, node.relations)
    nodesens_reln = calc_nodesens(node)
    tmp_reln = nodesens_reln
    for child in node.children:
        calc_botsens(child)
        tmp_reln = calc_joinsens(tmp_reln, child.botsensreln)
    node.botsensreln = tmp_reln
    #dprint('[- calc_botsens]: ', node.name, node.relations, tmp_reln.sens)
    return tmp_reln

def calc_nodesens(node: Node) -> Relation:
    if node.sensreln != None:
        return node.sensreln
    #dprint('[+ calc_nodesens]: ', node.name, node.relations)
    tmp_reln = gen_dummy_reln()
    for reln in node.relations:
        tmp_reln = calc_joinsens(tmp_reln, reln)
    node.sensreln = tmp_reln
    #dprint('[- calc_nodesens]: ', node.name, node.relations)
    return tmp_reln

def calc_joinsens(relnA: Relation, relnB: Relation) -> Relation:
    attrsA = relnA.attributes
    attrsB = relnB.attributes
    sizeA = relnA.size
    sizeB = relnB.size
    join_attrs = attrsA & attrsB
    sensA, mfA = relnA.sens, get_mf(relnA, join_attrs)
    sensB, mfB = relnB.sens, get_mf(relnB, join_attrs)

    new_sens = max(sensA * mfB, sensB * mfA)
    new_attrs = copy(attrsA | attrsB)
    update_mf(new_attrs, join_attrs, relnA, relnB)
    new_size = sizeA * sizeB
    new_name = gen_join_name(relnA, relnB)

    tmp_reln = gen_dummy_reln(name=new_name)
    tmp_reln.sens = new_sens
    tmp_reln.attributes = new_attrs
    tmp_reln.size = new_size

    debug_info =  'Join: {relnA}({attrsA}) and {relnB}({attrsB}) on {attrs}\n'
    debug_info += '  |-- {relnA}: sens({sensA}) mf({mfA}) size({sizeA})\n'
    debug_info += '  |-- {relnB}: sens({sensB}) mf({mfB}) size({sizeB})\n'
    debug_info += '  |-- New Attrs: {new_attrs} \n'
    debug_info += '  |-- New Size: {new_size} \n'
    debug_info += '  |-- New Sens: {new_sens} \n'
    debug_info = debug_info.format(relnA=relnA, relnB=relnB, attrs=join_attrs, sensA=sensA, sensB=sensB, mfA=mfA, mfB=mfB, sizeA=sizeA, sizeB=sizeB, attrsA=attrsA, attrsB=attrsB, new_sens=new_sens, new_attrs={'%s(%d)'%(attr, attr.mf) for attr in new_attrs}, new_size=new_size)
    #dprint(debug_info)

    return tmp_reln

def gen_dummy_reln(index='dummy', name='dummy'):
    reln = Relation(index, name, set())
    reln.size = 1
    reln.sens = 0
    return reln

def gen_join_name(relnA, relnB):
    if relnA.name == 'dummy':
        return relnB.name
    else:
        return relnA.name + '_' + relnB.name

def get_mf(reln, attrs):
    if len(attrs) == 0:
        mf = reln.size
    elif len(attrs) == 1:
        attr = list(attrs)[0]
        for reln_attr in reln.attributes:
            if reln_attr == attr:
                mf = reln_attr.mf
    elif len(attrs) > 1:

        mf = min(*[get_mf(reln, [attr]) for attr in attrs])
    return mf

def update_mf(new_attrs, join_attrs, relnA, relnB):
    for attr in new_attrs:
        #dprint('before: ', attr, attr.mf)
        if attr in relnA.attributes:
            attr.mf *= get_mf(relnB, join_attrs)
        elif attr in relnB.attributes:
            attr.mf *= get_mf(relnA, join_attrs)
        else:
            raise Exception('Unknown Attribute: %s'%attr)
        #dprint('after : ', attr, attr.mf)

def init_mf(reln):
    global conn
    for attr in reln.attributes:
        sql = "SELECT {attr} as attr, COUNT(*) AS mf FROM {reln} GROUP BY {attr} ORDER BY mf DESC LIMIT 1"
        sql = sql.format(reln=reln.name, attr=attr.orig_name)
        cur = run_sql(sql, conn)
        res = cur.fetchone()
        attr.mf = res['mf']
        attr.mf_value = res['attr']
        dprint(attr.orig_name, attr.mf, attr.mf_value)

def init_mf_attr(reln, attr, conn):
    sql = "SELECT {attr} as attr, COUNT(*) AS mf FROM {reln} GROUP BY {attr} ORDER BY mf DESC LIMIT 1"
    sql = sql.format(reln=reln.name, attr=attr.orig_name)
    cur = run_sql(sql, conn)
    res = cur.fetchone()
    attr.mf = res['mf']
    attr.mf_value = res['attr']

def init_size(reln):
    global conn
    sql = "SELECT COUNT(*) AS size FROM {reln}"
    sql = sql.format(reln=reln.name)
    cur = run_sql(sql, conn)
    res = cur.fetchone()
    reln.size = res['size']

def init_sens(reln, sens=1):
    reln.sens = sens

def init_node_sensreln(node):
    node.sensreln = None

def init_node_botsensreln(node):
    node.botsensreln = None
