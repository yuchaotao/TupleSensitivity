import numpy as np
import pandas as pd

from utils import Infix
from db import use_db, run_sql
from objects import get_relnsens_tablename, gen_sqlstr_attributes
import elastic

Lap = lambda scale: np.random.laplace(0, scale)
DIV = Infix(lambda x,y: np.divide(x,y))

def SVT(next_q, next_T, q_sens, eps, c):
    res = []
    eps_1 = eps |DIV| 2
    eps_2 = eps - eps_1
    rou = Lap(q_sens |DIV| eps_1)
    count = 0
    while count < c:
        q = next_q.__next__()
        T = next_T.__next__()
        v = Lap((2*c*q_sens) |DIV| eps_2)
        if q + v >= T + rou:
            res.append(True)
            count += 1
        else:
            res.append(False)
    return res

def get_TSens_distribution(arch, scale, reln):
    '''
    retrieve the relnsens table of this reln.
    TODO: Each reln should contain a query index
    '''
    conn = use_db(arch, scale)
    name = get_relnsens_tablename(reln)
    sql = "SELECT * FROM {name}".format(name=name)
    df = pd.read_sql(sql, conn)
    return df, np.array(df['tsens'].tolist())

def get_freq_distribution(arch, scale, reln, attrs):
    conn = use_db(arch, scale)
    attrs = gen_sqlstr_attributes(attrs)
    sql = "SELECT COUNT(*) AS freq FROM {name} GROUP BY {attrs}"
    sql = sql.format(name=reln.name, attrs=attrs)
    df = pd.read_sql(sql, conn)
    return df, np.array(df['freq'].tolist())

def learn_threshold_TSens(tsenses, eps):
    q_sens = 1
    c = 1
    def _next_q(tsenses):
    next_q = next_q_TSens(tsenses)
    res = SVT(next_q(), next_T(), q_sens, eps, c)
    threshold = len(res)
    return threshold

def truncate_TSens(tsenses, max_tsens):
    return np.sum(tsenses[tsenses <= max_tsens])

def DP_TSens(arch, scale, reln, eps=1.0):
    df, tsenses = get_TSens_distribution(arch, scale, reln)
    true_ans = np.sum(tsenses)
    pre_eps = eps |DIV| 2
    run_eps = eps - pre_eps
    max_tsens = learn_threshold_TSens(tsenses, pre_eps)
    gsens = max_tsens
    bias_ans = truncate_TSens(tsenses, max_tsens)
    noise = Lap(max_tsens |DIV| run_eps)
    nosy_ans = bias_ans + noise
    return nosy_ans, gsens, bias_ans, true_ans, eps, pre_eps, run_eps

def next_T():
    while True:
        yield 0

def next_q_TSens(tsenses):
    tsenses = tsenses
    def _func():
        nonlocal tsenses
        i = 1
        while True:
            tsenses = tsenses[tsenses > i]
            res = - (np.sum(tsenses) |DIV| i)
            yield res
            i += 1
    return _func

def next_q_PrivateSQL(freqs):
    def _func():
        nonlocal freqs
        i = 1
        while True:
            freqs = freqs[freqs > i]
            res = - (np.sum(freqs) |DIV| i)
            yield res
            i += 1
    return _func

def _learn_basesens_PrivateSQL(reln, arch, scale, eps_budget):
    if reln.sens != 0:
        sens = reln.sens
    else:
        parent_sens = sum(map(lambda key, pr:_learn_basesens_PrivateSQL(pr, arch, scale, eps_budget), reln.foreignkey_sources))
        for attr, pr in reln.foreignkey_sources:
            freqs = get_freq_distribution(arch, scale, reln, [attr])
            next_q = next_q_PrivateSQL(freqs)
            attr.mf = SVT(next_q(), next_T(), parent_sens, eps_budget[(attr, reln)])
            reln.sens += attr.mf * pr.sens
        sens = reln.sens
    for attr, fr in reln.primarykey_targets:
        _learn_basesens_PrivateSQL(fr, arch, scale, eps_budget)
    return sens

def create_eps_budget(primary_private_reln, eps):
    visited = {}
    def _travel(reln):
        nonlocal visited
        for attr, fr in reln.primarykey_targets:
            if (attr, fr) in visited:
                continue
            else:
                visited.add((attr, fr))
                _travel(pr)
    _travel(primary_private_reln)
    eps_each = eps |DIV| len(visited)
    eps_budget = {item: eps_each for item in visited}
    return eps_budget

def learn_basesens_PrivateSQL(arch, scale, primary_private_reln, schema, eps):
    # Init Sensitivity
    for reln in schema.relations:
        if reln == primary_private_reln:
            reln.sens = 1
        else:
            reln.sens = 0
    eps_budget = create_eps_budget(primary_private_reln, eps)
    for reln in schema.relations:
        _learn_basesens_PrivateSQL(reln, arch, scale, eps_budget)

def DP_PrivateSQL(arch, scale, primary_private_reln, schema, get_trueans, eps=1.0):
    pre_eps = eps |DIV| 2
    run_eps = eps - pre_eps

    learn_basesens_PrivateSQL(arch, scale, primary_private_reln, schema, eps)
    def _init_mf(arch, scale, schema):
        conn = db.use_db(arch, scale)
        def _func(reln):
            nonlocal conn, schema
            schema_reln = retrieve(schema.relations, reln)
            schema_attrs = schema_reln.attributes
            for attr in reln.attributes:
                if attr in schema_attrs:
                    schema_attr = retrieve(schema_attrs, attr)
                    if schema_attr.mf:
                        attr.mf = schema_attr.mf
                        continue
                # else + else
                elastic.init_mf_attr(reln, attr, conn)
        return _func
    init_mf = _init_mf(arch, scale, schema)
    def _init_size()


def retrieve(array, item):
    return array[array.index(item)]
