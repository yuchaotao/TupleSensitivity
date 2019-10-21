import numpy as np
import pandas as pd
from functools import reduce

from utils import Infix, dprint, INS
from db import use_db, run_sql
from objects import get_relnsens_tablename, gen_sqlstr_attributes
from objects import Tree
import elastic

Lap = lambda scale: np.random.laplace(0, scale)
DIV = Infix(lambda x,y: np.divide(x,y))

def SVT(next_q, next_T, q_sens, eps, c=1):
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
    attrs = [attr.orig_name for attr in attrs]
    attrs = gen_sqlstr_attributes(attrs)
    sql = "SELECT {attrs}, COUNT(*) AS freq FROM {name} GROUP BY {attrs}"
    sql = sql.format(name=reln.name, attrs=attrs)
    df = pd.read_sql(sql, conn)
    return df, np.array(df['freq'].tolist())

def learn_threshold_TSens(tsenses, tsens_limit, eps):
    q_sens = 1
    c = 1
    eps_Qhat = eps |DIV| 10
    eps_tsens = eps - eps_Qhat
    Q_hat = np.sum(tsenses) + Lap(tsens_limit |DIV| eps_Qhat)
    #Q_hat = np.sum(tsenses)
    next_q = next_q_TSens(tsenses, Q_hat)
    res = SVT(next_q(), next_T(), q_sens, eps_tsens, c)
    threshold = len(res)
    return threshold

def truncate_TSens(tsenses, max_tsens):
    return np.sum(tsenses[tsenses <= max_tsens])

def DP_TSens(arch, scale, reln, tsens_limit, eps=1.0):
    pre_eps = eps |DIV| 2
    run_eps = eps - pre_eps

    df, tsenses = get_TSens_distribution(arch, scale, reln)
    true_ans = np.sum(tsenses)
    max_tsens = learn_threshold_TSens(tsenses, tsens_limit, pre_eps)
    gsens = max_tsens
    bias_ans = truncate_TSens(tsenses, max_tsens)
    noise = Lap(max_tsens |DIV| run_eps)
    nosy_ans = bias_ans + noise
    return nosy_ans, gsens, bias_ans, true_ans, eps, pre_eps, run_eps

def next_T():
    while True:
        yield 0

def next_q_TSens(tsenses, Q_hat):
    tsenses = tsenses
    def _func():
        nonlocal tsenses
        i = 1
        shift = Q_hat - np.sum(tsenses)
        while True:
            tsenses = tsenses[tsenses > i]
            res = - ((np.sum(tsenses) + shift) |DIV| i)
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

class BasesensLearning:
    pass

def _learn_basesens_PrivateSQL(reln, arch, scale, eps_budget, direction='forward'):
    if reln.sens |INS| BasesensLearning:
        return
    if direction == 'backward':
        return reln.sens
    if reln.sens != 0:
        sens = reln.sens
    else:
        reln.sens = BasesensLearning()
        sens = 0
        parent_sens = 0
        for foreignkey_source in reln.foreignkey_sources:
            for attr, pr in foreignkey_source:
                parent_sens += _learn_basesens_PrivateSQL(pr, arch, scale, eps_budget, 'backward')
        for foreignkey_source in reln.foreignkey_sources:
            for attr, pr in foreignkey_source:
                if pr.sens == 0:
                    continue
                df, freqs = get_freq_distribution(arch, scale, reln, [attr])
                next_q = next_q_PrivateSQL(freqs)
                res = SVT(next_q(), next_T(), parent_sens, eps_budget[(attr, reln)])
                attr.mf = len(res)
                attr.actual_mf = max(freqs)
                dprint('[mf Learning]', (reln, attr), attr.mf, attr.actual_mf)
                sens += attr.mf * pr.sens
        reln.sens = sens
    for primarykey_target in reln.primarykey_targets:
        for attr, fr in primarykey_target:
            _learn_basesens_PrivateSQL(fr, arch, scale, eps_budget, 'forward')
    return sens

def create_eps_budget(primary_private_reln, eps):
    visited = set()
    def _travel(reln):
        nonlocal visited
        dprint(reln.name, reln.primarykey_targets)
        for primarykey_target in reln.primarykey_targets:
            for attr, fr in primarykey_target:
                if (attr, fr) in visited:
                    continue
                else:
                    visited.add((attr, fr))
                    _travel(fr)
    _travel(primary_private_reln)
    eps_each = eps |DIV| len(visited)
    eps_budget = {item: eps_each for item in visited}
    dprint(eps_budget)
    return eps_budget

def learn_basesens_PrivateSQL(arch, scale, primary_private_reln, schema, eps):
    # Init Sensitivity
    for reln in schema.relations:
        if reln == primary_private_reln:
            reln.sens = 1
            primary_private_reln = reln
        else:
            reln.sens = 0
    eps_budget = create_eps_budget(primary_private_reln, eps)
    _learn_basesens_PrivateSQL(primary_private_reln, arch, scale, eps_budget)

    for reln in schema.relations:
        dprint('[Base Sens]', reln, reln.sens)

def _init_mf_PrivateSQL(arch, scale, schema):
    conn = use_db(arch, scale)
    def _func(reln):
        # TODO: Deduct eps for private attributes
        nonlocal conn, schema
        schema_reln = retrieve(schema.relations, reln)
        schema_attrs = schema_reln.attributes
        for attr in schema_attrs:
            if not attr.mf:
                elastic.init_mf_attr(reln, attr, conn)
            #dprint('[INIT MF ATTR <reln, attr, mf, actual_mf>]', reln, attr.orig_name, attr.mf, attr.actual_mf)
        for attr in reln.attributes:
            attr.mf = retrieve(schema_attrs, attr).mf
    return _func

def _init_size_PrivateSQL(arch, scale):
    conn = use_db(arch, scale)
    def _func(reln):
        nonlocal conn
        universal_df, _ = get_freq_distribution(arch, scale, reln, reln.attributes)
        reln.actual_df = universal_df.rename(columns={attr.orig_name.lower(): attr.join_name.lower() for attr in reln.attributes})
        actual_size = sum(universal_df['freq'])
        for attr in reln.attributes:
            mf = attr.mf
            #dprint('[INIT SIZE - MF]', reln, attr, mf)
            attr_name = attr.orig_name.lower()
            partial_df, _ = get_freq_distribution(arch, scale, reln, [attr])
            partial_df = partial_df[partial_df['freq'] <= mf].rename(columns={'freq': 'freq'+attr_name})
            universal_df = universal_df.join(partial_df.set_index(attr_name), on=attr_name)
        universal_df = universal_df.dropna()
        size = sum(universal_df['freq'])
        reln.size = size
        reln.df = universal_df[[attr.orig_name.lower() for attr in reln.attributes] + ['freq']]
        reln.df = reln.df.rename(columns={attr.orig_name.lower(): attr.join_name.lower() for attr in reln.attributes})
        #dprint('[INIT SIZE]', reln, size, actual_size)
    return _func

def _init_sens_PrivateSQL(primary_private_reln):
    def _func(reln):
        if reln == primary_private_reln:
            reln.sens = 1
        else:
            reln.sens = 0
        #dprint('[INIT SENS]', reln, reln.sens)
    return _func

def natural_join(relations, flag = 'bias'):
    def _natural_join(dfA, dfB):
        dfA = dfA.rename(columns={'freq': 'freqA'})
        dfB = dfB.rename(columns={'freq': 'freqB'})
        common_columns = list(set(dfA.columns) & set(dfB.columns))
        if common_columns:
            df = pd.merge(dfA, dfB, on=common_columns, how='inner')
        else:
            dfA['key'] = 1
            dfB['key'] = 1
            df = pd.merge(dfA, dfB, on='key', how='outer').drop('key', axis=1)
        df['freq'] = df['freqA'] * df['freqB']
        df = df.drop('freqA', axis=1)
        df = df.drop('freqB', axis=1)

        #dprint(common_columns)
        #dprint(dfA.head(2))
        #dprint(dfB.head(2))
        #dprint(df.head(2))
        #dprint()

        return df
    if flag == 'bias':
        df = reduce(_natural_join, [reln.df for reln in relations])
    else:
        df = reduce(_natural_join, [reln.actual_df for reln in relations])
    return df

def DP_PrivateSQL(arch, scale, hypertree: Tree, primary_private_reln, limit, schema, eps=1.0):
    pre_eps = eps |DIV| 2
    run_eps = eps - pre_eps

    relations = hypertree.node_map.keys()

    learn_basesens_PrivateSQL(arch, scale, primary_private_reln, schema, pre_eps)
    init_mf = _init_mf_PrivateSQL(arch, scale, schema)
    init_size = _init_size_PrivateSQL(arch, scale)
    init_sens = _init_sens_PrivateSQL(primary_private_reln)

    _, _, esens = elastic.elastic_sens_w_policy(hypertree, None, init_mf, init_size, init_sens).asTuple()
    bias_ans = sum(natural_join(relations, 'bias')['freq'])
    true_ans = sum(natural_join(relations, 'true')['freq'])
    nosy_ans = bias_ans + Lap(esens |DIV| run_eps)

    dprint('[ESens]', esens)
    dprint('[Bias Ans]', bias_ans)
    dprint('[Nosy Ans]', nosy_ans)
    dprint('[True Ans]', true_ans)

    return nosy_ans, esens, bias_ans, true_ans, eps, pre_eps, run_eps

def retrieve(array, item):
    return array[array.index(item)]

def print_humanreadable_report(algo_name, arch, scale, q, reln, limit, reps, eps, pre_eps, run_eps, nosy_ans, gsens, bias_ans, true_ans, nosy_ans_list, bias_ans_list, gsens_list):
    print('arch: {arch}, scale: {scale}, query: {q}'.format(arch=arch, scale=scale, q=q))
    print('algo_name: {algo_name}, reps: {reps}'.format(algo_name=algo_name, reps=reps))
    print('pprivate reln: {reln}, eps: {eps}'.format(reln=reln, eps=eps))
    print('nosy_ans: %.3f, gsens: %d, bias_ans: %d, true_ans: %d'%(nosy_ans, gsens, bias_ans, true_ans))
    print('eps: %.3f, pre_eps: %.3f, run_eps: %.3f'%(eps, pre_eps, run_eps))
    print('nosy_ans_list:', nosy_ans_list)
    print('bias_ans_list:', bias_ans_list)
    print('gsens_list:', gsens_list)

def gen_report(algo_name, arch, scale, q, reln, limit, reps, eps, pre_eps, run_eps, nosy_ans, gsens, bias_ans, true_ans, nosy_ans_list, bias_ans_list, gsens_list):
    print(algo_name, arch, scale, q, reln, limit, reps, eps, pre_eps, run_eps, nosy_ans, gsens, bias_ans, true_ans, nosy_ans_list, bias_ans_list, gsens_list, sep=' | ')

def gen_report_title():
    print('algo_name', 'arch', 'scale', 'q', 'reln', 'limit', 'reps', 'eps', 'pre_eps', 'run_eps', 'nosy_ans', 'gsens', 'bias_ans', 'true_ans', 'nosy_ans_list', 'bias_ans_list', 'gsens_list', sep=' | ')


