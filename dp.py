import numpy as np
import pandas as pd

from utils import Infix
from db import use_db, run_sql
from objects import get_relnsens_tablename

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

def learn_threshold_TSens(tsenses, eps):
    q_sens = 1
    c = 1
    def _next_q(tsenses):
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
    next_q = _next_q(tsenses)
    def next_T():
        while True:
            yield 0
    res = SVT(next_q(), next_T(), q_sens, eps, c)
    threshold = len(res)
    return threshold

def truncate_TSens(tsenses, max_tsens):
    return np.sum(tsenses[tsenses <= max_tsens])

def DP_TSens(arch, scale, reln, eps=1.0):
    df, tsenses = get_TSens_distribution(arch, scale, reln)
    true_ans = np.sum(tsenses)
    max_tsens = learn_threshold_TSens(tsenses, eps)
    gsens = max_tsens
    bias_ans = truncate_TSens(tsenses, max_tsens)
    noise = Lap(max_tsens)
    nosy_ans = bias_ans + noise
    return nosy_ans, gsens, bias_ans, true_ans
