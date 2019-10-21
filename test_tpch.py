#!/usr/bin/python3

from run import run_TSens, run_Elastic, run_LTSens, run_Query, run_PrivateSQL, run_TSensDP
from algo import gen_report_title, gen_query_report_title
from objects import Relation
from dp import DP_TSens, DP_PrivateSQL
import dp
from import_schema import read_schema_from_file
from import_hypertree import read_hypertree_from_file

arch = 'tpch'

def test_full():
    for scale in ['0.1', '1', '2', '10']:
        for q in ['q1', 'q2']:
            run_TSens(arch, scale, q)

def test_q1():
    #for scale in ['0.0001', '0.01', '0.1', '1', '2', '10']:
    #for scale in ['0.01', '0.1']:
    for scale in ['0.01']:
        run_TSens(arch, scale, q='q1')

def test_q2():
    scale = '0.1'
    scale = '0.0001'
    #for scale in ['0.0001', '0.01', '0.1', '1', '2', '10']:
    #for scale in ['0.01', '0.1']:
    for scale in ['1']:
        run_TSens(arch, scale, q='q2')

def test_q3():
    q = 'q2'
    exclusion = ['LINEITEM']
    #for scale in ['0.0001', '0.01', '0.1']:
    #for scale in ['0.1']:
    for scale in ['0.01']:
        run_TSens(arch, scale, q='q3', exclusion=exclusion)

def test_q1_elastic():
    scale = '0.01'
    run_Elastic(arch, scale, q='q1')

def test_q2_elastic():
    scale = '0.01'
    run_Elastic(arch, scale, q='q2')

def test_q3_elastic():
    scale = '0.01'
    scale = '0.1'
    run_Elastic(arch, scale, q='q3')

def test_q1_LTSens():
    scale = '1'
    run_LTSens(arch, scale, q='q1', reps=1)

def test_q2_LTSens():
    scale = '0.01'
    run_LTSens(arch, scale, q='q2', reps=1)

def test_q3_LTSens():
    scale = '0.01'
    exclusion = ['LINEITEM']
    run_LTSens(arch, scale, q='q3', exclusion=exclusion, reps=1)

def testDP_q1_TSens():
    q = 'q1'
    scale = '0.01'
    eps = 1.0
    reln = Relation('any', 'CUSTOMER', {})
    limit = 100
    exclusion = []
    run_LTSens(arch, scale, q=q, exclusion=exclusion, report=None, reps=1)
    run_TSensDP(arch, scale, q, reln, limit, eps)

def testDP_q2_TSens():
    q = 'q2'
    scale = '0.01'
    eps = 1.0
    reln = Relation('any', 'SUPPLIER', {})
    limit = 500
    exclusion = []
    run_LTSens(arch, scale, q=q, exclusion=exclusion, report=None, reps=1)
    run_TSensDP(arch, scale, q, reln, limit, eps)

def testDP_q3_TSens():
    q = 'q3'
    scale = '0.01'
    eps = 1.0
    limit = 10
    reln = Relation('any', 'CUSTOMER', {})
    exclusion = ['LINEITEM']
    run_LTSens(arch, scale, q=q, exclusion=exclusion, report=None, reps=1)
    run_TSensDP(arch, scale, q, reln, limit, eps)

def testDP_q1_PrivateSQL():
    q = 'q1'
    scale = '0.01'
    eps = 1.0
    reln = Relation('any', 'CUSTOMER', {})
    run_PrivateSQL(arch, scale, q, reln, None, eps)

def testDP_q2_PrivateSQL():
    q = 'q2'
    scale = '0.01'
    eps = 1.0
    reln = Relation('any', 'SUPPLIER', {})
    run_PrivateSQL(arch, scale, q, reln, None, eps, reps=2)

def testDP_q3_PrivateSQL():
    q = 'q3'
    scale = '0.01'
    eps = 1.0
    reln = Relation('any', 'CUSTOMER', {})
    run_PrivateSQL(arch, scale, q, reln, None, eps, reps=2)

def test_q1_query():
    report = 'query'
    scale = '1'
    run_Query(arch, scale, q='q1', report=report, reps=1)

def gen_report():
    report='file'
    gen_report_title()
    for scale in ['0.0001', '0.001', '0.01', '0.1', '1', '2', '10']:
        for q in ['q1', 'q2', 'q3']:
            run_Elastic(arch, scale, q, report=report)
    for scale in ['0.0001', '0.001', '0.01', '0.1', '1', '2', '10']:
        run_TSens(arch, scale, q='q1', report=report)
    for scale in ['0.0001', '0.001', '0.01', '0.1', '1', '2', '10']:
        run_TSens(arch, scale, q='q2', exclusion=['LINEITEM'], report=report)
    for scale in ['0.0001', '0.001', '0.01', '0.1', '1']:
        run_TSens(arch, scale, q='q3', exclusion=['LINEITEM'], report=report)

def _gen_report():
    report='file'
    gen_report_title()
    for scale in ['0.0001']:
        for q in ['q1', 'q2', 'q3']:
            run_Elastic(arch, scale, q, report=report)
    for scale in ['0.0001']:
        run_TSens(arch, scale, q='q1', report=report)
    for scale in ['0.0001']:
        run_TSens(arch, scale, q='q2', exclusion=['LINEITEM'], report=report)
    for scale in ['0.0001']:
        run_TSens(arch, scale, q='q3', exclusion=['LINEITEM'], report=report)

def gen_query_report():
    report = 'query'
    gen_query_report_title()
    for scale in ['0.0001', '0.001', '0.01', '0.1', '1', '2', '10']:
        for q in ['q1', 'q2', 'q3']:
            run_Query(arch, scale, q, report=report)

def gen_DP_report():
    report = 'file'
    scale = '0.01'
    eps = 1.0
    reps = 10
    limits = {'q1': 100, 'q2': 500, 'q3': 10}
    relns = {
            'q1': Relation('any', 'CUSTOMER', {}),
            'q2': Relation('any', 'SUPPLIER', {}),
            'q3': Relation('any', 'CUSTOMER', {}),
            }
    exclusion = ['LINEITEM']
    dp.gen_report_title()
    for q in ['q1', 'q2', 'q3']:
        limit = limits[q]
        reln = relns[q]
        run_LTSens(arch, scale, q=q, exclusion=exclusion, report=None, reps=1)
        run_TSensDP(arch, scale, q, reln, limit, eps, report=report, reps=reps)
    for q in ['q1', 'q2', 'q3']:
        reln = relns[q]
        run_PrivateSQL(arch, scale, q, reln, None, eps, report=report, reps=reps)

def _gen_query_report():
    report = 'query'
    gen_query_report_title()
    for scale in ['0.001']:
        for q in ['q1', 'q2', 'q3']:
            run_Query(arch, scale, q, report=report)

def run_missing():
    #report=True
    report=False
    for scale in ['0.1', '1']:
        run_TSens(arch, scale, q='q3', exclusion=['LINEITEM'], report=report)

def run_elastic():
    report='file'
    gen_report_title()
    for scale in ['0.0001', '0.001', '0.01', '0.1', '1', '2', '10']:
        for q in ['q1', 'q2', 'q3']:
            run_Elastic(arch, scale, q, report=report)

if __name__ == '__main__':
    #test_q1_elastic()
    #test_q2_elastic()
    #test_q3_elastic()
    #test_q1()
    #test_q2()
    #test_q3()
    #test_q1_LTSens()
    #test_q2_LTSens()
    #test_q3_LTSens()
    #testDP_q1_TSens()
    #testDP_q2_TSens()
    #testDP_q3_TSens()
    #testDP_q1_PrivateSQL()
    #testDP_q2_PrivateSQL()
    #testDP_q3_PrivateSQL()
    #test_full()
    #test_q1_query()

    #gen_report()
    #_gen_report()
    #run_missing()
    #run_elastic()

    gen_query_report()
    #_gen_query_report()

    #gen_DP_report()
