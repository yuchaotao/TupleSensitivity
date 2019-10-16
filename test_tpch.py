#!/usr/bin/python3

from run import run_TSens, run_Elastic, run_LTSens
from algo import gen_report_title
from objects import Relation
from dp import DP_TSens

arch = 'tpch'

def test_full(report=False):
    for scale in ['0.1', '1', '2', '10']:
        for q in ['q1', 'q2']:
            run_TSens(arch, scale, q, report=report)

def test_q1(report=False):
    #for scale in ['0.0001', '0.01', '0.1', '1', '2', '10']:
    #for scale in ['0.01', '0.1']:
    for scale in ['0.01']:
        run_TSens(arch, scale, q='q1', report=report)

def test_q2(report=False):
    scale = '0.1'
    scale = '0.0001'
    #for scale in ['0.0001', '0.01', '0.1', '1', '2', '10']:
    #for scale in ['0.01', '0.1']:
    for scale in ['1']:
        run_TSens(arch, scale, q='q2', report=report)

def test_q3(report=False):
    q = 'q2'
    exclusion = ['LINEITEM']
    #for scale in ['0.0001', '0.01', '0.1']:
    #for scale in ['0.1']:
    for scale in ['0.01']:
        run_TSens(arch, scale, q='q3', exclusion=exclusion, report=report)

def test_q1_elastic():
    scale = '0.01'
    run_Elastic(arch, scale, q='q1')

def test_q2_elastic():
    scale = '0.01'
    run_Elastic(arch, scale, q='q2')

def test_q3_elastic():
    scale = '0.01'
    run_Elastic(arch, scale, q='q3')

def test_q1_LTSens():
    scale = '1'
    run_LTSens(arch, scale, q='q1')

def test_q2_LTSens():
    scale = '0.01'
    run_LTSens(arch, scale, q='q1')

def testDP_q1_TSens():
    scale = '1'
    eps = 1.0
    reln = Relation('any', 'CUSTOMER', {})
    nosy_ans, gsens, bias_ans, true_ans = DP_TSens(arch, scale, reln, eps)
    print('Noisy Ans', nosy_ans)
    print('Global Sens:', gsens)
    print('Bias  Ans', bias_ans)
    print('True  Ans:', true_ans)


def gen_report():
    report=True
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
    report=True
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

def run_missing():
    #report=True
    report=False
    for scale in ['0.1', '1']:
        run_TSens(arch, scale, q='q3', exclusion=['LINEITEM'], report=report)


if __name__ == '__main__':
    #test_q1_elastic()
    #test_q2_elastic()
    #test_q3_elastic()
    #test_q1()
    #test_q2()
    #test_q3()
    test_q1_LTSens()
    testDP_q1_TSens()
    #test_full()

    #gen_report()
    #run_missing()
