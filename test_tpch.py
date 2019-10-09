#!/usr/bin/python3

import algo
import import_hypertree
import psycopg2 as pg2
import time
import traceback

conn = None
arch = 'tpch'
qfile_format = 'queries/{arch}/{q}.hypertree'

def use_db(arch, scale):
    global conn
    dbname = 'arch-{arch}-scale-{scale}'.format(arch=arch, scale=scale)
    conn = pg2.connect(dbname=dbname, host='localhost', user='duke', password='duke')

def _test(arch, scale, q, hypertree_file, report=False):
    global conn
    use_db(arch, scale)
    T, nodes, relations = import_hypertree.read_hypertree_from_file(hypertree_file)

    tstar, ltstars, elapsed = algo.run_algo(T, conn)
    reln, tupl, sens = tstar

    test_pass = 'Unknown'
    #if True:
    if scale in ['0.0001']:
         try:
             algo.test_ground(relations, ltstars)
         except:
             test_pass = 'Failed'
             traceback.print_exc()
         else:
             test_pass = 'Succeeded'

    if report:
        algo.gen_report(arch, scale, q, tstar, ltstars, elapsed, test_pass)
    else:
        algo.print_humanreadable_report(arch, scale, q, tstar, ltstars, elapsed, test_pass)

def test_full(report=False):
    for scale in ['0.1', '1', '2', '10']:
        for q in ['q1', 'q2']:
            hypertree_file = qfile_format.format(arch=arch, q=q)
            _test(arch, scale, q, hypertree_file, report)

def test_q1(report=False):
    q = 'q1'
    scale = '0.1'
    scale = '0.0001'
    #for scale in ['0.0001', '0.01', '0.1', '1', '2', '10']:
    for scale in ['0.01', '0.1']:
        hypertree_file = qfile_format.format(arch=arch, q=q)
        _test(arch, scale, q, hypertree_file, report)

def test_q2(report=False):
    q = 'q2'
    #for scale in ['0.0001', '0.01', '0.1']:
    for scale in ['0.01']:
        hypertree_file = qfile_format.format(arch=arch, q=q)
        _test(arch, scale, q, hypertree_file, report)

if __name__ == '__main__':
    #test_q1()
    test_q2()
    #test_full()
