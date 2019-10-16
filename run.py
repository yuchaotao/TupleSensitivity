#!/usr/bin/python3
import traceback
import sys

import import_hypertree
import db
from algo import TStarError
from utils import TimeoutError
from algo import print_tuplesens, gen_report, print_humanreadable_report
from algo import tuple_sens
from algo import local_tuple_sens
from algo import test_ground
from elastic import elastic_sens, elastic_sens_ltstars

qfile_format = 'queries/{arch}/{q}.hypertree'

def test_correctness(relations, ltstars):
    try:
        test_ground(relations, ltstars)
    except TStarError:
        test_pass = 'Failed'
        traceback.print_exc()
    except TimeoutError:
        test_pass = 'Unknown'
    else:
        test_pass = 'Succeeded'
    return test_pass

def run(algo_name, arch, scale, q,  exclusion=[], report='print'):
    conn = db.use_db(arch, scale)
    hypertree_file = qfile_format.format(arch=arch, q=q)
    T, nodes, relations = import_hypertree.read_hypertree_from_file(hypertree_file)

    if algo_name == 'TSens':
        tstar, ltstars, elapsed = tuple_sens(T, conn, exclusion)
        test_pass = test_correctness(relations, ltstars)
    elif algo_name == 'Elastic':
        tstar, ltstars, elapsed = elastic_sens_ltstars(T, conn)
        test_pass = 'Unknown'
    elif algo_name == 'LTSens':
        local_tuple_sens(T, conn, exclusion)
    else:
        raise Exception('Unknown Algorithm')

    if report == 'file':
        gen_report(algo_name, arch, scale, q, tstar, ltstars, elapsed, test_pass)
    elif report == 'print':
        print_humanreadable_report(algo_name, arch, scale, q, tstar, ltstars, elapsed, test_pass)

    sys.stdout.flush()

def run_TSens(arch, scale, q, exclusion=[], report='print'):
    run('TSens', arch, scale, q, exclusion, report)

def run_Elastic(arch, scale, q, exclusion=[], report='print'):
    run('Elastic', arch, scale, q, exclusion, report)

def run_LTSens(arch, scale, q, exclusion=[], report=None):
    run('LTSens', arch, scale, q, exclusion, report)
