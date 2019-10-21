#!/usr/bin/python3
import traceback
import sys
import numpy as np

import import_hypertree
import import_schema
import db
from algo import TStarError, QueryCntError
from utils import TimeoutError
from algo import print_tuplesens, gen_report, print_humanreadable_report
from algo import gen_query_report
from algo import tuple_sens
from algo import local_tuple_sens
from algo import evaluate_query
from algo import test_ground, test_query_ground, ground_truth
from elastic import elastic_sens, elastic_sens_ltstars
from objects import ElasticTupleSens
from dp import DP_PrivateSQL, DP_TSens
import dp

qfile_format = 'queries/{arch}/{q}.hypertree'
schema_file_format = 'schema/{arch}.schema'

def test_correctness(algo_name, relations, ltstars_or_total_cnt):
    try:
        if algo_name in ['TSens']:
            ltstars = ltstars_or_total_cnt
            test_ground(relations, ltstars)
        elif algo_name in ['Query']:
            total_cnt = ltstars_or_total_cnt
            test_query_ground(relations, total_cnt)
    except (TStarError, QueryCntError):
        test_pass = 'Failed'
        traceback.print_exc()
    except TimeoutError:
        test_pass = 'Unknown'
    else:
        test_pass = 'Succeeded'
    return test_pass

def lower_case_attr(attr):
    attr = [(k.lower(), v) for k,v in attr]
    return attr

def extend_elastic_ltstars(ltstars, relations, conn):
    ltstars = [ElasticTupleSens(ltstar.reln, ltstar.attr, ltstar.sens, ground_truth(relations, ltstar.reln, lower_case_attr(ltstar.attr), conn))  for ltstar in ltstars]
    return ltstars

def run(algo_name, arch, scale, q,  exclusion=[], report='print', reps=10):
    conn = db.use_db(arch, scale)
    hypertree_file = qfile_format.format(arch=arch, q=q)

    time_list = []
    for r in range(reps):
        T, nodes, relations = import_hypertree.read_hypertree_from_file(q, hypertree_file)

        if algo_name == 'TSens':
            tstar, ltstars, elapsed = tuple_sens(T, conn, exclusion)
            if r == 0:
                test_pass = test_correctness(algo_name, relations, ltstars)
        elif algo_name == 'Elastic':
            tstar, ltstars, elapsed = elastic_sens_ltstars(T, conn)
            ltstars = extend_elastic_ltstars(ltstars, relations, conn)
            test_pass = 'Unknown'
        elif algo_name == 'LTSens':
            elapsed = local_tuple_sens(T, conn, exclusion)
        elif algo_name == 'Query':
            total_cnt, elapsed = evaluate_query(T, conn)
            if r == 0:
                test_pass = 'Unknown'
                #test_pass = test_correctness(algo_name, relations, total_cnt)
            tstar, ltstars = None, []
        else:
            raise Exception('Unknown Algorithm')
        time_list.append(elapsed)
    avg_time = np.mean(time_list)

    if report == 'file':
        gen_report(algo_name, arch, scale, q, tstar, ltstars, avg_time, time_list, test_pass)
    elif report == 'print':
        print_humanreadable_report(algo_name, arch, scale, q, tstar, ltstars, avg_time, time_list, test_pass)
    elif report == 'query':
        gen_query_report(arch, scale, q, total_cnt, avg_time, time_list, test_pass)

    sys.stdout.flush()

def run_TSens(arch, scale, q, exclusion=[], report='print', reps=10):
    run('TSens', arch, scale, q, exclusion, report, reps)

def run_Elastic(arch, scale, q, exclusion=[], report='print', reps=10):
    run('Elastic', arch, scale, q, exclusion, report, reps)

def run_LTSens(arch, scale, q, exclusion=[], report=None, reps=10):
    run('LTSens', arch, scale, q, exclusion, report, reps)

def run_Query(arch, scale, q, exclusion=[], report='query', reps=10):
    run('Query', arch, scale, q, exclusion, report, reps)


def run_DP(algo_name, arch, scale, q, reln, limit, eps, report='print', reps=10):

    nosy_ans_list = []
    bias_ans_list = []
    gsens_list = []
    for r in range(reps):
        if algo_name == 'TSensDP':
            nosy_ans, gsens, bias_ans, true_ans, eps, pre_eps, run_eps = DP_TSens(arch, scale, reln, limit, eps)
        elif algo_name == 'PrivateSQL':
            schema_file = schema_file_format.format(arch=arch)
            schema = import_schema.read_schema_from_file(arch, schema_file)
            hypertree_file = qfile_format.format(arch=arch, q=q)
            T, nodes, relations = import_hypertree.read_hypertree_from_file(q, hypertree_file)
            nosy_ans, gsens, bias_ans, true_ans, eps, pre_eps, run_eps = DP_PrivateSQL(arch, scale, T, reln, limit, schema, eps)
        else:
            raise Exception('Unknown Algorithm')
        nosy_ans_list.append(nosy_ans)
        bias_ans_list.append(bias_ans)
        gsens_list.append(gsens)

    nosy_ans = np.mean(nosy_ans_list)
    bias_ans = np.mean(bias_ans_list)
    gsens = np.mean(gsens_list)
    if report == 'print':
        dp.print_humanreadable_report(algo_name, arch, scale, q, reln, limit, reps, eps, pre_eps, run_eps, nosy_ans, gsens, bias_ans, true_ans, nosy_ans_list, bias_ans_list, gsens_list)
    elif report == 'file':
        dp.gen_report(algo_name, arch, scale, q, reln, limit, reps, eps, pre_eps, run_eps, nosy_ans, gsens, bias_ans, true_ans, nosy_ans_list, bias_ans_list, gsens_list)
        pass

def run_TSensDP(arch, scale, q, reln, limit, eps, report='print', reps=10):
    run_DP('TSensDP', arch, scale, q, reln, limit, eps, report, reps)

def run_PrivateSQL(arch, scale, q, reln, limit, eps, report='print', reps=10):
    run_DP('PrivateSQL', arch, scale, q, reln, limit, eps, report, reps)
