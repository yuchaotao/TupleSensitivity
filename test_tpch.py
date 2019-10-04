#!/usr/bin/python3

import algo
import import_hypertree
import psycopg2 as pg2
import time

conn = None

def use_db(arch, scale):
    global conn
    dbname = 'arch-{arch}-scale-{scale}'.format(arch=arch, scale=scale)
    conn = pg2.connect(dbname=dbname, host='localhost', user='duke', password='duke')

def _test(arch, scale, hypertree_file):
    global conn
    use_db(arch, scale)
    T, nodes, relations = import_hypertree.read_hypertree_from_file(hypertree_file)

    sql = 'select * from region limit 1'
    cur = conn.cursor()
    cur.execute(sql)
    r = cur.fetchone()
    print(r)

    tstar = algo.run_algo(T, conn)
    reln, tupl, sens = tstar

    algo.print_humanreadable_report(arch, scale, tstar, 0, 'Unknown')

def test():
    arch = 'tpch'
    scale = '0.1'
    hypertree_file = 'queries/tpch/q1.hypertree'
    _test(arch, scale, hypertree_file)

if __name__ == '__main__':
    test()
