#!/usr/bin/python3

import psycopg2 as pg2

from algo import Relation, use_db
from random import sample, choices

#DEBUG = True
DEBUG = False

R1 = Relation(1, 'R1', ['A'])
R2 = Relation(2, 'R2', ['A', 'C'])
R3 = Relation(3, 'R3', ['A', 'B'])
R4 = Relation(4, 'R4', ['B', 'C'])
R5 = Relation(5, 'R5', ['C', 'D'])
R6 = Relation(6, 'R6', ['A', 'B', 'D'])
R7 = Relation(7, 'R7', ['C'])
R8 = Relation(8, 'R8', ['A', 'D'])
R9 = Relation(9, 'R9', ['D'])
relations = [R1, R2, R3, R4, R5, R6, R7, R8, R9]

scope = 10
cover_rate = 0.8
n = 1000

def _create_db(dbname):
    global conn
    conn.autocommit = True
    sql = 'DROP DATABASE IF EXISTS ' + dbname
    cur = run_sql(sql)
    sql = 'CREATE DATABASE ' + dbname
    cur = run_sql(sql)
    conn.autocommit = False

def create_db(arch, scale):
    dbname = arch + '_' + scale
    _create_db(dbname)

def connect_db():
    global conn
    conn = pg2.connect(dbname='yuchao', user='yuchao', password='')

def use_db(arch: str, scale: str):
    global conn
    dbname = arch + "_" + scale
    conn = pg2.connect(dbname=dbname, user='yuchao', password='')

def create_table(relations):
    global conn
    for reln in relations:
        attrs = reln.attributes
        sql   = 'DROP TABLE IF EXISTS %s;'%reln.name
        sql  += 'CREATE TABLE %s (%s);'%(reln.name, " ,".join(attr + " varchar(20)" for attr in attrs))
        run_sql(sql)
    conn.commit()

def _gen_data(relations, n, scope, cover_rate):
    global conn
    for reln in relations:
        dprint(reln)
        rows = {}
        for attr in reln.attributes:
            cand = sample(range(1, scope+1), int(scope * cover_rate))
            rows[attr] = choices(cand, k=n)
        attr = rows.keys()
        vals = zip(*rows.values())
        for val in vals:
            sql = "INSERT INTO {reln} ({attr}) VALUES ({vals})"
            sql = sql.format(reln=reln.name, attr=','.join(attr), vals=','.join("'%s%s'"%(k,v) for k,v in zip(attr, val)))
            dprint(sql)
            run_sql(sql)
            dprint('')
    conn.commit()

def gen_data(arch, scale):
    connect_db()
    create_db(arch, scale)
    use_db(arch, scale)
    create_table(relations)
    if scale == 'small':
        n = 10
        scope = 5
        cover_rate = 1.0
    elif scale == 'medium':
        n = 1000
        scope = 100
        cover_rate = 0.8
    elif scale == 'large':
        n = 10000
        scope = 1000
        cover_rate = 0.5
    _gen_data(relations, n, scope, cover_rate)

def run_sql(sql):
    global conn
    cur = conn.cursor()
    cur.execute(sql)
    cur.close()

def dprint(s):
    if DEBUG:
        print(s)

def main():
    for arch in ['hw1td2', 'hw1td3', 'hw2td2', 'hw3td3']:
        for scale in ['small', 'medium', 'large']:
            print(arch + '_' + scale)
            gen_data(arch=arch, scale=scale)

if __name__ == '__main__':
    main()
    #gen_data(arch='hw1td2', scale='small')
