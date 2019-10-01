import psycopg2 as pg2

import sys

def run_sql(sql):
    print(sql)
    cur = conn.cursor()
    cur.execute(sql)
    cur.close()

conn = pg2.connect(dbname='yuchao', user='yuchao', password='')
dbfile = sys.argv[1]

f = open(dbfile, 'r')
for line in f.readlines():
    line = line.strip()
    row = line.split(' ')
    flag = row[0]
    if flag == 'p':
        reln  = row[1]
        attrs = row[2:]
        sql   = 'DROP TABLE IF EXISTS %s;'%reln
        sql  += 'CREATE TABLE %s (%s);'%(reln, " ,".join(attr + " varchar(20)" for attr in attrs))
        run_sql(sql)
    elif flag == 'd':
        vals  = row[1:]
        sql   = 'INSERT INTO %s (%s) VALUES (%s)'%(reln, ' ,'.join(attrs), ' ,'.join("'%s'"%val for val in vals))
        run_sql(sql)

conn.commit()
conn.close()


