'''
Table Schema:
    edges:
        source  fnode   tnode
    circles:
        source  circle  node
    combined:
        fnode tnode
'''

def create_circles(conn):
    cursor = conn.cursor()
    sql = 'CREATE TABLE circles (source varchar(30), circle varchar(30), node varchar(30))'
    cursor.execute(sql)
    conn.commit()

def create_edges(conn):
    cursor = conn.cursor()
    sql = 'CREATE TABLE edges (source varchar(30), fnode varchar(30), tnode varchar(30))'
    cursor.execute(sql)
    conn.commit()

def create_combined(conn):
    cursor = conn.cursor()
    sql = 'CREATE TABLE IF NOT EXISTS combined (fnode varchar(30), tnode varchar(30))'
    cursor.execute(sql)
    conn.commit()

def insert_circles(dirname, source, conn):
    cursor = conn.cursor()
    filename = '%s/%s.circles'%(dirname, source)
    f = open(filename, 'r')
    for l in f.readlines():
        a = l.strip().split('\t')
        circle = a[0]
        for node in a[1:]:
            sql = 'INSERT INTO circles (source, circle, node) VALUES (%s, %s, %s)'
            cursor.execute(sql, (source, circle, node))
    conn.commit()

def insert_edges(dirname, source, conn):
    cursor = conn.cursor()
    filename = '%s/%s.edges'%(dirname, source)
    f = open(filename, 'r')
    for l in f.readlines():
        fnode, tnode = l.strip().split(' ')
        sql = 'INSERT INTO edges (source, fnode, tnode) VALUES (%s, %s, %s)'
        cursor.execute(sql, (source, fnode, tnode))
    conn.commit()

def insert_combined(filename, conn):
    cursor = conn.cursor()
    f = open(filename, 'r')
    for l in f.readlines():
        fnode, tnode = l.strip().split(' ')
        sql = 'INSERT INTO combined (fnode, tnode) VALUES (%s, %s)'
        cursor.execute(sql, (fnode, tnode))
    conn.commit()

def retrieve_source_set(dirname):
    import os
    source_set = set()
    for filename in os.listdir(dirname):
        source = filename.split('.')[0]
        source_set.add(source)
    return source_set
