#!/usr/bin/env python
import load_util
import psycopg2 as pg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import sys

def create_group_edge_table(conn, source, circle):
    table_name = 'source_%s_circle_%s'%(source, circle)
    cursor = conn.cursor()
    sql = 'DROP TABLE IF EXISTS {table} CASCADE;'.format(table=table_name)
    print('dropping table %s'%table_name)
    cursor.execute(sql)
    print('dropped table %s'%table_name)
    sql = 'CREATE TABLE {table} (fnode varchar(30), tnode varchar(30))'.format(table=table_name)
    cursor.execute(sql)
    conn.commit()
    
def create_group_triangle_table(conn, source, circle):
    table_name = 'tri_source_%s_circle_%s'%(source, circle)
    cursor = conn.cursor()
    sql = 'DROP TABLE IF EXISTS {table} CASCADE;'.format(table=table_name)
    print('dropping table %s'%table_name)
    cursor.execute(sql)
    print('dropped table %s'%table_name)
    sql = 'CREATE TABLE {table} (node1 varchar(30), node2 varchar(30), node3 varchar(30))'.format(table=table_name)
    cursor.execute(sql)
    conn.commit()

def insert_group_edge_table(conn, source, circle):
    table_name = 'source_%s_circle_%s'%(source, circle)
    sql = '''INSERT INTO {table} select fnode, tnode from same_circle_edges where source='{source}' and circle = '{circle}'
    '''.format(table=table_name, source=source, circle=circle)
    #print(sql)
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    
def insert_group_triangle_table(conn, source, circle):
    table_name = 'tri_source_%s_circle_%s'%(source, circle)
    sql = '''INSERT INTO {table} select node1, node2, node3 from same_circle_triangles where source='{source}' and circle = '{circle}'
    '''.format(table=table_name, source=source, circle=circle)
    #print(sql)
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()

def extract_edges(plat, source, circle):
    # Get the target database name
    arch = 'snap_%s'%plat
    scale = '1'
    database = 'arch-%s-scale-%s'%(arch, scale)

    # Read from files and load into the target database
    conn = pg2.connect('user=duke host=localhost password=duke port=5422 dbname=%s'%database)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    create_group_edge_table(conn, source, circle)
    insert_group_edge_table(conn, source, circle)

def extract_triangles(plat, source, circle):
    # Get the target database name
    arch = 'snap_%s'%plat
    scale = '1'
    database = 'arch-%s-scale-%s'%(arch, scale)

    # Read from files and load into the target database
    conn = pg2.connect('user=duke host=localhost password=duke port=5422 dbname=%s'%database)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    create_group_triangle_table(conn, source, circle)
    insert_group_triangle_table(conn, source, circle)
    
if __name__ == '__main__':
    plat = sys.argv[1]
    source = sys.argv[2]
    circle = sys.argv[3]
    main(plat, source, circle)

'''
    WITH full_info AS (
        SELECT
            edges.source, fnode, c1.circle as fcircle, tnode, c2.circle as tcircle
        FROM
            (SELECT * FROM edges WHERE source={source}) as edges,
            (SELECT * FROM circles WHERE source={source} AND circle={circle}) as c1,
            (SELECT * FROM circles WHERE source={source} AND circle={circle}) as c2
        where edges.fnode = c1.node AND edges.tnode = c2.node
    )
'''
