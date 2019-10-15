import psycopg2 as pg2
import psycopg2.extras

def use_db(arch, scale):
    global conn
    dbname = 'arch-{arch}-scale-{scale}'.format(arch=arch, scale=scale)
    conn = pg2.connect(dbname=dbname, host='localhost', user='duke', password='duke', port='5422')
    return conn

def run_sql(sql, conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(sql)

    conn.commit()
    return cur

def run_sql_silent(sql, conn):
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(sql)

        conn.commit()
        return cur
    except:
        conn.rollback()
        return None
