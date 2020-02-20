#!/usr/bin/env python
import pandas as pd
import psycopg2 as pg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import numpy as np
import sys
import extract_table
import query_schema_template as qst

plat = sys.argv[1]
mode = 'circle' if len(sys.argv) == 2 else sys.argv[2] 

print('<.py> <plat> <mode> \n mode: \n\t\t "circle" (default) \n\t\t "combcircle: combine multiple circles in the same ego-network"')

arch = 'snap_%s'%plat
qprefix = '4path'
query_folder = '../../../queries/snap_%s/'%plat
schema_folder = '../../../schema/snap_%s/'%plat
query_metadata_folder = '../../../data/snap/%s/query_metadata/'%plat

database = 'arch-snap_%s-scale-1'%plat
conn = pg2.connect("host='localhost'  port=5422 user='duke' password='duke' dbname=%s"%database)
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cursor = conn.cursor()

def get_index(s):
    import hashlib
    m = hashlib.md5()
    m.update(s.encode('utf-8'))
    return m.hexdigest()[:6]

class CombCircle():
    def __init__(self):
        self.ego_network = input('Input the ego-network number. Default is 348') or '348'
        self.source = self.ego_network
        self.n = 4 # number of base tables
        self.edge_cnt_df = self.extract_edge_count()
        self.data_list = self.get_data_list(self.edge_cnt_df)
        
        self.extract_base_tables()
        self.extract_tri_tables()
        self.generate_queries()

    # Get the edge count for each source-cirlce
    def extract_edge_count(self):
        sql = 'SELECT circle, count(*) as edge_cnt FROM same_circle_edges WHERE source = \'%s\' GROUP BY circle ORDER BY edge_cnt DESC'%self.source
        df = pd.read_sql(sql, conn)
        return df

    # Get the meta data about each circle group
    def get_data_list(self, edge_cnt_df):
        df = edge_cnt_df
        df = df.sort_values(by='edge_cnt', ascending=False)
        tables = [[] for i in range(self.n)]
        data_list = []
        i = 0
        for index, row in df.iterrows():
            tables[i].append(row['circle'])
            i = (i + 1) % self.n
        for i in range(self.n):
            circles = tables[i]
            data = {
                'circles':  circles,
                'circle_name':  ','.join(circles),
                'source':   self.source
            }
            data['circle_index'] = get_index(data['circle_name'])
            data_list.append(data)
        return data_list
          
    # Get 4 base tables():
    def extract_base_tables(self):       
        for data in self.data_list:
            table_name = 'source_%s_circle_%s'%(data['source'], data['circle_index'])
            data['base_name'] = table_name
            cursor.execute('DROP TABLE IF EXISTS %s'%(table_name))
            cursor.execute('CREATE TABLE %s (fnode varchar(30), tnode varchar(30))'%(table_name))
            sql = '''INSERT INTO {table} select fnode, tnode from same_circle_edges where source='{source}' and circle in ({circle})
            '''.format(table=table_name, source=self.source, circle=','.join("'%s'"%cir for cir in data['circles']))
            cursor.execute(sql)

    # Get 4 triangle tables():
    def extract_tri_tables(self):       
        for data in self.data_list:
            table_name = 'tri_source_%s_circle_%s'%(data['source'], data['circle_index'])
            data['tri_name'] = table_name
            cursor.execute('DROP TABLE IF EXISTS %s'%(table_name))
            cursor.execute('CREATE TABLE {table} (node1 varchar(30), node2 varchar(30), node3 varchar(30))'.format(table=table_name))
            sql = '''INSERT INTO {table} 
                select e1.fnode as node1, e2.fnode as node2, e3.fnode as node3
                from {t} as e1, {t} as e2, {t} as e3 
                where e1.tnode = e2.fnode AND e2.tnode = e3.fnode AND e3.tnode = e1.fnode
            '''.format(table=table_name, t=data['base_name'])
            cursor.execute(sql)
                
    # Generate queries
    def generate_queries(self):
        data_list = self.data_list
        info = {
            'circle1': self.data_list[0]['circle_index'],
            'circle2': self.data_list[1]['circle_index'],
            'circle3': self.data_list[2]['circle_index'],
            'circle4': self.data_list[3]['circle_index'],
            'circle_tri': self.data_list[3]['circle_index'],
            'source': self.source
        }
        for qtype in ['triangle', '4path', '4cycle', '4star']:
            info['qtype'] = qtype
            info['qfullname'] = 'q_{qtype}_{source}_{circle1}_{circle2}_{circle3}_{circle4}'.format(**info)
            info['index'] = get_index(info['qfullname'])
            info['qname'] = 'q_{qtype}_{index}'.format(**info)
            
            query = qst.queries[qtype].format(**info)
            schema = qst.schemas[qtype].format(**info)
            qfile = open(query_folder + info['qname'] +'.hypertree', 'w')
            sfile = open(schema_folder + info['qname'] +'.schema', 'w')
            qfile.write(query)
            sfile.write(schema)
            qfile.close()
            sfile.close()
            
if __name__ == '__main__':
    if mode == 'combcircle':
        cc = CombCircle()
    else:
        print('no support')
            
                
                
                
                


                
            