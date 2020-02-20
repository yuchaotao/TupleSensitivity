#!/usr/bin/env python
import pandas as pd
import sys
import extract_table

plat = sys.argv[1]
arch = 'snap_%s'%plat
top_triangles_csv = '../%s/top_triangles.csv'%plat
top_triangles = pd.read_csv(top_triangles_csv)
query_folder = '../../../queries/snap_%s/'%plat
schema_folder = '../../../schema/snap_%s/'%plat

def main():
    tables = set()
    for _, triangle in top_triangles.iterrows():
        for i in [1,2,3]:
            tables.add((triangle['source'],  triangle['circle%d'%i]))
        add_query(triangle.to_dict())
    add_tables(tables)

def add_schema(tables):
    global schema_template
    i = 1
    schema = schema_template
    for table in tables:
        source, circle = table
        table_name = 'source_{source}_circle_{circle}'.format(source=source, circle=circle)
        schema += 'r {i} {table}\na {i} X fnode\na {i} Y tnode\nc\n'.format(i=i, table=table_name)
        i += 1
    sfile = open(schema_folder + 'snap_%s.schema'%plat, 'w')
    sfile.write(schema)
    sfile.close()

def add_query(triangle):
    triangle['qname'] = 'q{source}_{circle1}_{circle2}_{circle3}_{cnt}'.format(**triangle)
    query = query_template.format(**triangle)
    schema = schema_template.format(**triangle)
    qfile = open(query_folder + triangle['qname'] +'.hypertree', 'w')
    sfile = open(schema_folder + triangle['qname'] +'.schema', 'w')
    qfile.write(query)
    sfile.write(schema)
    qfile.close()
    sfile.close()

def add_tables(tables):
    for table in tables:
        source, circle = table
        extract_table.main(plat, source, circle)

if __name__ == '__main__':
    main()
