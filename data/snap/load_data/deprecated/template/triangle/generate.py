triangles = None

def init(plat, threshold):
    # May support different level of triangles ... maybe separate it as a different query type would be better
    database = 'arch-snap_%s-scale-1'%plat
    conn = pg2.connect("host='localhost'  port=5422 user='duke' password='duke' dbname=%s"%database)

    _triangles = pd.read_sql(sql, conn)
    triangles = _triangles.query('cnt > %s'%threshold)

def next_query():
    global triangles
    
    for _, triangle in triangles.iterrows():
        triangle = triangle.to_dict()
        for i in [1,2,3]:
            triangle['table%d'%i] = 'source_%d_circle_%d'%(triangle['source'], triangle['circle%d'%i])
        triangle['qname'] = 'q{source}_{circle1}_{circle2}_{circle3}_{cnt}'.format(**triangle)
        yield triangle
    
def next_table():
    global triangles
    
    tables = set()
    for _, triangle in triangles.iterrows():
        for i in [1,2,3]:
            tables.add((triangle['source'],  triangle['circle%d'%i]))
            
    for table in tables

    
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



# Count how many triangles such that all individuals share the common friend, and each of edge from different circle
# Output:
# source, circle 1, circle 2, circle 3, triangles
sql = '''
WITH full_info AS (
    SELECT
        edges.source, fnode, c1.circle as fcircle, tnode, c2.circle as tcircle
    FROM
        edges, circles c1, circles c2
    where edges.source = c1.source AND edges.source = c2.source AND edges.fnode = c1.node AND edges.tnode = c2.node
        AND c1.circle = c2.circle
)
select
    e1.source as source, e1.fcircle as circle1, e2.fcircle as circle2, e3.fcircle as circle3, count(*) as cnt
from
    full_info e1, full_info e2, full_info e3
WHERE
    True
    AND e1.fcircle < e2.fcircle
    AND e2.fcircle < e3.fcircle
    AND e1.source = e2.source
    AND e2.source = e3.source
    AND e1.tnode = e2.fnode AND e2.tnode = e3.fnode AND e3.tnode = e1.fnode
GROUP BY
    e1.source, e1.fcircle, e2.fcircle, e3.fcircle
;
'''