#!/usr/bin/env python3

import psycopg2 as pg2
from algo import *
from objects import *

def _Relation(index, name, attributes: List[str]):
    attributes = set(Attribute(0, attr, attr) for attr in attributes)
    return Relation(index, name, attributes)

def use_db(arch: str, scale: str):
    global conn
    dbname = arch + "_" + scale
    conn = pg2.connect(dbname=dbname, user='yuchao', password='', port='5422')

def gen_arch_hw2td3():
    R1 = _Relation(1, 'R1', ['A', 'B'])
    R2 = _Relation(2, 'R2', ['B', 'C'])
    R3 = _Relation(3, 'R3', ['C'])
    R4 = _Relation(4, 'R4', ['A'])
    R5 = _Relation(5, 'R5', ['B'])
    R6 = _Relation(6, 'R6', ['B'])
    relations = [R1, R2, R3, R4, R5, R6]

    N1 = Node(1, 'N1', [R1, R6])
    N2 = Node(2, 'N2', [R2])
    N3 = Node(3, 'N3', [R3])
    N4 = Node(4, 'N4', [R4])
    N5 = Node(5, 'N5', [R5])
    nodes = [N1, N2, N3, N4, N5]

    N1.parent = N2
    N3.parent = N2
    N4.parent = N1
    N5.parent = N1

    N2.children = [N1, N3]
    N1.children = [N4, N5]

    T = Tree('T', nodes)
    return T, nodes, relations

def gen_relations():
    R1 = _Relation(1, 'R1', ['A'])
    R2 = _Relation(2, 'R2', ['A', 'C'])
    R3 = _Relation(3, 'R3', ['A', 'B'])
    R4 = _Relation(4, 'R4', ['B', 'C'])
    R5 = _Relation(5, 'R5', ['C', 'D'])
    R6 = _Relation(6, 'R6', ['A', 'B', 'D'])
    R7 = _Relation(7, 'R7', ['C'])
    R8 = _Relation(8, 'R8', ['A', 'D'])
    R9 = _Relation(9, 'R9', ['D'])
    relations = [R1, R2, R3, R4, R5, R6, R7, R8, R9]
    return relations

def gen_arch_hw1td2():
    relations = gen_relations()
    R1, R2, R3, R4, R5, R6, R7, R8, R9 = relations
    relations = [R1, R3, R4, R5, R9]

    N1 = Node(1, 'N1', [R1])
    N2 = Node(2, 'N2', [R3])
    N3 = Node(3, 'N3', [R4])
    N4 = Node(4, 'N4', [R5])
    N5 = Node(5, 'N5', [R9])
    nodes = [N1, N2, N3, N4, N5]

    N2.parent = N1
    N3.parent = N2
    N4.parent = N3
    N5.parent = N4

    N1.children = [N2]
    N2.children = [N3]
    N3.children = [N4]
    N4.children = [N5]

    T = Tree(nodes)
    return T, nodes, relations

def gen_arch_hw1td3():
    relations = gen_relations()
    R1, R2, R3, R4, R5, R6, R7, R8, R9 = relations
    relations = [R1, R3, R4, R7, R5, R9]

    N1 = Node(1, 'N1', [R1])
    N2 = Node(2, 'N2', [R3])
    N3 = Node(3, 'N3', [R4])
    N4 = Node(4, 'N4', [R7])
    N5 = Node(5, 'N5', [R5])
    N6 = Node(6, 'N6', [R9])
    nodes = [N1, N2, N3, N4, N5, N6]

    N1.parent = N2
    N3.parent = N2
    N4.parent = N3
    N5.parent = N3
    N6.parent = N5

    N2.children = [N1, N3]
    N3.children = [N4, N5]
    N5.children = [N6]

    T = Tree(nodes)
    return T, nodes, relations

def gen_arch_hw2td2():
    relations = gen_relations()
    R1, R2, R3, R4, R5, R6, R7, R8, R9 = relations
    relations = [R1, R3, R4, R5, R6, R8]

    N1 = Node(1, 'N1', [R1])
    N2 = Node(2, 'N2', [R3, R4])
    N3 = Node(3, 'N3', [R5, R6])
    N4 = Node(4, 'N4', [R8])
    nodes = [N1, N2, N3, N4]

    N1.parent = N2
    N3.parent = N2
    N4.parent = N3

    N2.children = [N1, N3]
    N3.children = [N4]

    T = Tree('T', nodes)
    return T, nodes, relations

def gen_arch_hw3td3():
    relations = gen_relations()
    R1, R2, R3, R4, R5, R6, R7, R8, R9 = relations
    relations = [R1, R2, R3, R4, R5, R6, R7, R8]

    N1 = Node(1, 'N1', [R1])
    N2 = Node(2, 'N2', [R2, R3, R4])
    N3 = Node(3, 'N3', [R5, R6])
    N4 = Node(4, 'N4', [R7])
    N5 = Node(5, 'N5', [R8])
    nodes = [N1, N2, N3, N4, N5]

    N1.parent = N2
    N3.parent = N2
    N4.parent = N3
    N5.parent = N3


    N2.children = [N1, N3]
    N3.children = [N4, N5]

    T = Tree(nodes)
    return T, nodes, relations

def test_toy():
    arch = 'hw2td3'
    scale = 'toy'
    test_correctness(arch, scale)

def test():
    gen_report_title()
    for arch in ['hw1td2', 'hw1td3', 'hw2td2', 'hw3td3']:
         for scale in ['small', 'medium', 'large']:
             test_correctness(arch, scale)

def test_correctness(arch, scale):
    global conn
    use_db(arch, scale)
    T, nodes, relations = globals()['gen_arch_'+arch]()

    tstar, local_tstar_list, elapsed = run_algo(T, conn)

    test_pass = 'Unknown'
    if scale in ['small', 'toy'] or (arch == 'tpch' and scale in ['0.0001', '0.01']):
        try:
            test_ground(relations)
        except:
            test_pass = 'Failed'
        else:
            test_pass = 'Succeeded'

    print_humanreadable_report(arch, scale, arch, tstar, local_tstar_list, elapsed, test_pass)
    #gen_report(arch, scale, arch, tstar, local_tstar_list, elapsed, test_pass)

if __name__ == '__main__':
    #test()
    test_toy()
