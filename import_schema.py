from objects import Relation, Attribute, Schema

def read_schema_from_file(name, schema_file):
    relations = {}
    for line in open(schema_file, 'r').readlines():
        info = line.strip().split(' ')
        flag = info[0]
        if flag == 'c':
            pass
        elif flag == 'r':
            index   = info[1]
            name    = info[2]
            relation = Relation(index, name, set())
            relation.attributes = {}
            relations[index] = relation
        elif flag == 'a':
            index = info[1]
            join_name = info[2]
            orig_name = info[3]
            attribute = Attribute(index, join_name, orig_name)
            relation.attributes[index] = attribute
        elif flag == 'e':
            parent = relations[info[1]]
            child  = relations[info[2]]
            indices = info[3:]
            parent.primarykey_targets.append((paraent.attributes[i], child) for i in indices)
            child.foreignkey_sources.append((child.attributes[i], parent) for i in indices)
    for reln in relations:
        reln.attribtues = reln.attributes.values()
    schema = Schema(name, relations.values())
    return schema
