def generate_column_description(column_info):
    row_description = '"%s" %s' % (column_info['name'], column_info['type'])

    if column_info['notnull']:
        row_description = "%s NOT NULL" % row_description

    if column_info['primarykey'] == 't':
        row_description = "%s PRIMARY KEY" % row_description

    if column_info['uniquekey'] == 't':
        row_description = "%s UNIQUE"
    return row_description


def generate_create_table_for(user,password,db,schema,table,target_table,type=None,host="localhost",port=5432):
    import psycopg2

    table_specification_sql = open("identify_table_info.sql","r").read()
    create_table_sql = ""

    with psycopg2.connect(host=host, port=port, user=user, password=password, database=db) as conn:
        cursor = conn.cursor()
        cursor.execute(table_specification_sql, (schema,table))
        colnames = [desc[0] for desc in cursor.description]
        row = cursor.fetchone()
        rows = []    
        while row:
            rows.append(dict(zip(colnames, row)))
            row = cursor.fetchone()


        create_table_sql = "CREATE TABLE %s.%s (" % (schema, target_table) + ','.join(map(lambda x: generate_column_description(x), rows)) + ");"
           

    return create_table_sql
