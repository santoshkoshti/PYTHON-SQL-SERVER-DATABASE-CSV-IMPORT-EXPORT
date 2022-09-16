import csv
from SQL_SERVER_CONFIG import sqlserver_connect

def get_all_tables(cur,db_name):
    query = 'select table_name from {}.information_schema.tables;'.format(db_name)
    get_tables = cur.execute(query)
    if get_tables.rowcount == 0:
        print("no tables found in {} database".format(db_name))
    else:
        for i in get_tables.fetchall():
            sqlserver_to_csv(cur,i[0])

def sqlserver_to_csv(cur,table_name):
    results = cur.execute('SELECT * FROM {}'.format(table_name))
    with open("{}.csv".format(table_name), "w", encoding='utf-8') as outputfile:
        writer = csv.writer(outputfile, lineterminator="\n")
        writer.writerow(i[0] for i in results.description)
        writer.writerows(results)
        print("{}.csv file created..".format(table_name))

if __name__ == '__main__':
    cur, con,db_name = sqlserver_connect()
    get_all_tables(cur,db_name)