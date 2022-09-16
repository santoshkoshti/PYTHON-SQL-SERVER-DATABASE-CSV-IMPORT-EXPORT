import ntpath
import numpy as np
import pandas as pd
import re
import os
import pyodbc
from datetime import datetime
from dateutil import parser
from SQL_SERVER_CONFIG import sqlserver_connect

csv_file_path = r"D:\CSV_FILES\HRARecords.csv"     # MENTION CSV FILE PATH
head, tail = ntpath.split(csv_file_path)
table_name = tail.replace(".csv","").replace(" ","_")

def extract_file_data(path,table_name,cur,con,db_name):
    data = pd.read_csv(path)
    data = data.replace(to_replace=np.nan, value='-')
    df = pd.DataFrame(data)
    csv_data_insert_sqlserver(table_name, df,cur,con,db_name)

def get_columns_csv(df):
    query = []
    for name, dtype in df.dtypes.iteritems():
        try:
            varch_len = df[name].apply(len).max()
        except:
            varch_len = 100
        col = re.sub('[-,.\'~`() "?!@#$%+|^&*<>/{}[\]]', '', name)
        if object == dtype:
            if 'date' in str(col).lower():
                db_col = col + ' DATE'
                query.append(db_col)
            else:
                db_col = col + ' VARCHAR({})'.format(varch_len)
                query.append(db_col)
        else:
            db_col = col + ' INT'
            query.append(db_col)
    return query

def create_table_sqlserver(tb_name,query,cur,con,db_name):
    cur.execute("USE {}".format(db_name))
    make_query = 'CREATE TABLE {}('.format(tb_name) + ",".join(query) + ')'
    try:
        cur.execute(make_query)
        con.commit()
        return "table {} created".format(tb_name)
    except pyodbc.ProgrammingError as er:
        if 'There is already' in str(er):
            return 'DataBase Already exits..'
        else:
            return "something wrong in query or connection"


def is_date_matching(dt_date):
    if dt_date:
        try:
            int(dt_date)
            return False
        except:
            try:
                parser.parse((dt_date))
                return True
            except:
                return False
    return False

def csv_data_insert_sqlserver(tb_name,df,cur,con,db_name):
    op_create_table = create_table_sqlserver(tb_name, get_columns_csv(df), cur,con,db_name)  # replace oracle table name in place of santu
    print(op_create_table)
    count = 1
    for row in df.itertuples():
        query1 =''
        for i in row:
            if i=='-':
                query1 +=',null'
            elif type(i)==int or type(i)==float:
                query1 += ',{}'.format(i)
            elif is_date_matching(i)==True:
                if ":" in i:
                    query1 += ",'{}'".format(i)
                elif any(num.isdigit() for num in i)==False:
                    query1 += ",'{}'".format(i)
                else:
                    h = parser.parse(i)
                    i = datetime.strptime(str(h),'%Y-%m-%d  %H:%M:%S').strftime('%m/%d/%Y')
                    query1 += ",'{}'".format(i)
            else:
                i = i.replace("'","")
                query1 += ",'{}'".format(i)
        query1= 'INSERT INTO {} VALUES('.format(tb_name) + query1.split(',', 2)[-1]+')'
        print(query1)
        cur.execute(query1)
        print("{} rows inserted".format(count))
        count += 1
    con.commit()
    con.close()
    print("all data loaded..")

if __name__ == '__main__':
    cur, con ,db_name = sqlserver_connect()
    extract_file_data(csv_file_path,table_name,cur,con,db_name)