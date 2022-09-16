import pyodbc

driver="{SQL Server Native Client 11.0};"
server= "DESKTOP-NME4VL2\SQLEXPRESS;"
database = "SANTU_DB"                       #Create database and mention here

def sqlserver_connect():
    con = pyodbc.connect(driver=driver,host=server,database=database,trusted_Connection="yes;")
    cur = con.cursor()
    return cur ,con,database
