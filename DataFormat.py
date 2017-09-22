'''Mapper Script converts Teradata Data Types to Python acceptable Data Types and Vice Versa.
Please add new Types to mapper array as you encounter them or run into issues

***Prabu Rethinakumar 09/14/2017
'''
import teradata

def mapping(in_tablename, in_dbname, in_username, in_pass, in_system):
    mapper = {'I' : 'int', 'DA' : 'date', 'I1' : 'int', 'CV' : 'str', 'CF' : 'str','D' : 'float', 'TS' : 'timestamp', 'I2': 'int'}
    query = "select columntype  from {}.{} where tablename = '{}' and databasename = '{}' order by columnid".format('dbc', 'columnsV', in_tablename, in_dbname)
    dataType = []
    udaExec = teradata.UdaExec(appName="abcd", version="1.0", logConsole=True)
    session = udaExec.connect(driver='Teradata', method="odbc", system=in_system, username=in_username, password=in_pass)
    for row in session.execute(query):
            text = str(row[0]).rstrip()
            data = mapper[text]
            dataType.append(data)
    print(dataType)
    return dataType


