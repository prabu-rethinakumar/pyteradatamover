''' This Script reads from Teradata table and writes into file
Usage:
    Best suited for Lower Life cycle Data setup

Inputs:
    Db Name,
    Credentials
    Query
    Location where file should be saved

**Prabu Rethinakumar 09/14/2017
'''

import teradata
from DataFormat import mapping

in_tablename = 'xyz'
in_dbname = 'abcd'
in_username = 'dejifn'
in_pass = '^@*#@^)'
in_system = 'abcd.xyz.com'
#query = "select * from {}.{} ".format(in_dbname, in_tablename)
query = "select * from abcd.xyz"

# Output filename with Data - will be required for Loading
temp_file = "{}_tmp.txt".format(in_tablename)
file_location = "C:\\Users\abcd\Desktop\Python works\TD Setup\{}".format(temp_file)


# Initializing components
dataType = mapping(in_tablename, in_dbname, in_username, in_pass, in_system)
row_array = []


class DataCopy:

    def init(self):
        self.i = 0
        self.row_array = []
        self.length = 0
        self.row_length = 0

    def loaddata(self, filename):
        i = 0
        for line in filename:
            mod_line = line.rstrip('\n')
            dat1 = mod_line.split(',')
            print('dat1 is', dat1)
            row_array.insert(i, dat1)
            i += 1
        print('row_array is', len(row_array))
        return row_array

    def converter(self, typ, value):
        if value is None:
            if typ == 'int':
                return "NULL"
            if typ == 'str':
                return "NULL"
            if typ == 'date':
                return "NULL"
            if typ == 'timestamp':
                return "NULL"
            if typ == 'float':
                return "NULL"
        else:
            if typ == 'int':
                return int(value)
            if typ == 'str':
                try:
                    int(value)
                    return value
                except:
                    val = str(value)
                    return val
            if typ == 'float':
                return float(value)
            if typ == 'date':
                #val = value.strftime("%Y%m%d")
                val = value.strftime("%Y-%m-%d")
                return val
            if typ == 'timestamp':
                val = value.strftime("%Y-%m-%d %H:%M:%S.%f")
                return val

    def executor(self, query, temp_file, file_location, in_username, in_pass):
        converter = DataCopy()
        f = open(file_location, mode='w+')
        udaExec = teradata.UdaExec(appName="abcd", version="1.0", logConsole=True)
        session = udaExec.connect(driver='Teradata', method="odbc", system=in_system, charset='UTF8', username=in_username, password=in_pass)
        for row in session.execute(query):
            row_length = len(row)
            phrase = ''
            for i in range(0, row_length, 1):
                if i == 0:
                    dat = converter.converter(dataType[i], row[i])
                    phrase = str(dat)
                else:
                    dat = converter.converter(dataType[i], row[i])
                    phrase = phrase + '|' + str(dat)
            f.write(phrase)
            f.write('\n')
        f.close()

        return temp_file

objA = DataCopy()
fname = objA.executor(query, temp_file, file_location, in_username, in_pass)




