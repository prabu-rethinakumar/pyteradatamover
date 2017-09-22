'''
This is a Data copier script which loads a flat file into Teradata table
Prabu Rethinakumar 09/14/2017

'''
#
# Notes:
#  - will probably insert duplicate records if you load the same file twice
#  - assumes that the number of fields in the header row is the same
#    as the number of columns in the rest of the file and in the database
#  - assumes the column order is the same in the file and in the database
#
# Speed: ~ 1s/MB
#

import sys
import teradata
import csv
from DataFormat import mapping

#Provide table name of your map , this will map Columns and their data types
map_tablename = 'xyz'
map_dbname = 'qa_abcd'

#Provide the table to be loaded
out_dbname = 'qa_abcd'
out_tablename = 'xyz_out'
out_username = 'dcba'
out_pass = "!%*&@!#)!"
out_system = 'xyz.lmn.com'
ckpt_freq = 5000
# This number represents load batch mode checkpoint value. Increasing it will reduce runtime but may result in
# failure. Make this value a trade off between performance and reliability

temp_file = "{}_tmp.txt".format(map_tablename)

def main(user, passw, db, table, csvfile):
    try:
        udaExec = teradata.UdaExec(appName="datacopier", version="1.0", logConsole=False)
        session = udaExec.connect(driver='Teradata', method="odbc", system=out_system, charset='UTF8',
                                  username=user,
                                  password=passw)
    except:
        print("Error in DB connection")
        sys.exit(1)
    numfields = nums(map_tablename, map_dbname)
    loadcsv(numfields, session, db, table, csvfile)

def nullify(L):
    """Convert empty strings in the given list to None."""
    def f(x):
        if x == "NULL":
            return None
        else:
            return x

    return [f(x) for x in L]


def nums(out_tablename, out_dbname):
    dataType = mapping(out_tablename, out_dbname, out_username, out_pass, out_system)
    nums = len(dataType)
    return nums


def loadcsv(numfields, session, db, table, filename):
    f = csv.reader(open(filename), delimiter='|')
    query = buildInsertCmd(db, table, numfields)
    global i
    global count
    global ins_flg
    i = 0
    count = 0
    row_arr = []
    ins_flg = 'N'
    total = 0
    for line in f:
        vals = nullify(line)
        row_arr.insert(i, vals)
        count += 1
        i += 1
        total += 1
        if count == ckpt_freq:
            session.executemany(query, row_arr, batch=True)
            count = 0
            row_arr = []
            ins_flg = 'Y'
            print("inserted {} records".format(total))

    if len(row_arr) > 0 and count < ckpt_freq:
            session.executemany(query, row_arr, batch=True)
            print("inserted {} records".format(total))
    return


def buildInsertCmd(db, table, numfields):
    assert (numfields > 0)
    placeholders = (numfields - 1) * "?, " + "?"
    query = ("insert into %s.%s" % (db, table)) + (" values (%s)" % placeholders)
    print("Query is", query)
    return query

main(out_username, out_pass, out_dbname, out_tablename, temp_file)
