import psycopg2

import os

import sys

import threading


##################### This needs to changed based on what kind of table we want to sort. ##################

##################### To know how to change this, see Assignment 3 Instructions carefully #################

FIRST_TABLE_NAME = 'ratings'

SECOND_TABLE_NAME = 'ratings'

SORT_COLUMN_NAME_FIRST_TABLE = 'rating'

SORT_COLUMN_NAME_SECOND_TABLE = 'rating'

JOIN_COLUMN_NAME_FIRST_TABLE = 'userid'

JOIN_COLUMN_NAME_SECOND_TABLE = 'userid'

##########################################################################################################

def RangePartition(InputTable, SortingColumnName, openconnection):

    cur = openconnection.cursor()

    #cur.execute('DROP TABLE IF EXISTS ' + InputTable + 'RANGEPARTITION_INFO')

    #cur.execute('CREATE TABLE ' + InputTable + 'RANGEPARTITION_INFO(TableNum integer PRIMARY KEY, minm double precision, maxm double precision)')

    cur.execute("SELECT MIN(" + SortingColumnName + ") FROM " + InputTable)

    minm = cur.fetchall()[0][0]

    cur.execute("SELECT MAX(" + SortingColumnName + ") FROM " + InputTable)

    maxm = cur.fetchall()[0][0]

    rng = (float)(maxm - minm) / 5

    for partition in range(0, 5):

        maximum = minm + rng

        #if(maximum==maxm):

        #    maximum+=0.01;

        cur.execute("DROP TABLE IF EXISTS " + InputTable + "RANGE_PART" + str(partition+1))

        #cur.execute('INSERT INTO ' + InputTable + 'RANGEPARTITION_INFO VALUES(' + str(partition+1) + ',' + str(minm) + ',' + str(maximum) + ')')

        cur.execute("CREATE TABLE " + InputTable + "RANGE_PART" + str(partition+1) + " (LIKE " + InputTable +")")

        cur.execute("INSERT INTO " + InputTable + "RANGE_PART" + str(partition+1)+" SELECT * FROM " + InputTable + " WHERE " + SortingColumnName + ">=" + str(minm) + " AND " + SortingColumnName + "<" + str(maximum))

        minm = minm + rng

def ParallelSortThread(InputTable, SortingColumnName, nofthread, objectlist, openconnection):

    cur = openconnection.cursor()

    sort = "SELECT * FROM " + InputTable + "RANGE_PART" + str(nofthread) + " ORDER BY " + SortingColumnName

    cur.execute(sort)

    rows = cur.fetchall()

    objectlist.append(rows)

    cur.execute("DROP TABLE IF EXISTS " + InputTable + "RANGE_PART" + str(nofthread))

def mergeParallelSort(OutputTable,openconnection,list1,list2,list3,list4,list5):


    cur = openconnection.cursor()


    listsort= [list1, list2, list3, list4, list5]


    for value in listsort:

        for listnum in value:

            if str(listnum)!="[]":

                num = str(listnum).lstrip('[').rstrip(']')

                sortq = "INSERT INTO " + OutputTable + " VALUES " + num

                cur.execute(sortq)


def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):

    cur = openconnection.cursor()

    #con = getOpenConnection(dbname='postgres')

    RangePartition(InputTable, SortingColumnName, openconnection);

    cur.execute("DROP TABLE IF EXISTS " + OutputTable)

    cur.execute("CREATE TABLE " + OutputTable + " AS SELECT * FROM " + InputTable + " WHERE 1=2")

    list1 = []

    list2 = []

    list3 = []

    list4 = []

    list5 = []

    thread1 = threading.Thread(target = ParallelSortThread, args = (InputTable, SortingColumnName,'1',list1,openconnection))

    thread2 = threading.Thread(target = ParallelSortThread, args = (InputTable, SortingColumnName,'2',list2,openconnection))

    thread3 = threading.Thread(target = ParallelSortThread, args = (InputTable, SortingColumnName,'3',list3,openconnection))

    thread4 = threading.Thread(target = ParallelSortThread, args = (InputTable, SortingColumnName,'4',list4,openconnection))

    thread5 = threading.Thread(target = ParallelSortThread, args = (InputTable, SortingColumnName,'5',list5,openconnection))

    thread1.start()

    thread2.start()

    thread3.start()

    thread4.start()

    thread5.start()

    thread1.join()

    thread2.join()

    thread3.join()

    thread4.join()

    thread5.join()

    mergeParallelSort(OutputTable,openconnection,list1,list2,list3,list4,list5);

    #cur.execute('DROP TABLE IF EXISTS ' + InputTable + 'RANGEPARTITION_INFO')

################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='pks', password='pks', dbname='pks'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='pks'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='pks')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            conn.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" %(ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d`+",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            conn.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

if __name__ == '__main__':
    try:
        # Creating Database ddsassignment3
        print "Creating Database named as ddsassignment3"
        createDB();
        
        # Getting connection to the database
        print "Getting connection from the ddsassignment3 database"
        con = getOpenConnection();

        # Calling ParallelSort
        print "Performing Parallel Sort"
        ParallelSort(FIRST_TABLE_NAME, SORT_COLUMN_NAME_FIRST_TABLE, 'parallelSortOutputTable', con);

        # Calling ParallelJoin
        #print "Performing Parallel Join"
        #ParallelJoin(FIRST_TABLE_NAME, SECOND_TABLE_NAME, JOIN_COLUMN_NAME_FIRST_TABLE, JOIN_COLUMN_NAME_SECOND_TABLE, 'parallelJoinOutputTable', con);
        
        # Saving parallelSortOutputTable and parallelJoinOutputTable on two files
        saveTable('parallelSortOutputTable', 'parallelSortOutputTable.txt', con);
        #saveTable('parallelJoinOutputTable', 'parallelJoinOutputTable.txt', con);

        # Deleting parallelSortOutputTable and parallelJoinOutputTable
        deleteTables('parallelSortOutputTable', con);
        #       deleteTables('parallelJoinOutputTable', con);

        if con:
            con.close()

    except Exception as detail:
        print "Something bad has happened!!! This is the error ==> ", detail
