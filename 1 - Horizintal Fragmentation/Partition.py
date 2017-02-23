#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='pks', password='pks', dbname='dds_assgn1'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratings, ratingsfilepath, conn):
    try:
        file = open(ratingsfilepath, 'r')


        #conn = psycopg2.connect("dbname='pks' user='pks' host='localhost' password='pks'")
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS "+ratings)
        create_table(conn)
        cur.copy_from(file, ratings, ':')
        cur.execute("ALTER TABLE "+ratings+" DROP COLUMN dummy1, DROP COLUMN dummy2, DROP COLUMN dummy3, DROP COLUMN dummy4;")
        
        file.close()
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print error
    # finally:
    #     if conn is not None:
    #         conn.close()


def create_table(conn):
    try:
        #conn = psycopg2.connect("dbname='pks' user='pks' host='localhost' password='pks'")
        cur = conn.cursor()
        command =         """
        CREATE TABLE ratings (
            UserID INT NOT NULL,
            Dummy1 Varchar(255) NOT NULL,
            MovieID INT NOT NULL,
            Dummy2 Varchar(255) NOT NULL,
            Rating FLOAT4 NOT NULL,
            Dummy3 VARCHAR(255) NOT NULL,
            Dummy4 Varchar(255) NOT NULL
        )
        """
        cur.execute(command)

        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print error
    # finally:
    #     if conn is not None:
    #         conn.close()


def rangepartition(ratings, N, conn):
    try:
        #conn = psycopg2.connect("dbname='pks' user='pks' host='localhost' password='pks'")
        cur = conn.cursor()
        
        # begin = float(0)
        # end = float(5)/float(N)
        # period = end
        # for i in range(N):  
        #     cur.execute("CREATE TABLE range_part"+str(i)+" (CHECK ( rating >= "+str(begin)+" AND rating < "+str(end)+" )) INHERITS ("+ratings+");")
        #     begin = end
        #     end = end + period 

        global RangePart
        RangePart = N

        begin = float(0)
        end = float(5)/float(N)
        period = end
        
        cur.execute("CREATE TABLE range_part"+str(0)+" AS SELECT * FROM "+ratings+" WHERE rating>="+str(begin)+ " AND rating<="+str(end)+";")
        begin = end
        end = end + period

        for i in range(N-1):    
            #cur.execute("CREATE TABLE range_part"+str(i)+" (CHECK ( rating >= "+str(begin)+" AND rating < "+str(end)+" )) INHERITS ("+table+");")
            cur.execute("CREATE TABLE range_part"+str(i+1)+" AS SELECT * FROM "+ratings+" WHERE rating>"+str(begin)+ " AND rating<="+str(end)+";")
            begin = end
            end = end + period 

        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print error
        print traceback.format_exc()
    # finally:
    #     if conn is not None:
    #         conn.close()


def roundrobinpartition(ratings, N, conn):
    try:
        #conn = psycopg2.connect("dbname='pks' user='pks' host='localhost' password='pks'")
        cur = conn.cursor()


        for i in range(N):  
            #cur.execute("CREATE TABLE range_part"+str(i)+"(UserID VARCHAR(255) NOT NULL, MovieID VARCHAR(255) NOT NULL, Rating FLOAT4 NOT NULL);")
            cur.execute("CREATE TABLE range_part"+str(i)+" () INHERITS ("+ratings+");")

        cur.execute("SELECT * FROM "+ratings)
        numrows = cur.rowcount
        row = []
        #col = []
        for x in range(numrows):
            #i = x % N
            #row = cur.fetchone()
            row.append(cur.fetchone())
            #cur.execute("INSERT INTO range_part"+str(i)+" VALUES ("+row[0]+","+row[1]+","+str(row[2])+");")

        for x in range(numrows):
            i = x % N
            cur.execute("INSERT INTO range_part"+str(i)+" VALUES ("+row[x][0]+","+row[x][1]+","+str(row[x][2])+");")

        #global count = ((count+1) % 5) 
            #print row[0], "-->", row[1]

        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print error
        print traceback.format_exc()
    # finally:
    #     if conn is not None:
    #         conn.close()


def roundrobininsert(ratings, userid, itemid, rating, conn):
    try: 
        #conn = psycopg2.connect("dbname='pks' user='pks' host='localhost' password='pks'")
        cur = conn.cursor()

        cur.execute("SELECT COUNT(relname) FROM pg_inherits i join pg_class c on c.oid = inhrelid where inhparent = '"+ratings+"'::regclass;")
        total_partitions = cur.fetchone()
        #print total_partitions[0]
        cur.execute("SELECT * FROM range_part"+str(0)+";")
        prev_row_count = cur.rowcount
        start_here = 0
        for i in range(total_partitions[0]):
            cur.execute("SELECT * FROM range_part"+str(i)+";")
            
            row_count = cur.rowcount

            if (row_count < prev_row_count):
                start_here = i
                #print start_here
                break
            else :
                prev_row_count = row_count
                continue

        #print start_here       
        cur.execute("INSERT INTO range_part"+str(start_here)+" VALUES ("+str(userid)+","+str(itemid)+","+str(rating)+");")
        #count = ((count+1) % 5)

        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print error
        print traceback.format_exc()
    # finally:
    #     if conn is not None:
    #         conn.close()


def rangeinsert(ratings, userid, itemid, rating, conn):
    try: 
        #conn = psycopg2.connect("dbname='pks' user='pks' host='localhost' password='pks'")
        cur = conn.cursor()

        # cur.execute("SELECT COUNT(relname) FROM pg_inherits i join pg_class c on c.oid = inhrelid where inhparent = 'ratings'::regclass;")
        # total_partitions = cur.fetchone()
        # print total_partitions

        global RangePart

        begin = float(0)
        end = float(5)/float(RangePart)
        period = end

        if (rating == 5):
            cur.execute("INSERT INTO range_part"+str((RangePart-1))+" VALUES ("+str(userid)+","+str(itemid)+","+str(rating)+");")
        else :  
            for i in range(RangePart-1):
                #print 
                if((begin <= rating) and (rating < end)):
                    #print begin
                    #print end
                    #print rating
                    cur.execute("INSERT INTO range_part"+str(i)+" VALUES ("+str(userid)+","+str(itemid)+","+str(rating)+");")
                    break               
                else :
                    begin = end
                    end = end + period
                    continue

        cur.close()
        conn.commit()           

    except (Exception, psycopg2.DatabaseError) as error:
        print error
        print traceback.format_exc()
    # finally:
    #     if conn is not None:
    #         conn.close()


def deletepartitionsandexit(conn):
    try:
        #conn = psycopg2.connect("dbname='pks' user='pks' host='localhost' password='pks'")
        cur = conn.cursor()
        
        #cur.execute("SELECT COUNT(relname) FROM pg_inherits i join pg_class c on c.oid = inhrelid where inhparent = 'ratings'::regclass;")
        #print cur
        #total_partitions = cur.fetchone()
        #print total_partitions[0]
        
        global RangePart
        for i in range(RangePart):
            cur.execute("""DROP TABLE range_part"""+str(i))

        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print "You have create partitions first inorder to delete them"
        print error
    finally:
        if conn is not None:
            conn.close()



def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='pks')
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
    con.close()


# Middleware
def before_db_creation_middleware():
    # Use it if you want to
    pass


def after_db_creation_middleware(databasename):
    # Use it if you want to
    pass


def before_test_script_starts_middleware(openconnection, databasename):
    # Use it if you want to
    pass


def after_test_script_ends_middleware(openconnection, databasename):
    # Use it if you want to
    pass


if __name__ == '__main__':
    try:

        # Use this function to do any set up before creating the DB, if any
        before_db_creation_middleware()

        create_db(DATABASE_NAME)

        # Use this function to do any set up after creating the DB, if any
        after_db_creation_middleware(DATABASE_NAME)

        with getopenconnection() as con:
            # Use this function to do any set up before I starting calling your functions to test, if you want to
            before_test_script_starts_middleware(con, DATABASE_NAME)

            # Here is where I will start calling your functions to test them. For example,
            loadratings('ratings.dat', con)
            # ###################################################################################
            # Anything in this area will not be executed as I will call your functions directly
            # so please add whatever code you want to add in main, in the middleware functions provided "only"
            # ###################################################################################

            # Use this function to do any set up after I finish testing, if you want to
            after_test_script_ends_middleware(con, DATABASE_NAME)

    except Exception as detail:
        print "OOPS! This is the error ==> ", detail
