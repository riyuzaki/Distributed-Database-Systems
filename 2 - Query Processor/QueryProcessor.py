#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2
import traceback
import os
import sys


def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, conn):

    try:

        cur = conn.cursor()

        target = open('RangeQueryOut.txt', 'w') #This is like global for this method!


        #Range 
        cur.execute("SELECT count(*) FROM rangeratingsmetadata;")

        total_partitions_range = cur.fetchone()
        end = float(5)/float(total_partitions_range[0])
        period = end

        for i in range(total_partitions_range[0]):
            if(ratingMaxValue < end):
                end = period
                for j in range(i+1):
                    if(ratingMinValue < end):
                        #select tables j through i(including both)
                        for k in range(j,i+1):
                            cur.execute("SELECT * FROM RangeRatingsPart"+str(k)+" WHERE rating >"+str(ratingMinValue)+" AND rating <="+str(ratingMaxValue)+";")
                            
                            numrows = cur.rowcount
                            for x in range(numrows):
                                result = cur.fetchone()
                                string = "RangeRatingsPart"+str(k)+ "," +str(result[0]) + "," + str(result[1]) + "," + str(result[2])
                                target.write(string)
                                target.write("\n")  


                        break
                    else:
                        end = end + period
                        continue    
                break        
            else :
                end = end  + period
                continue  



        #Round Robin
        cur.execute("SELECT * FROM roundrobinratingsmetadata;")

        total_partitions_round = cur.fetchone()
        for i in range(total_partitions_round[0]):
            cur.execute("SELECT * FROM RoundRobinRatingsPart"+str(i)+" WHERE rating >"+str(ratingMinValue)+" AND rating <="+str(ratingMaxValue)+";")

            numrows = cur.rowcount
        
            for x in range(numrows):
                result = cur.fetchone()
                string = "RoundRobinRatingsPart"+str(i)+ "," +str(result[0]) + "," + str(result[1]) + "," + str(result[2])
                target.write(string)
                target.write("\n")  

        cur.close()
        target.close()


    except (Exception, psycopg2.DatabaseError) as error:
        print error
        print traceback.format_exc()


def PointQuery(ratingsTableName, ratingValue, conn):

    try:

        cur = conn.cursor()

        target = open('PointQueryOut.txt', 'w') #This is like global for this method!


        #Range 
        cur.execute("SELECT count(*) FROM rangeratingsmetadata;")

        total_partitions_range = cur.fetchone()
        end = float(5)/float(total_partitions_range[0])
        period = end

        for i in range(total_partitions_range[0]):
            if(ratingValue < end) :
                #do
                cur.execute("SELECT * FROM RangeRatingsPart"+str(i)+" WHERE rating ="+str(ratingValue)+";")
                
                numrows = cur.rowcount
                for x in range(numrows):
                    result = cur.fetchone()
                    string = "RangeRatingsPart"+str(i)+ "," +str(result[0]) + "," + str(result[1]) + "," + str(result[2])
                    target.write(string)
                    target.write("\n")      

                break    
            else :
                end = end + period
                continue    

        #Round Robin
        cur.execute("SELECT * FROM roundrobinratingsmetadata;")

        total_partitions_round = cur.fetchone()
        for i in range(total_partitions_round[0]):
            cur.execute("SELECT * FROM RoundRobinRatingsPart"+str(i)+" WHERE rating ="+str(ratingValue)+";")

            numrows = cur.rowcount
        
            for x in range(numrows):
                result = cur.fetchone()
                string = "RoundRobinRatingsPart"+str(i)+ "," +str(result[0]) + "," + str(result[1]) + "," + str(result[2])
                target.write(string)
                target.write("\n")  

        cur.close()
        target.close()


    except (Exception, psycopg2.DatabaseError) as error:
        print error
        print traceback.format_exc()








