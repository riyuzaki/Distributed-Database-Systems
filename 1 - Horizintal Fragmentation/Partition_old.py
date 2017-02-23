import psycopg2
import traceback

def Load_ratings(file):
	try:
		file = open(file, 'r')
		conn = psycopg2.connect("dbname='pks' user='pks' host='localhost' password='pks'")
		cur = conn.cursor()
		cur.copy_from(file, 'ratings', ':')
		cur.execute("ALTER TABLE ratings DROP COLUMN dummy1, DROP COLUMN dummy2, DROP COLUMN dummy3, DROP COLUMN dummy4;")
		
		file.close()
		cur.close()
		conn.commit()
	except (Exception, psycopg2.DatabaseError) as error:
		print error
	finally:
		if conn is not None:
			conn.close()


def Range_Partition(table, N):
	try:
		conn = psycopg2.connect("dbname='pks' user='pks' host='localhost' password='pks'")
		cur = conn.cursor()
		
		begin = float(0)
		end = float(5)/float(N)
		period = end
		for i in range(N):	
			cur.execute("CREATE TABLE range_part"+str(i)+" (CHECK ( rating >= "+str(begin)+" AND rating < "+str(end)+" )) INHERITS ("+table+");")
			begin = end
			end = end + period 

		cur.close()
		conn.commit()
	except (Exception, psycopg2.DatabaseError) as error:
		print error
		print traceback.format_exc()
	finally:
		if conn is not None:
			conn.close()


def Range_Insert(table,UserID,ItemID,Rating):
	try: 
		conn = psycopg2.connect("dbname='pks' user='pks' host='localhost' password='pks'")
		cur = conn.cursor()

		cur.execute("SELECT COUNT(relname) FROM pg_inherits i join pg_class c on c.oid = inhrelid where inhparent = 'ratings'::regclass;")
		total_partitions = cur.fetchone()

		begin = float(0)
		end = float(5)/float(total_partitions[0])
		period = end

		for i in range(total_partitions[0]):
			#print 
			if((begin <= Rating) and (Rating < end)):
				print begin
				print end
				print Rating
				cur.execute("INSERT INTO range_part"+str(i)+" VALUES ("+UserID+","+ItemID+","+str(Rating)+");")
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
	finally:
		if conn is not None:
			conn.close()


def RoundRobin_Partition(table, N):
	try:
		conn = psycopg2.connect("dbname='pks' user='pks' host='localhost' password='pks'")
		cur = conn.cursor()


		for i in range(N):	
			#cur.execute("CREATE TABLE range_part"+str(i)+"(UserID VARCHAR(255) NOT NULL, MovieID VARCHAR(255) NOT NULL, Rating FLOAT4 NOT NULL);")
			cur.execute("CREATE TABLE range_part"+str(i)+" () INHERITS (ratings);")

		cur.execute("SELECT * FROM ratings")
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
	finally:
		if conn is not None:
			conn.close()	


def	RoundRobin_Insert(table,UserID,ItemID,Rating):
	try: 
		conn = psycopg2.connect("dbname='pks' user='pks' host='localhost' password='pks'")
		cur = conn.cursor()

		cur.execute("SELECT COUNT(relname) FROM pg_inherits i join pg_class c on c.oid = inhrelid where inhparent = 'ratings'::regclass;")
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
  		cur.execute("INSERT INTO range_part"+str(start_here)+" VALUES ("+UserID+","+ItemID+","+str(Rating)+");")
  		#count = ((count+1) % 5)

		cur.close()
		conn.commit()
	except (Exception, psycopg2.DatabaseError) as error:
		print error
		print traceback.format_exc()
	finally:
		if conn is not None:
			conn.close()		


def Delete_Partitions():
	try:
		conn = psycopg2.connect("dbname='pks' user='pks' host='localhost' password='pks'")
		cur = conn.cursor()
		
		cur.execute("SELECT COUNT(relname) FROM pg_inherits i join pg_class c on c.oid = inhrelid where inhparent = 'ratings'::regclass;")
		#print cur
		total_partitions = cur.fetchone()
		#print total_partitions[0]
		
		for i in range(total_partitions[0]):
			cur.execute("""DROP TABLE range_part"""+str(i))

		cur.close()
		conn.commit()
	except (Exception, psycopg2.DatabaseError) as error:
		print "You have create partitions first inorder to delete them"
		print error
	finally:
		if conn is not None:
			conn.close()						


def create_table():
	try:
		conn = psycopg2.connect("dbname='pks' user='pks' host='localhost' password='pks'")
		cur = conn.cursor()
		command =         """
        CREATE TABLE Ratings (
            UserID VARCHAR(255) NOT NULL,
            Dummy1 Varchar(255) NOT NULL,
            MovieID VARCHAR(255) NOT NULL,
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
	finally:
		if conn is not None:
			conn.close()		


def main():
	#try:conn = psycopg2.connect("dbname='pks' user='pks' host='localhost' password='pks'")
	#except:print traceback.format_exc()#"I am unable to connect to the database"
	file = "test.dat"
	#create_table()
	#Load_ratings(file)
	#drop_table()
	#Range_Partition("ratings",5)
	#Delete_Partitions()
	#RoundRobin_Partition("ratings", 5)
	#RoundRobin_Insert("ratings","2","555",3)
	Range_Insert("ratings","3","777",2)

if __name__ == '__main__':
	main()