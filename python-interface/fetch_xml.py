<<<<<<< HEAD
import psycopg2

try:
    connection = psycopg2.connect(user = "sa",
                                  password = "database",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "tpcds-1")
    #cursor = connection.cursor(cursor_factory=RealDictCursor)
    cursor = connection.cursor()
    #Query to be executed with format & other specifiers
    query = '''EXPLAIN (COSTS, VERBOSE, FORMAT XML) select * from call_center;'''
    cursor.execute(query)
    f = open('output.xml','w')
    f.write(cursor.fetchone()[0])
    f.close()

except (Exception, psycopg2.DatabaseError) as error :
    print ("Error while connecting to PostgreSQL", error)
finally:
    #closing database connection.
    if(connection):
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")
=======
import psycopg2

try:
    connection = psycopg2.connect(user = "sa",
                                  password = "database",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "tpcds-1")
    #cursor = connection.cursor(cursor_factory=RealDictCursor)
    cursor = connection.cursor()
    #Query to be executed with format & other specifiers
    query = '''EXPLAIN (COSTS, VERBOSE, FORMAT XML) select * from call_center;'''
    cursor.execute(query)
    f = open('output.xml','w')
    f.write(cursor.fetchone()[0])
    f.close()

except (Exception, psycopg2.DatabaseError) as error :
    print ("Error while connecting to PostgreSQL", error)
finally:
    #closing database connection.
    if(connection):
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")
>>>>>>> 72dc2fdb0f896e116071f74406ca0c2954a9f573
