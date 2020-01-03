<<<<<<< HEAD
import psycopg2
#No idea why below import exists
from psycopg2 import Error
try:
    connection = psycopg2.connect(user = "postgres",
                                  password = "Iwilldoit#1",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "TPC-DS")
    cursor = connection.cursor()
    # Print PostgreSQL Connection properties
    print ( connection.get_dsn_parameters(),"\n")
    # Print PostgreSQL version
    cursor.execute("SELECT version();")
    record = cursor.fetchone()
    print("You are connected to - ", record,"\n")
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
#No idea why below import exists
from psycopg2 import Error
try:
    connection = psycopg2.connect(user = "postgres",
                                  password = "Iwilldoit#1",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "TPC-DS")
    cursor = connection.cursor()
    # Print PostgreSQL Connection properties
    print ( connection.get_dsn_parameters(),"\n")
    # Print PostgreSQL version
    cursor.execute("SELECT version();")
    record = cursor.fetchone()
    print("You are connected to - ", record,"\n")
except (Exception, psycopg2.DatabaseError) as error :
    print ("Error while connecting to PostgreSQL", error)
finally:
    #closing database connection.
    if(connection):
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")
>>>>>>> 72dc2fdb0f896e116071f74406ca0c2954a9f573
