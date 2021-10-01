#! /usr/bin/env python
import psycopg2
from psycopg2 import pool
import time

try:
	PostgreSQL = psycopg2.pool.SimpleConnectionPool(1,20, user="postgres",
							password="1q2w3e4r%T^Y",
							host="127.0.0.1",
							port="5432",
							database="lab1")
	if (PostgreSQL):
		print("Connection pool created")
	ps_connection = PostgreSQL.getconn()

	if (ps_connection):
		print("Successful conn from ConnectionPool")
		ps_cursor = ps_connection.cursor()
		start = time.time()
		for i in  range (0,100000):
			ps_cursor.execute("INSERT INTO lab1 (id, value) VALUES (%s, %s);", (i,"asd"))
			ps_connection.commit()
		ps_cursor.close()
		PostgreSQL.putconn(ps_connection)
		print("Put away PostgreSQL conn")
		print("--- %s seconds ---" % (time.time() - start))
except (Exeption, psycopg2.DatabaseError) as error:
	print("Err conn to PostgreSQL", error)

finally:
	if PostgreSQL:
		PostgreSQL.closeall
	print("PostgreSQL conn poll is closed")
