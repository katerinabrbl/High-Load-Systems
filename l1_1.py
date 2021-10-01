#! /usr/bin/env python
import psycopg2
from psycopg2 import pool
import time
import threading
from concurrent.futures import ThreadPoolExecutor

def incert_value(p,x):
	ps_connection=p.getconn()
	ps_cursor = ps_connection.cursor()
	ps_cursor.execute("INSERT INTO lab120 (id, value) VALUES (%s, %s);", (x,"asd"))
	ps_connection.commit()
	ps_cursor.close()
	p.putconn(ps_connection)

try:
	PostgreSQL = psycopg2.pool.ThreadedConnectionPool(1,20, user="postgres",
							password="1q2w3e4r%T^Y",
							host="127.0.0.1",
							port="5432",
							database="lab1")
	if (PostgreSQL):
		print("Connection pool created")
		values = list(i for i in range(20))
		print("Successful conn from ConnectionPool")
		start = time.time()

		for j in  range (0,100000,20):
			with ThreadPoolExecutor(max_workers=20) as pool:
				transaction = [pool.submit(incert_value(PostgreSQL, j+i)) for i in values]
		print("20 threads")
		print("--- %s seconds ---" % (time.time() - start))

except (Exeption, psycopg2.DatabaseError) as error:
	print("Err conn to PostgreSQL", error)

finally:
	if PostgreSQL:
		PostgreSQL.closeall
	print("PostgreSQL conn poll is closed")
