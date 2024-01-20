import psycopg
from sys import argv

class PostgresQueries:
	tables = ['pokemon']
	
	def __init__(self, user, password, database, host):
		try:
			self.conn = psycopg.connect(host = host, user = user, password = password,
															 dbname = database, autocommit = True)
		except psycopg.OperationalError:
			# could be because database doesn't exist
			self.conn = psycopg.connect(host = host, user = user, password = password,
                               autocommit = True)
			with self.conn.cursor() as cursor:
				cursor.execute(f'CREATE DATABASE {database}')
			self.conn.close()
			self.conn = psycopg.connect(host = host, user = user, password = password,
															 dbname = database, autocommit = True)
	
	def close(self):
		self.conn.close()
	
	def clear(self):
		with self.conn.cursor() as cursor:
			for table in self.tables:
				cursor.execute(f'DROP TABLE {table} CASCADE')
  
	def create_tables(self):
		with self.conn.cursor() as cursor:
			for table in self.tables:
				cursor.execute(f'CREATE TABLE {table} (id SERIAL PRIMARY KEY)')

if __name__ == '__main__':
	try:
		argv = argv[1:]
		user = argv[argv.index('-u') + 1] if '-u' in argv else 'postgres'
		password = argv[argv.index('-p') + 1] if '-p' in argv else 'password'
		database = argv[argv.index('-d') + 1] if '-d' in argv else 'bdspe_ng_ss'
		host = argv[argv.index('-h') + 1] if '-h' in argv else 'localhost'
		psql = PostgresQueries(user, password, database, host)
	except psycopg.Error as e:
		print(f'Error: {e}')
		print('Usage: python postgres-queries.py -u <user> -p <password>'
					+ ' -d <database> -h <host>')
		exit(1)
	psql.create_tables()
	psql.close()