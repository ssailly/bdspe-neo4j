import psycopg
from sys import argv

tables = [
	'pokemon', 'type', 'ability', 'pokemon_type', 'pokemon_ability',
	'pokemon_percentage_male', 'pokemon_sensibility', 'pokemon_classification',
	'pokemon_basic_stats', 'pokemon_battle_stats', 'pokemon_generation',
	'pokemon_legendary'
]

class PostgresQueries:
	def __init__(self, user, password, database, host, datafile):
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
		self.create_and_populate(datafile)
	
	def close(self):
		self.conn.close()
	
	def clear(self):
		'''
		Drop all tables in database.
		'''
		with self.conn.cursor() as cursor:
			for table in tables:
				cursor.execute(f'DROP TABLE {table} CASCADE')
	
	def create_and_populate(self, datafile: str):
		'''
		Create all tables in database, and populate them with data from csv file.

		Args:
			datafile: path to a csv file containing data to populate tables with.
		'''

		with self.conn.cursor() as cursor:
			for table in tables:
				cursor.execute(f'DROP TABLE IF EXISTS {table} CASCADE')
			self.__create_tables(cursor)
			self.__populate_tables(cursor, datafile)

	def __create_tables(self, cursor: psycopg.cursor):
		'''
		Create all tables in database.
		'''
	 
		cursor.execute(QueryUtils.create_pokemon_table())
		cursor.execute(QueryUtils.create_basic_table('type'))
		cursor.execute(QueryUtils.create_association_table('type'))
		cursor.execute(QueryUtils.create_basic_table('ability'))
		cursor.execute(QueryUtils.create_association_table('ability'))
		cursor.execute(
			QueryUtils.create_basic_association_table(
				'percentage_male',
				'REAL'
			)
		)
		cursor.execute(
			QueryUtils.create_basic_association_table(
			 	'classification'
			)
		)
		cursor.execute(
			QueryUtils.create_basic_association_table(
			 	'generation',
				'INTEGER'
			)
		)
		cursor.execute(QueryUtils.create_pokemon_sensibility_table())
		cursor.execute(QueryUtils.create_pokemon_basic_stats_table())
		cursor.execute(QueryUtils.create_pokemon_battle_stats_table())
		cursor.execute(QueryUtils.create_pokemon_legendary_table())

	def __populate_tables(self, cursor: psycopg.cursor, datafile: str):
		'''
		Populate all tables in database with data from csv file.

		Args:
			datafile: path to a csv file containing data to populate tables with.
		'''

		cursor.execute('''
			CREATE TEMP TABLE tmp (
				abilities TEXT[],
				against_bug REAL, against_dark REAL, against_dragon REAL,
			 	against_electric REAL, against_fairy REAL, against_fight REAL,
				against_fire REAL,against_flying REAL,against_ghost REAL,
				against_grass REAL, against_ground REAL, against_ice REAL,
				against_normal REAL, against_poison REAL, against_psychic REAL,
				against_rock REAL, against_steel REAL, against_water REAL,
				attack INTEGER, base_egg_steps INTEGER,base_happiness INTEGER,
				base_total INTEGER, capture_rate INTEGER, classfication TEXT,
				defense INTEGER, experience_growth INTEGER, height_m REAL, hp INTEGER,
				japanese_name TEXT, name TEXT, percentage_male REAL,
				pokedex_number INTEGER, sp_attack INTEGER,sp_defense INTEGER,
				speed INTEGER, type1 TEXT, type2 TEXT, weight_kg REAL,
				generation INTEGER,is_legendary BOOLEAN
			)
		''')
		with open(datafile, 'r') as f:
			with cursor.copy("COPY tmp FROM STDIN DELIMITER ',' CSV HEADER") as copy:
					while data := f.read(100): 
						data = data.replace('[', '{').replace(']', '}').replace('\'', '')
						copy.write(data)
		

class QueryUtils:
	'''
	Convenience methods for storing queries. All methods should be static and 
	return a string, which is the query to be executed.
	'''

	@staticmethod
	def create_pokemon_table() -> str:
		return '''
			CREATE TABLE pokemon (
				pokedex_id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				japanese_name TEXT
			)
		'''
	
	@staticmethod
	def create_basic_table(name: str, datatype: str = 'TEXT') -> str:
		return f'''
			CREATE TABLE {name} (
				{name}_id SERIAL PRIMARY KEY,
				name {datatype} NOT NULL
			)
		'''
	
	@staticmethod
	def create_association_table(name: str) -> str:
		'''
		One-to-many association table between pokemon and a basic table.
		'''

		res = f'''
			CREATE TABLE pokemon_{name} (
				pokemon_id INTEGER references pokemon(pokedex_id) NOT NULL,
				{name}_id INTEGER references {name}({name}_id) NOT NULL
		'''
		if name == 'type': res += ', first_type BOOLEAN NOT NULL'
		return res + ')'
	
	@staticmethod
	def create_basic_association_table(name: str, datatype: str = 'TEXT') -> str:
		'''
		One-to-many association table between pokemon and characteristics that
		are not in a basic table.
		'''

		return f'''
			CREATE TABLE pokemon_{name} (
				pokemon_id INTEGER references pokemon(pokedex_id) NOT NULL,
				{name} {datatype} NOT NULL
			)
		'''
	
	@staticmethod
	def create_pokemon_sensibility_table() -> str:
		return '''
			CREATE TABLE pokemon_sensibility (
				pokemon_id INTEGER references pokemon(pokedex_id) NOT NULL,
				type_id INTEGER references type(type_id) NOT NULL,
				sensibility REAL 
				CONSTRAINT check_sensibility CHECK (
					sensibility = 0 OR sensibility = 0.25 OR sensibility = 0.5 OR
					sensibility = 1 OR sensibility = 2 OR sensibility = 4
				)
			)
		'''
	
	@staticmethod
	def create_pokemon_basic_stats_table() -> str:
		return '''
			CREATE TABLE pokemon_basic_stats (
				pokemon_id INTEGER references pokemon(pokedex_id) NOT NULL,
				height_m REAL,
				weight_kg REAL,
				capture_rate INTEGER,
				base_egg_steps INTEGER,
				experience_growth INTEGER,
				base_happiness INTEGER
			)
		'''
	
	@staticmethod
	def create_pokemon_battle_stats_table() -> str:
		return '''
			CREATE TABLE pokemon_battle_stats (
				pokemon_id INTEGER references pokemon(pokedex_id) NOT NULL,
				hp INTEGER,
				attack INTEGER,
				defense INTEGER,
				sp_attack INTEGER,
				sp_defense INTEGER,
				speed INTEGER
			)
		'''
	
	@staticmethod
	def create_pokemon_legendary_table() -> str:
		return '''
			CREATE TABLE pokemon_legendary (
				pokemon_id INTEGER references pokemon(pokedex_id) NOT NULL
			)
		'''
	

if __name__ == '__main__':
	try:
		argv = argv[1:]
		user = argv[argv.index('-u') + 1] if '-u' in argv else 'postgres'
		password = argv[argv.index('-p') + 1] if '-p' in argv else 'password'
		database = argv[argv.index('-d') + 1] if '-d' in argv else 'bdspe_ng_ss'
		host = argv[argv.index('-h') + 1] if '-h' in argv else 'localhost'
		datafile = argv[argv.index('-f') + 1] if '-f' in argv else 'pokemon.csv'
		psql = PostgresQueries(user, password, database, host, datafile)
	except psycopg.Error as e:
		print(f'Error: {e}')
		print('Usage: python postgres-queries.py -u <user> -p <password>'
					+ ' -d <database> -h <host> -f <datafile>')
		exit(1)
	psql.close()