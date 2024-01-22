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
	
		tmp_table = 'tmp'

		cursor.execute(f'''
			CREATE TEMP TABLE {tmp_table} (
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
			with cursor.copy(
		 		f"COPY {tmp_table} FROM STDIN DELIMITER ',' CSV HEADER"
			) as copy:
					while data := f.read(100): 
						data = data.replace('[', '{').replace(']', '}').replace('\'', '')
						copy.write(data)
		cursor.execute(QueryUtils.populate_pokemon_table(tmp_table))
		cursor.execute(QueryUtils.populate_type_table(tmp_table))
		cursor.execute(QueryUtils.populate_ability_table(tmp_table))
		cursor.execute(
			QueryUtils.populate_basic_association_table(
				'classification',
			 	tmp_table
			)
		)
		cursor.execute(
			QueryUtils.populate_basic_association_table(
				'generation',
			 	tmp_table
			)
		)
		cursor.execute(
			QueryUtils.populate_basic_association_table(
				'percentage_male',
			 	tmp_table
			)
		)
		cursor.execute(QueryUtils.populate_pokemon_basic_stats_table(tmp_table))
		cursor.execute(QueryUtils.populate_pokemon_battle_stats_table(tmp_table))
		cursor.execute(QueryUtils.populate_pokemon_legendary_table(tmp_table))
		cursor.execute(QueryUtils.populate_pokemon_sensibility_table(tmp_table))
		cursor.execute(QueryUtils.populate_pokemon_ability_table(tmp_table))
		cursor.execute(QueryUtils.populate_pokemon_type_table(tmp_table))
		cursor.execute(f'DROP TABLE {tmp_table}')

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
	def create_basic_table(name: str) -> str:
		return f'''
			CREATE TABLE {name} (
				{name}_id SERIAL PRIMARY KEY,
				name TEXT NOT NULL
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
				{name} {datatype}
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
	
	@staticmethod
	def populate_pokemon_table(tmp_table: str) -> str:
		return f'''
			INSERT INTO pokemon (pokedex_id, name, japanese_name)
			SELECT pokedex_number, name, japanese_name FROM {tmp_table}
		'''
	
	@staticmethod
	def populate_type_table(tmp_table: str) -> str:
		# every type appears at least once as first type
		return f'''
			INSERT INTO type (name)
			SELECT DISTINCT type1 FROM {tmp_table}
		'''
	
	@staticmethod
	def populate_ability_table(tmp_table: str) -> str:
		return f'''
			INSERT INTO ability (name)
			SELECT DISTINCT unnest(abilities) FROM {tmp_table}
		'''
	
	@staticmethod
	def populate_basic_association_table(type: str, tmp_table: str) -> str:
		type2 = 'classfication' if type == 'classification' else type
		return f'''
			INSERT INTO pokemon_{type}
			SELECT pokedex_number, {type2} FROM {tmp_table}
		'''
	
	@staticmethod
	def populate_pokemon_basic_stats_table(tmp_table: str) -> str:
		return f'''
			INSERT INTO pokemon_basic_stats
			SELECT pokedex_number, height_m, weight_kg, capture_rate,
				base_egg_steps, experience_growth, base_happiness
			FROM {tmp_table}
		'''
	
	@staticmethod
	def populate_pokemon_battle_stats_table(tmp_table: str) -> str:
		return f'''
			INSERT INTO pokemon_battle_stats
			SELECT pokedex_number, hp, attack, defense, sp_attack, sp_defense, speed
			FROM {tmp_table}
		'''
	
	@staticmethod
	def populate_pokemon_legendary_table(tmp_table: str) -> str:
		return f'''
			INSERT INTO pokemon_legendary
			SELECT pokedex_number FROM {tmp_table} WHERE is_legendary
		'''
	
	@staticmethod
	def populate_pokemon_sensibility_table(tmp_table: str) -> str:
		types = [
			'bug', 'dark', 'dragon', 'electric', 'fairy', 'fighting', 'fire',
	 		'flying', 'ghost', 'grass', 'ground', 'ice', 'normal', 'poison',
		 	'psychic', 'rock', 'steel', 'water'
		]
		res = '''INSERT INTO pokemon_sensibility
		SELECT pokedex_number, type_id, sensibility FROM (
		'''
		for type in types:
			against = 'fight' if type == 'fighting' else type
			res += f'''
				SELECT pokedex_number, type_id, against_{against} AS sensibility
				FROM {tmp_table} JOIN type ON '{type}' = type.name
			'''
			if type != types[-1]: res += ' UNION ALL '
		return res + ') AS foo'
	
	@staticmethod
	def populate_pokemon_ability_table(tmp_table: str) -> str:
		return f'''
		INSERT INTO pokemon_ability
		SELECT pokedex_number, ability_id FROM (
			SELECT pokedex_number, unnest(abilities) AS ability
			FROM {tmp_table}
		) AS foo JOIN ability ON foo.ability = ability.name
		'''

	@staticmethod
	def populate_pokemon_type_table(tmp_table: str) -> str:
		return f'''
		INSERT INTO pokemon_type
		SELECT pokedex_number, type_id, first_type FROM (
			SELECT pokedex_number, type1 AS type, true AS first_type
			FROM {tmp_table}
			UNION ALL
			SELECT pokedex_number, type2 AS type, false AS first_type
			FROM {tmp_table} WHERE type2 IS NOT NULL
		) AS foo JOIN type ON foo.type = type.name
		'''

class Neo4jEquivalents:
	'''
	Convenience methods for storing queries equivalent to those written for
	querying on the Neo4j database. All methods should be static and return a
	string, which is the query to be executed. All methods' names should be
	the same as the corresponding method in neo4j-queries.py, even if it is
	irrelevant to the SQL query.
	'''

	@staticmethod
	def negative_filter() -> str:
		raise NotImplementedError('Not implemented for SQL')

	@staticmethod
	def optional_match() -> str:
		'''
		Get resistences of Psychic type Pokemon, apart from against Psychic and
		Fighting (well-known resistences for Psychic Pokemon), if any.
		'''

		return '''
		SELECT DISTINCT pokemon.name, type.name, sensibility FROM pokemon_type
		JOIN pokemon ON
			pokemon.pokedex_id = pokemon_type.pokemon_id
			AND type_id = (SELECT type_id FROM type WHERE name = 'psychic')
			AND pokemon_type.first_type
		LEFT JOIN pokemon_sensibility ON
			pokemon_sensibility.pokemon_id = pokemon_type.pokemon_id
			AND pokemon_sensibility.type_id IN (
		 		SELECT type_id FROM type WHERE name NOT IN ('psychic', 'fighting')
			)
			AND pokemon_sensibility.sensibility IN (0.25, 0.5)
		LEFT JOIN type ON type.type_id = pokemon_sensibility.type_id
	 ORDER BY pokemon.name
		'''

	@staticmethod
	def collect_unwind() -> str:
		'''
		Find the abilities of Pokemon (very) weak against Psychic type, and count
		how many of them have each ability.
	 	'''
		
		return '''
		SELECT DISTINCT ability.name ability, COUNT(DISTINCT pokemon_ability.pokemon_id) FROM pokemon_sensibility
		JOIN pokemon_ability ON
			pokemon_ability.pokemon_id = pokemon_sensibility.pokemon_id
		JOIN ability ON
			ability.ability_id = pokemon_ability.ability_id
		WHERE sensibility IN (2, 4)
	 		AND type_id = (
			 	SELECT type_id FROM type WHERE name = 'psychic'
			)
		GROUP BY ability.name
		ORDER BY ability.name
		'''

	@staticmethod
	def reduce() -> str:
		'''
		For each ability, sum the attack of all Pokemon (very) weak against Fire,
		Water or Grass, whose name starts with 'A'. If there is no such Pokemon for
		an ability, the ability should not be returned.
		'''	

		return '''
		SELECT name, SUM(attack), array_agg(pname) FROM (
			SELECT DISTINCT ability.name name, pokemon.name pname, attack FROM ability
			JOIN pokemon_ability ON
				pokemon_ability.ability_id = ability.ability_id
			JOIN pokemon_battle_stats ON
				pokemon_battle_stats.pokemon_id = pokemon_ability.pokemon_id
			JOIN pokemon ON
				pokemon.pokedex_id = pokemon_ability.pokemon_id
				AND pokemon.name LIKE 'A%'
			JOIN pokemon_sensibility ON
				pokemon_sensibility.pokemon_id = pokemon.pokedex_id
				AND pokemon_sensibility.type_id IN (
					SELECT type_id FROM type WHERE name IN ('fire', 'water', 'grass')
				)
				AND pokemon_sensibility.sensibility IN (2, 4)
		) foo
		GROUP BY name
		ORDER BY name
		'''

	@staticmethod
	def with_filter_aggregate() -> str:
		'''
		Get Pokemon who are immunized against more than one type.
		'''
	
		return '''
		SELECT pokemon.name, COUNT(DISTINCT pokemon_sensibility.type_id) count_types
		FROM pokemon_sensibility
	 	JOIN pokemon ON
			pokemon_sensibility.pokemon_id = pokemon.pokedex_id
		 	AND sensibility = 0
		GROUP BY pokemon.name
		HAVING COUNT(DISTINCT pokemon_sensibility.type_id) > 1
		ORDER BY pokemon.name
		'''

	@staticmethod
	def predicate_function() -> str:
		'''
		Get distinct pairs of Pokemon who have a common type, who both are immunized
		against a type, and where either of one of them or their common type starts
		with 'f' or 'g', and the two other nodes start with another letter.
	 	'''

		xor_p1 = "p1.name SIMILAR TO '[fgFG]%'"
		xor_p2 = "p2.name SIMILAR TO '[fgFG]%'"
		xor_type = "type.name SIMILAR TO '[fgFG]%'"
		xor_exp = f'''
		({xor_p1} AND NOT {xor_p2} AND NOT {xor_type})
		OR (NOT {xor_p1} AND {xor_p2} AND NOT {xor_type})
		OR (NOT {xor_p1} AND NOT {xor_p2} AND {xor_type})
		'''
		return f'''
		SELECT DISTINCT p1.name, p2.name, type.name
		FROM pokemon_sensibility ps1
		JOIN pokemon_sensibility ps2 ON
			ps1.pokemon_id < ps2.pokemon_id
			AND ps1.sensibility = 0
			AND ps2.sensibility = 0
		JOIN pokemon_type pt1 ON
			pt1.pokemon_id = ps1.pokemon_id
		JOIN pokemon_type pt2 ON
			pt2.pokemon_id = ps2.pokemon_id
			AND pt1.type_id = pt2.type_id
		JOIN type ON
			type.type_id = pt1.type_id
			AND type.type_id = pt2.type_id
		JOIN pokemon p1 ON
			p1.pokedex_id = ps1.pokemon_id
		JOIN pokemon p2 ON
			p2.pokedex_id = ps2.pokemon_id
		WHERE {xor_exp}
		ORDER BY p1.name, p2.name
		'''

	@staticmethod
	def post_union_processing() -> str:
		'''
		Get the 10 heaviest and lightest Pokemon and their types.
	 	'''

		return '''
		SELECT * FROM (
			SELECT pokemon.name, weight_kg, types FROM pokemon
			JOIN pokemon_basic_stats ON
				pokemon_basic_stats.pokemon_id = pokemon.pokedex_id
				AND weight_kg IS NOT NULL
			JOIN (
				SELECT pokemon_id, array_agg(type.name) types FROM pokemon_type
				JOIN type ON type.type_id = pokemon_type.type_id
				GROUP BY pokemon_id
			) foo ON foo.pokemon_id = pokemon.pokedex_id
			ORDER BY weight_kg DESC, pokemon.name
			LIMIT 10
	 	) bar
		UNION (
			SELECT pokemon.name, weight_kg, types FROM pokemon
			JOIN pokemon_basic_stats ON
				pokemon_basic_stats.pokemon_id = pokemon.pokedex_id
				AND weight_kg IS NOT NULL
			JOIN (
				SELECT pokemon_id, array_agg(type.name) types FROM pokemon_type
				JOIN type ON type.type_id = pokemon_type.type_id
				GROUP BY pokemon_id
			) foo ON foo.pokemon_id = pokemon.pokedex_id
			ORDER BY weight_kg ASC, pokemon.name
			LIMIT 10
		) ORDER BY weight_kg, name
		'''

	#################################
	# OOM killer would get involved #
	#    without 'LIMIT' clause     #
	# If it still doesn't work, try #
	# with a smaller depth (e.g. 4) #
	#################################
	@staticmethod
	def data_and_topo() -> str:
		'''
		Get paths such as there is a loop of 3 or 4 Pokemon strong against each
		other, and where the first is not strong against the last.
		'''
		
		create_pokemon_strong = Neo4jEquivalents.__create_pokemon_strong()
		populate_pokemon_strong = Neo4jEquivalents.__populate_pokemon_strong()
		# 'AND EXISTS...' for ensuring there is no loop in the path
		recursive_query = '''
		WITH RECURSIVE path AS (
			SELECT pid_1, pid_2, 0 AS depth, ARRAY[pid_1] arr FROM pokemon_strong
			UNION
			SELECT p.pid_1, ps.pid_2, depth + 1, arr || ps.pid_2 FROM path p
			JOIN pokemon_strong ps ON
				p.pid_2 = ps.pid_1
			WHERE depth < 5
		)
		SELECT pid_1 start, pid_2 end, arr path FROM path p
		WHERE
			EXISTS (
				SELECT * FROM pokemon_strong ps
				WHERE p.pid_1 = ps.pid_2
					AND p.pid_2 = ps.pid_1
			)
			AND NOT EXISTS (
				SELECT * FROM pokemon_strong ps
				WHERE p.pid_1 = ps.pid_1
					AND p.pid_2 = ps.pid_2
			)
			AND depth = 3 OR depth = 4
			AND EXISTS (
				WITH dist_c AS (
					SELECT COUNT(DISTINCT u) dist_c FROM unnest(arr) u
				), c AS (
					SELECT COUNT(u) c FROM unnest(arr) u
    		)
				SELECT * FROM dist_c JOIN c ON dist_c.dist_c = c.c
			)
		LIMIT 30;
		'''
		return f'''
		{create_pokemon_strong}
		{populate_pokemon_strong}
		{recursive_query}
		'''

	@staticmethod
	def __create_pokemon_strong() -> str:
		'''
		Create a table pokemon_strong(p1, p2) where p1 is strong against p2.
	 	'''
		
		return '''
		CREATE TABLE pokemon_strong (
			pid_1 INTEGER references pokemon(pokedex_id) NOT NULL,
		 	pid_2 INTEGER references pokemon(pokedex_id) NOT NULL
			CONSTRAINT check_different CHECK (
				pid_1 <> pid_2
			)
		);
		'''
	
	@staticmethod
	def __populate_pokemon_strong() -> str:
		return '''
		INSERT INTO pokemon_strong
		SELECT DISTINCT ps.pokemon_id pid_1, pt.pokemon_id pid_2
		FROM pokemon_sensibility ps
		JOIN pokemon_type pt ON
			ps.type_id = pt.type_id
			AND ps.pokemon_id <> pt.pokemon_id
		WHERE sensibility IN (0.25, 0.5);
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