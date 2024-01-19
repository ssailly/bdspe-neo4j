from neo4j import GraphDatabase
from sys import argv

class Neo4jRequest:
	def __init__(self, uri, user, password):
		self.driver = GraphDatabase.driver(uri, auth = (user, password))
		self.session = self.driver.session()
		self.driver.verify_connectivity()

	def close(self):
		self.driver.close()

	def clear(self):
		'''
		Deletes all nodes and relationships in the database.
		'''
		self.session.run('MATCH (n) DETACH DELETE n')

	def import_data(self):
		'''
		Imports the data from pokemon.csv file into the database.
		The file *must* already be placed in the import directory of Neo4j.
		'''

		types = [
			'bug', 'dark', 'dragon', 'electric', 'fairy', 'fighting', 'fire',
			'flying', 'ghost', 'grass', 'ground', 'ice', 'normal', 'poison',
			'psychic', 'rock', 'steel', 'water'
		]
		# Ability cleaning should also work using 
		# apoc.text.replace(ability, '[^a-zA-Z]', '') but is not used here because
		# we want to avoid the use of an extra library.
		r = '''
		LOAD CSV WITH HEADERS FROM 'file:///pokemon.csv' AS row
		CREATE (p:Pokemon {
			attack: toInteger(row.attack),
			base_egg_steps: toInteger(row.base_egg_steps),
			base_happiness: toInteger(row.base_happiness),
			base_total: toInteger(row.base_total),
			capture_rate: toFloat(row.capture_rate),
			classification: row.classfication,
			defense: toInteger(row.defense),
			experience_growth: toInteger(row.experience_growth),
			height_m: toFloat(row.height_m),
			hp: toInteger(row.hp),
			japanese_name: row.japanese_name,
			name: row.name,
			percentage_male: toFloat(row.percentage_male),
			pokedex_number: toInteger(row.pokedex_number),
			sp_attack: toInteger(row.sp_attack),
			sp_defense: toInteger(row.sp_defense),
			speed: toInteger(row.speed),
			weight_kg: toFloat(row.weight_kg),
			generation: toInteger(row.generation),
			is_legendary: toInteger(row.is_legendary)
		})
		WITH p, row
		UNWIND split(row.abilities, ',') AS ability
		MERGE (a:Ability {
			name: replace(
				replace(
					replace(
						trim(ability), ']', ''
					), '[', ''
				), "'", ''
			)
		})
		MERGE (p)-[:HAS_ABILITY]->(a)
		MERGE (t:Type {name: row.type1})
		MERGE (p)-[:HAS_TYPE {first: true}]->(t)
		WITH p, row WHERE row.type2 IS NOT NULL
		MERGE (t2:Type {name: row.type2})
		MERGE (p)-[:HAS_TYPE {first: false}]->(t2)
		'''
		i = 2
		for t in types:
			t2 = 'fight' if t == 'fighting' else t
			i += 1		
			var = f't{i}'
			r += f'''
			MERGE ({var}:Type {{name: '{t}'}})
			MERGE (p)-[:AGAINST {{value: toFloat(row.against_{t2})}}]->({var})
			'''
		self.session.run(r)
	
	def negative_filter(self):
		'''
		Counts the number of Pokemon that are weak against Fire and strong against
		Water.
		'''
		
		r = '''
		MATCH (p:Pokemon)-[r]->(m)
		WHERE NOT (p)-[:AGAINST {value: 2}]->(:Type {name: 'fire'})
			AND NOT (p)-[:AGAINST {value: 0.5}]->(:Type {name: 'water'})
		RETURN count(distinct p)
		'''
		res = self.session.run(r)
		print('1. Number of Pokemon weak against Fire and strong against Water: '
					+ str(res.single()[0]))
	
	def optional_match(self):
		'''
		Get resistences of Psychic type Pokemon, apart from against Psychic and
		Fighting (well-known resistences for Psychic Pokemon).
		'''

		print('2. Psychic type Pokemon resistences:')
		r = '''
		MATCH (p:Pokemon)-[:HAS_TYPE {first: true}]->(:Type {name: 'psychic'})
		OPTIONAL MATCH (p)-[r:AGAINST]->(t:Type)
		WHERE NOT t.name IN ['psychic', 'fighting']
				AND r.value IN [0.5, 0.25]
		RETURN p.name, t.name, r.value
		'''
		res = self.session.run(r)
		print('Pokemon\t\tType\t\tValue')
		for r in res:
			tab1 = '\t\t' if len(r[0]) < 8 else '\t'
			tab2 = '\t\t' if (r[1] != None and len(r[1]) < 8) else '\t'
			print(f'{r[0]}{tab1}{r[1]}' + (f'{tab2}{r[2]}' if r[1] != None else ''))

	# TODO: check plans of collect_unwind and collect_unwind_variant
	def collect_unwind(self):
		'''
		Find the abilities of Pokemon (very) weak against Psychic type, and count
		how many of them have each ability.
	 	'''

		r = '''
		MATCH (p:Pokemon)-[r:AGAINST]->(t:Type {name: 'psychic'})
		WHERE r.value IN [2, 4]
		WITH p, COLLECT {
			MATCH (p)-[:HAS_ABILITY]->(a:Ability)
			RETURN a.name
		} AS abilities
		UNWIND abilities AS ability
		RETURN ability, COUNT(distinct p)
		'''
		res = self.session.run(r)
		print('3. Abilities of Pokemon (very) weak against Psychic type:')
		for r in res: print(f'{r[0]}: {r[1]}')
	
	def collect_unwind_variant(self):
		'''
		Same as collect_unwind, but without using COLLECT and UNWIND.
		'''
		
		r = '''
		MATCH (p:Pokemon)-[r:AGAINST]->(t:Type {name: 'psychic'})
		WHERE r.value IN [2, 4]
		MATCH (p)-[:HAS_ABILITY]->(a:Ability)
		RETURN a.name, COUNT(distinct p)
		'''
		res = self.session.run(r)
		print('3b. Same as 3., but without using COLLECT and UNWIND:')
		for r in res: print(f'{r[0]}: {r[1]}')

	def reduce(self):
		'''
		For each ability, sum the attack of all Pokemon (very) weak against Fire,
		Water or Grass, whose name starts with 'A'. If there is no such Pokemon for
		an ability, the ability should not be returned.
		'''	

		r = '''
		MATCH (t:Type)<-[r:AGAINST]-(p:Pokemon)-[:HAS_ABILITY]->(a:Ability)
		WHERE r.value IN [2, 4]
			AND p.name STARTS WITH 'A'
			AND t.name IN ['fire', 'water', 'grass']
		WITH a, collect(distinct p) AS list_pkmn
		RETURN a.name AS ability,
			reduce(
				total_attack = 0, pp IN list_pkmn | total_attack + pp.attack
			) AS total_attack,
			reduce(
		 		names = [], p IN list_pkmn | 
			 	CASE WHEN NOT p.name IN names THEN names + p.name ELSE names END
			) as pokemons
		ORDER BY ability
		'''
	
		res = self.session.run(r)
		print("4. Total attack of Pokemon (very) weak against Fire, Water or Grass,"
					+ " whose name starts with 'A' and who can learn a given ability:")
		for r in res: print(f'{r[0]}: {r[1]} ({r[2]})')

	def with_filter_aggregate(self):
		'''
		Get Pokemon who are immunized against more than one type.
		'''

		r = '''
		MATCH (p:Pokemon)-[:AGAINST {value: 0}]->(t:Type)
		WITH p, count(distinct t) AS count_types
		WHERE count_types > 1
		RETURN p.name AS pokemon, count_types
		'''
		res = self.session.run(r)
		print('5. Pokemon who are immunized against more than one type:')
		for r in res: print(f'{r[0]}: {r[1]}')

	def run_all(self):
		'''
		Runs all the requests.
		'''

		self.negative_filter()
		print()
		self.optional_match()
		print()
		self.collect_unwind()
		print()
		self.collect_unwind_variant()
		print()
		self.reduce()
		print()
		self.with_filter_aggregate()


if __name__ == '__main__':
	if len(argv) < 3:
		print('Usage: python neo4j-requests.py [user] [password] <OPTIONS>')
		print('OPTIONS:')
		print('- import_only: import data without running requests')
		exit(1)
	argv = argv[1:]
	options = argv[2:]
	uri = 'bolt://localhost:7687'
	nrq = Neo4jRequest(uri, argv[0], argv[1])
	nrq.clear()
	nrq.import_data()
	if 'import_only' not in options: nrq.run_all()
	nrq.close()