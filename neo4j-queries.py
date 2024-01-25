from neo4j import GraphDatabase
from sys import argv

class Neo4jDB:
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
		r += '''
		WITH p, row WHERE row.type2 IS NOT NULL
		MERGE (t2:Type {name: row.type2})
		MERGE (p)-[:HAS_TYPE {first: false}]->(t2)
		'''
		self.session.run(r)

class Neo4jQueries:

	def __init__(self, session):
		self.session = session

	def negative_filter(self):
		'''
		Counts the number of Pokemon that are not weak against Fire and not strong
		against	Water.
		'''
		
		r = '''
		MATCH (p:Pokemon)-[r]->(m)
		WHERE NOT (p)-[:AGAINST {value: 2}]->(:Type {name: 'fire'})
			AND NOT (p)-[:AGAINST {value: 0.5}]->(:Type {name: 'water'})
		RETURN count(distinct p)
		'''
		res = self.session.run(r)
		print('1. Number of Pokemon not weak against Fire and not strong against'
					+ ' Water: ' + str(res.single()[0]))
	
	def optional_match(self):
		'''
		Get resistences of Psychic type Pokemon, apart from against Psychic and
		Fighting (well-known resistences for Psychic Pokemon), if any.
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
		ORDER BY ability
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
		ORDER BY pokemon
		'''
		res = self.session.run(r)
		print('5. Pokemon who are immunized against more than one type:')
		for r in res: print(f'{r[0]}: {r[1]}')

	def predicate_function(self):
		'''
		Get distinct pairs of Pokemon who have a common type, who both are immunized
		against a type, and where either of one of them or their common type starts
		with 'f' or 'g', and the two other nodes start with another letter.
	 	'''
	
		r = '''
		MATCH path = (p1:Pokemon)-[:HAS_TYPE]->(t:Type)<-[:HAS_TYPE]-(p2:Pokemon)
		WHERE p1 <> p2
			AND t IS NOT NULL
			AND p1.pokedex_number < p2.pokedex_number
			AND single(
				n IN nodes(path)
				WHERE n.name STARTS WITH 'f'
					OR n.name STARTS WITH 'g'
					OR n.name STARTS WITH 'F'
					OR n.name STARTS WITH 'G'
			)
			AND exists(
				(p1)-[:AGAINST {value: 0}]->(:Type)
			)
			AND exists(
				(p2)-[:AGAINST {value: 0}]->(:Type)
			)
		RETURN DISTINCT p1.name, p2.name, t.name
		ORDER BY p1.name, p2.name
		'''
		res = self.session.run(r)
		print('6. Pairs of Pokemon who have a common type, who both are immunized'
					+ ' against a type, and where either of one of them or their common'
					+ " type starts with 'f' or 'g':")
		for r in res: print(f'{r[0]} - {r[1]} (type {r[2]})')

	# TODO: check plans of post_union_processing(_variant)
	def post_union_processing(self):
		'''
		Get the 10 heaviest and lightest Pokemon and their types.
	 	'''
		
		r = '''
		CALL {
				MATCH (p:Pokemon)
				WHERE p.weight_kg IS NOT NULL
				RETURN p
				ORDER BY p.weight_kg DESC
				LIMIT 10
			UNION
				MATCH (p:Pokemon)
				WHERE p.weight_kg IS NOT NULL
				RETURN p
				ORDER BY p.weight_kg ASC
				LIMIT 10
		} WITH *
		MATCH (p)-[:HAS_TYPE]->(t:Type)
		RETURN p.name AS name, p.weight_kg AS weight_kg, collect(t.name) AS types
		ORDER BY weight_kg, name
	 	'''
		res = self.session.run(r)
		print('7. 10 heaviest and lightest Pokemon and their types:')
		for r in res: print(f'{r[0]} ({r[1]} kg): {r[2]}')

	def post_union_processing_variant(self):
		'''
		Same as post_union_processing, but with a twist.
	 	'''
		
		r = '''
		CALL {
				MATCH (p:Pokemon)-[:HAS_TYPE]->(t:Type)
				WHERE p.weight_kg IS NOT NULL
				RETURN p, collect(t.name) as types
				ORDER BY p.weight_kg DESC
				LIMIT 10
			UNION
				MATCH (p:Pokemon)-[:HAS_TYPE]->(t:Type)
				WHERE p.weight_kg IS NOT NULL
				RETURN p, collect(t.name) as types
				ORDER BY p.weight_kg ASC
				LIMIT 10
		} WITH *
		RETURN p.name AS name, p.weight_kg AS weight_kg, types
		ORDER BY weight_kg, name
		'''
		res = self.session.run(r)
		print('7b. 10 heaviest and lightest Pokemon and their types:')
		for r in res: print(f'{r[0]} ({r[1]} kg): {r[2]}')

	def data_and_topo(self):
		'''
		Get paths such as there is a loop of 3 or 4 Pokemon strong against each
		other, and where the first is not strong against the last.
		Warning: this query can be very long to run
	 	'''
		
		# first, create relationships between Pokemon where one is strong against
		# the other
		r = '''
		MATCH (p1:Pokemon)-[r:AGAINST]->(t:Type)<-[:HAS_TYPE]-(p2:Pokemon)
		WHERE r.value IN [0.25, 0.5]
			AND p1 <> p2
		MERGE (p1)-[:STRONG_AGAINST]->(p2)
		'''
		self.session.run(r)
		# run the real query
		r = '''
		MATCH path = (p1:Pokemon) ((i1:Pokemon)-[:STRONG_AGAINST]->(i2:Pokemon)){3,4} (p2)
		WHERE none(n IN i1 WHERE exists((n)-[:STRONG_AGAINST]->(p1)))
				AND exists((p2)-[:STRONG_AGAINST]->(p1))
				AND NOT exists((p1)-[:STRONG_AGAINST]->(p2))
		RETURN path
		'''
		res = self.session.run(r)
		print('8. Paths such as there is a loop of 3 or 4 Pokemon strong against'
					+ ' each other, and where the first is not strong against the last:')
		for r in res: print(r[0])
		# clean up
		r = '''
		MATCH (:Pokemon)-[r:STRONG_AGAINST]->(:Pokemon)
		DELETE r
		'''
		self.session.run(r)

	def run_queries(self, run_topo: bool = False):
		'''
		Runs all the queries.
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
		print()
		self.predicate_function()
		print()
		self.post_union_processing()
		print()
		self.post_union_processing_variant()
		if run_topo:
			print()
			self.data_and_topo()

class Neo4jAnalysis:
	
	def __init__(self, session):
		self.session = session
	
	def louvain(self):
		'''
		Count the number of communities found by the Louvain algorithm.
	 	'''
		
		r_create = '''
		CALL gds.graph.project(
				'graph1',
				['Ability', 'Pokemon', 'Type'],
				{
						AGAINST: {
								orientation: 'NATURAL'
						},
						HAS_ABILITY: {
								orientation: 'NATURAL'
						},
						HAS_TYPE: {
								orientation: 'NATURAL'
						}
				},
				{
				}
		);
		'''

		r_call = '''
		CALL gds.louvain.stream('graph1')
		YIELD nodeId, communityId, intermediateCommunityIds
		WITH gds.util.asNode(nodeId).name AS name, communityId
		RETURN communityId, COUNT(name) AS count
		ORDER BY count DESC;
		'''

		r_remove = '''
		CALL gds.graph.drop('graph1', false);
		'''

		self.session.run(r_create)

		res = self.session.run(r_call)
		print(str(res.single()[0]))

		self.session.run(r_remove)
	
	def leiden(self):
		'''
		Count the number of communities found by the Leiden algorithm.
	 	'''
	 
		r_create = '''
		CALL gds.graph.project(
				'graph1',
				['Ability', 'Pokemon', 'Type'],
				{
						AGAINST: {
								orientation: 'UNDIRECTED'
						},
						HAS_ABILITY: {
								orientation: 'UNDIRECTED'
						},
						HAS_TYPE: {
								orientation: 'UNDIRECTED'
						}
				},
				{
				}
		);
		'''
		
		r_call = '''
		CALL gds.leiden.stream('graph1')
		YIELD nodeId, communityId, intermediateCommunityIds
		WITH gds.util.asNode(nodeId).name AS name, communityId
		RETURN communityId, COUNT(name) AS count
		ORDER BY count DESC;
		'''
		
		r_remove = '''
		CALL gds.graph.drop('graph1', false);
		'''

		self.session.run(r_create)

		res = self.session.run(r_call)
		print(str(res.single()[0]))

		self.session.run(r_remove)

	def run_analysis(self):
		'''
		Runs all the queries.
		'''

		self.louvain()
		print()
		self.leiden()

def print_usage():
	print('Usage: python neo4j-queries.py <user> <password> [-r] [-t]')
	print('	-r run_queries:  import data and run general queries (default)')
	print('	-r run_analysis: import data and run analysis queries')
	print('	-r import_only:  import data without running any queries')
	print('	-t: run the last query (can be very long to run)')

if __name__ == '__main__':
	if len(argv) < 3:
		print_usage()
		exit(1)

	argv = argv[1:]
	options = argv[2:]

	run_type = argv[argv.index('-r') + 1] if '-r' in argv else 'run_queries'
	if run_type not in ['run_queries', 'run_analysis', 'import_only']:
		print_usage()
		exit(1)
	
	run_topo = True if '-t' in argv else False

	uri = 'bolt://localhost:7687'
	ndb = Neo4jDB(uri, argv[0], argv[1])
	ndb.clear()
	ndb.import_data()

	nrq = Neo4jQueries(ndb.session)
	nra = Neo4jAnalysis(ndb.session)

	if run_type != 'import_only':
		if run_type == 'run_queries':
			nrq.run_queries(run_topo)
		if run_type == 'run_analysis':
			nra.run_analysis()

	ndb.close()