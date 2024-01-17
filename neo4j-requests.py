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
			'Bug', 'Dark', 'Dragon', 'Electric', 'Fairy', 'Fighting', 'Fire',
			'Flying', 'Ghost', 'Grass', 'Ground', 'Ice', 'Normal', 'Poison',
			'Psychic', 'Rock', 'Steel', 'Water'
		]
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
			type1: row.type1,
			type2: row.type2,
			weight_kg: toFloat(row.weight_kg),
			generation: toInteger(row.generation),
			is_legendary: toInteger(row.is_legendary)
		})
  	'''
		i = 0
		for t in types:
			t_lowered = t.lower()
			i += 1		
			var = f't{i}'
			r += f'''
			MERGE ({var}:Type {{name: '{t}'}})
			CREATE (p)-[:AGAINST {{value: toFloat(row.against_{t_lowered})}}]->({var})
    	'''
		self.session.run(r)
  
	def negative_filter(self):
		'''
  	Counts the number of Pokemon that are weak against Fire and strong against
  	Water.
  	'''
		
		r = '''
  	MATCH (p:Pokemon)-[r]->(m)
		WHERE NOT (p)-[:AGAINST {value: 2}]->(:Type {name: 'Fire'})
			AND NOT (p)-[:AGAINST {value: 0.5}]->(:Type {name: 'Water'})
		RETURN count(distinct p)
  	'''
		res = self.session.run(r)
		print(res.single()[0])


if __name__ == '__main__':
	if len(argv) != 3:
		print('Usage: python neo4j-requests.py [user] [password]')
		exit(1)
	argv = argv[1:]
	uri = 'bolt://localhost:7687'
	nrq = Neo4jRequest(uri, argv[0], argv[1])
	nrq.clear()
	nrq.import_data()
	nrq.negative_filter()
	nrq.close()