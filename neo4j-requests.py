from neo4j import GraphDatabase
from sys import argv

if __name__ == "__main__":
	if len(argv) != 3: print("Usage: python neo4j-requests.py [user] [password]")
	argv = argv[1:]
	uri = "bolt://localhost:7687"
	driver = GraphDatabase.driver(uri, auth = (argv[0], argv[1]))
	driver.verify_connectivity()