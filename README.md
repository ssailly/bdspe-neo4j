# bdspe-neo4j

Nicolas Graff & Steven Sailly

## Dependencies

* Neo4j Python Driver
* Psycopg3

Dependencies can be installed running `pip install -r requirements`.

## Usage

### Neo4j

`pokemon.csv` should be placed in Neo4j's `import` folder **manually** before
running the script.

`python neo4j-requests.py <user> <password> [OPTIONS]`

### PostgreSQL

`python postgresql-requests.py -u <user> -p <password> -d <database> [OPTIONS]`