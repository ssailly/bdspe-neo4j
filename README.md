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

`python neo4j-queries.py <user> <password> [OPTIONS]`

Options:
- `-h`: Print help
- `-r run_queries`: Import data and run general queries (default)
- `-r run_analysis`: Import data and run analysis queries
- `-r import_only`: Import data without running any queries
- `-k [number]`: Choose the query to run for **run_queries**
- `-t:` Run the last query (can be very long to run)

#### Example Usage:
- Run General Queries:
    - `python neo4j-queries.py <user> <password> -r run_queries`
- Run Specific Query (e.g., Query 1):
    - `python neo4j-queries.py <user> <password> -r run_queries -k 1`
- Run the Last Query:
    - `python neo4j-queries.py <user> <password> -r run_queries -t`
- Run Analysis Queries:
    - `python neo4j-queries.py <user> <password> -r run_analysis`

### PostgreSQL

`python postgres-queries.py -u <user> -p <password> -d <database> [OPTIONS]`