import csv
from sys import argv

if __name__ == '__main__':
	argv = argv[1:]
	if len(argv) != 1: argv = ['pokemon.csv']
	
	with open(argv[0], 'r') as f:
		reader = csv.DictReader(f)
		new_rows = []
		for row in reader:
			# if capture_rate is not a single number, split the row into multiple rows
			try:
				int(row['capture_rate'])
				new_rows.append(row)
			except ValueError:
				vals = row['capture_rate'].split(' ')
				for i in range(len(vals)):
					vals[i] = ''.join(c for c in vals[i] if c.isdigit())
				vals = [val for val in vals if val]
				i = 0
				for val in vals:
					i += 1
					new_row = row.copy()
					# to avoid duplicate pokedex numbers
					# works because there are less than 1000 pokemon
					new_row['name'] = new_row['name'] + '_' + str(i)
					new_row['pokedex_number'] = int(
			 			new_row['pokedex_number']
					) + 1000 * vals.index(val)
					new_row['capture_rate'] = val
					new_rows.append(new_row)
	with open(argv[0], 'w') as f:
		data = csv.DictWriter(f, reader.fieldnames)
		data.writeheader()
		data.writerows(new_rows)