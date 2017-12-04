import csv

inpath = "./data/Customer.csv"
inpath2 = "./data/Address.csv"
outfile = "test.txt"

output_str = "INSERT INTO customer (name, email, contact) VALUES ("
output_str2 = "INSERT INTO address (street, city, state, zipcode) VALUES ("

NUM_ROWS = 201

Query = "SELECT * FROM customer, address\n"
row_break = [5, 10, 15, 20, 50, 75, 100, 125, 150, 180, 200]
row_break = set(row_break)

file = open(inpath)
file2  = open(inpath2)
out = open(outfile,"w+")
readCSV = csv.reader(file, delimiter=',')
readList = list(csv.reader(file2, delimiter=','))

curr = 0
out.write("CREATE TABLE customer (name STR20, email STR20, contact STR20)\n")
out.write("CREATE TABLE address (street STR20, city STR20, state STR20, zipcode INT)\n")
for row in readCSV:
	outStr = output_str+"\""+row[0]+"\", \""+row[1]+"\", \""+row[2]+"\")\n"
	out.write(outStr)
	outStr = output_str2+"\""+readList[curr][0]+"\", \""+\
	readList[curr][1]+"\", \""+readList[curr][2]+"\", "+readList[curr][3]+")\n"
	out.write(outStr)
	curr+=1
	if curr in row_break:
		out.write(Query)
out.close()
file.close()