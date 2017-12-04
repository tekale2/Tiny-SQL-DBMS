import csv

inputFile = "log2.txt"
output = "selectJoined3Tables.csv"

file = open(inputFile)
out = open(output, "w+")

writer = csv.writer(out)

row_break = [40, 80, 120, 160]
row_id = 0
diskIO = 0
diskTime = 0

writer.writerow(("Num Tuples","DiskIOs","Execution Time (ms)"))
for line in file:
	if "SELECT" in line:
		for l in file:
			if "Disk I/O" in l:
				diskIO = int(l.split()[-1])
			if "Query Exec Time" in l:
				diskTime = float(l.split()[-2])
				writer.writerow((row_break[row_id],diskIO,diskTime))
				row_id+=1
				break
file.close()
out.close()