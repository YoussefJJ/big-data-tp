import sys
import csv
  
# open input CSV file as source
# open output CSV file as result
with open(sys.argv[1], "r") as source:
    reader = csv.reader(source)
    next(reader)
    
    with open(sys.argv[2], "w") as result:
        writer = csv.writer(result)
        for r in reader:
            # Use CSV Index to remove a column from CSV
            #r[3] = r['year']
            writer.writerow((r[0],r[9]))
