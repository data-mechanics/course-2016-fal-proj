import csv

with open('communityHeatmapValues.txt') as fin, open('communityScatter.csv', 'w') as fout:
    o=csv.writer(fout)
    for line in fin:
        o.writerow(line.split())

with open('crimeHeatmapValues.txt') as fin, open('crimeScatter.csv', 'w') as fout:
    o=csv.writer(fout)
    for line in fin:
        o.writerow(line.split())

with open('propertyHeatMapValues.txt') as fin, open('propertyScatter.csv', 'w') as fout:
    o=csv.writer(fout)
    for line in fin:
        o.writerow(line.split())

"""communityList = []
crimeList = []
propertyList = []

with open('communityHeatmapValues.txt', 'r') as f:
	for line in f:
		line = line.join(',')
		

with open('crimeHeatmapValues.txt', 'r') as f:
	for line in f:
		line = line.split()
		crimeList += [[float(x) for x in line]]

with open('propertyHeatMapValues.txt', 'r') as f:
    for line in f:
    	line = line.split()
    	propertyList += [[float(x) for x in line]]

print(communityList)
"""


#[[x,y,z]]
