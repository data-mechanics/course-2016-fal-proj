'''
Written by Jennifer Tsui
12/15/16

Purpose: Generate CSV files from heatmap text files.
Eventually assemble the csv we need for the scatter plot visualization by 
1) adding a title row (in the form of x1, y1, z1, ... , x3, y3, z3)
2) making each x1, y1, z1 correspond with latitude, longitude, and value
   for community scores, crime scores, and property scores.
'''
import csv

with open('text_and_csv/communityHeatmapValues.txt') as fin, open('communityScatter.csv', 'w') as fout:
    o=csv.writer(fout)
    for line in fin:
        o.writerow(line.split())

with open('text_and_csv/crimeHeatmapValues.txt') as fin, open('crimeScatter.csv', 'w') as fout:
    o=csv.writer(fout)
    for line in fin:
        o.writerow(line.split())

with open('text_and_csv/propertyHeatMapValues.txt') as fin, open('propertyScatter.csv', 'w') as fout:
    o=csv.writer(fout)
    for line in fin:
        o.writerow(line.split())
