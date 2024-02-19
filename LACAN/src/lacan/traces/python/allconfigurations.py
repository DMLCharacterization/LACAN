#!/usr/bin/env python
# -*- coding: utf-8 -*-
##################################################
## Listing all configurations considered in the experiments.
##################################################
## LACAN is licensed under the LGPL license
##################################################
## Author: Sara Bouchenak (Sara.Bouchenak@insa-lyon.fr)
## Copyright: Copyright 2019, LACAN
## License: LGPL
## Version: 1.0
## Date: May 28, 2019
## Email: Sara.Bouchenak@insa-lyon.fr
##################################################

##################################################
## Usage:
## python allconfigurations.py ../../../../../results/2019-05-26T11:06:34.518724 
##################################################

import json
import csv
import sys
import os
import operator

#print ("Input result directory: " + (sys.argv[1])) #root directory that contains result files
output_file = (sys.argv[1]+"/traces/AllConfigurations.csv")
#print ("Output file that lists all configurations considered in the experiments: " + output_file) #output file

writeFile = open(output_file+'-tmp', "wt")
writeCSV = csv.writer(writeFile,delimiter=";")

input_dir = (sys.argv[1]) + "/monitoring/DDF/classification/BLR"

#print (input_dir)

for dir, sub_dir, files in os.walk(input_dir):
	#print dir, sub_dir
	for file in files:
		if file.endswith('.json'):
			with open(dir+'/'+file) as json_file:
				data = json.load(json_file)
				platformId = data['platformId']
				writeCSV.writerow([data['platformId'], data['platform']['spark.executor.cores'], 
data['platform']['spark.serializer'], data['platform']['spark.io.compression.codec'], data['platform']['spark.reducer.maxSizeInFlight'],
data['platform']['spark.shuffle.io.preferDirectBufs'], data['platform']['spark.shuffle.spill.compress'],
data['platform']['spark.shuffle.compress'], data['platform']['spark.locality.wait'], data['platform']['spark.rdd.compress'],
data['platform']['spark.shuffle.file.buffer'], data['platform']['spark.executor.memory'], data['platform']['spark.storage.memoryFraction'] ])

writeFile.close()

data = csv.reader(open(output_file+'-tmp'),delimiter=';')
sortedlist = sorted(data, key=lambda column: int(column[0]))

with open(output_file, "w") as writeFile:
	writeCSV = csv.writer(writeFile, delimiter=';')
	writeCSV.writerow(['Platform Id', 'spark.executor.cores','spark.serializer','spark.io.compression.codec','spark.reducer.maxSizeInFlight','spark.shuffle.io.preferDirectBufs','spark.shuffle.spill.compress','spark.shuffle.compress','spark.locality.wait','spark.rdd.compress','spark.shuffle.file.buffer','spark.executor.memory','spark.storage.memoryFraction'])
	for row in sortedlist:
		writeCSV.writerow(row)

writeFile.close()
os.remove(output_file+'-tmp')
