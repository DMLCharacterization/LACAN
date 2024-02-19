#!/usr/bin/env python
# -*- coding: utf-8 -*-
##################################################
## Listing all values considered for a given configuration parameter.
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
## python configofoneparam.py ../../../../../results/2019-05-26T11:06:34.518724 spark.executor.cores
## Or:
## python configofoneparam.py ../../../../../results/2019-05-26T11:06:34.518724 spark.executor.memory
##################################################

import csv
import sys
import os

input_file = sys.argv[1]+"/traces/AllConfigurations.csv"
#print "Input file: " + input_file #input file
param = sys.argv[2] #considered configuration parameter

readFile = open(input_file, "rt")
readCSV = csv.reader(readFile,delimiter=";")

output_file = sys.argv[1]+"/traces/Configurations-" + param + ".csv"
writeFile = open(output_file, "wt")
writeCSV = csv.writer(writeFile,delimiter=";")
#handle parameter units if necessary
if (param == "spark.executor.memory" or param == "spark.reducer.maxSizeInFlight" or param == "spark.shuffle.file.buffer"):
	writeCSV.writerow(["Platform Id", param+" (KB)"])
elif param == "spark.locality.wait":
	writeCSV.writerow(["Platform Id", param+" (ms)"])
else:
	writeCSV.writerow(["Platform Id", param])

row_nb = 0
column_nb = 1
param_value_set = set()
for row in readCSV:
	if row_nb == 0:
		while row[column_nb] != param:
			column_nb += 1

		# Here, row[column_nb] == param
	else:
		param_value = row[column_nb]
		
				
		if param_value not in param_value_set:
			platform_id = row[0]

			#handle parameter units if necessary
			if (param == "spark.executor.memory" or param == "spark.reducer.maxSizeInFlight" or param == "spark.shuffle.file.buffer"):
				str_len = len(param_value)
				param_value_wo_unit = param_value[:str_len-1]
				unit = param_value[str_len - 1]
				int_param_value = param_value_wo_unit
				if (unit == 'm' or unit == 'M'):
					int_param_value = (int(param_value_wo_unit) * 1024) #convert MB to KB
				elif (unit == 'g' or unit == 'G'):
					int_param_value = (int(param_value_wo_unit) * 1024 * 1024) #convert GB to KB
					
				writeCSV.writerow([platform_id, int_param_value])
				param_value_set.add(param_value)
			elif param == "spark.locality.wait":
				str_len = len(param_value)
				unit = param_value[str_len-2:str_len]
				if (unit == 'ms'):
					param_value_wo_unit = param_value[:str_len-2]
					int_param_value = int(param_value_wo_unit)
				elif unit.endswith('s'):
					param_value_wo_unit = param_value[:str_len-1]
					int_param_value = int(param_value_wo_unit) * 1000 #convert seconds to milliseconds

				writeCSV.writerow([platform_id, int_param_value])
				param_value_set.add(param_value)
			else:
				writeCSV.writerow([platform_id, param_value])
				param_value_set.add(param_value)
		
	row_nb += 1

readFile.close()
writeFile.close()

#print ("Produced output file: " + output_file)
