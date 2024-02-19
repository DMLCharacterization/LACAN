#!/usr/bin/env python
# -*- coding: utf-8 -*-
##################################################
## Produce merged monitored output variables and their statistics for all
## ML workloads (i.e., dataset and ML algorithm) when varying
## each configuration parameter.
##################################################
## LACAN is licensed under the LGPL license
##################################################
## Author: Sara Bouchenak (Sara.Bouchenak@insa-lyon.fr)
## Copyright: Copyright 2019, LACAN
## License: LGPL
## Version: 1.0
## Date: May 29, 2019
## Email: Sara.Bouchenak@insa-lyon.fr
##################################################

##################################################
## Usage:
## python config2outputs4allworkloads.py <result directory>  <overall or tasks>
## Produces:
## Files named Config-Outputs-<dataset>-<ML algorithm>-<configuration parameter>.csv in <result directory>/traces directory 
## Example:
## python config2outputs4allworkloads.py ../../../../../results/2019-05-26T11:06:34.518724 
##################################################

import csv
import sys
import os
import statistics
from statistics import mean, stdev

nested_program="python config2outputs.py" + sys.argv[1]

dataset_list = ["DDF", "DDR", "DGS", "DHG", "DSS"] 
clustering_algo_list = ["KM", "BKM",  "GMM"]
classification_algo_list = ["DT", "MLP", "BLR"]
regression_algo_list = ["LR", "RFR", "GBT"]
ml_algo_list = clustering_algo_list + classification_algo_list + regression_algo_list
config_param_list = ["spark.executor.cores", "spark.serializer", "spark.io.compression.codec", "spark.reducer.maxSizeInFlight", 
"spark.shuffle.io.preferDirectBufs", "spark.shuffle.spill.compress", "spark.shuffle.compress", "spark.locality.wait", 
"spark.rdd.compress", "spark.shuffle.file.buffer","spark.executor.memory", "spark.storage.memoryFraction" ]

target_sub_dir = "traces/Config-Outputs"
target_dir = sys.argv[1] + "/" + target_sub_dir
overall_or_tasks = sys.argv[2]
try:
	os.mkdir(target_dir)
except FileExistsError as fee:
	# print(fee)
	pass

for d in dataset_list:
	target_sub_dir = "traces/Config-Outputs"
	target_dir = sys.argv[1] + "/" + target_sub_dir
	dataset_dir = target_dir + "/" + d
	target_sub_dir = target_sub_dir + "/" + d
	try:
		os.mkdir(dataset_dir)
		os.mkdir(dataset_dir+"/clustering")
		os.mkdir(dataset_dir+"/classification")
		os.mkdir(dataset_dir+"/regression")

	except FileExistsError as fee:
		# print(fee)
		pass
	
	for a in ml_algo_list:
		target_sub_dir = "traces/Config-Outputs"
		target_sub_dir = target_sub_dir + "/" + d

		if a in clustering_algo_list:
			target_sub_dir = target_sub_dir + "/clustering/" + a
			ml_algo_dir = dataset_dir + "/clustering/" + a
		elif a in classification_algo_list:
			target_sub_dir = target_sub_dir + "/classification/" + a
			ml_algo_dir = dataset_dir + "/classification/" + a
		elif a in regression_algo_list:
			target_sub_dir = target_sub_dir + "/regression/" + a
			ml_algo_dir = dataset_dir + "/regression/" + a
			
		try:
			os.mkdir(ml_algo_dir)
		except FileExistsError as fee:
			#print(fee)
			pass
		
		for c in config_param_list:
			nested_program="python config2outputs.py " + sys.argv[1] + " " + d + " " + a + " " + c + " " + overall_or_tasks + " " + target_sub_dir
			os.system(nested_program)
