#!/usr/bin/env python
# -*- coding: utf-8 -*-
##################################################
## Merge monitored output variables and their statistics for a given
## ML workload (i.e., dataset and ML algorithm) when varying
## a given configuration parameters.
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
## python config2outputs.py <result directory> <dataset> <ML algorithm> <configuration parameter> <overall or tasks> <target sub-directory>
## Produces:
## File named Config-Outputs-<dataset>-<ML algorithm>-<configuration parameter>.csv in <target sub-directory> in <result directory> 
## Example:
## python config2outputs.py ../../../../../results/2019-05-26T11:06:34.518724 DGS DT spark.executor.cores overall traces/DGS/classification/DT
##################################################

import csv
import sys
import os
import statistics
from statistics import mean, stdev

dataset = sys.argv[2] #considered dataset
ml_algo = sys.argv[3] #considered dataset
config_param = sys.argv[4] #considered configuration parameter
overall_or_tasks = sys.argv[5] #overall or tasks

configurations_file = sys.argv[1]+"/traces/Configurations-" + config_param + ".csv"

try:
   readConfigFile = open(configurations_file, "rt")

except FileNotFoundError as fnfe:
	print(fnfe)
	sys.exit()
	
readConfigCSV = csv.reader(readConfigFile,delimiter=";")

if (ml_algo == "KM") or (ml_algo == "BKM") or (ml_algo == "GMM"):
	# Clustering algorithms
	ml_algo_familly = "clustering"
	accuracy_output = "Silhouette"
elif (ml_algo == "DT") or (ml_algo == "MLP") or (ml_algo == "BLR"):
	# Classification algorithms
	ml_algo_familly = "classification"
	accuracy_output = "F-score "
elif (ml_algo == "LR") or (ml_algo == "RFR") or (ml_algo == "GBT"):
	# Regression algorithms
	ml_algo_familly = "regression"
	accuracy_output = "R-squared"
else:
	print ("Specified (wrong?) ML algorithm: " + ml_algo)
	accuracy_output = " "
	ml_algo_familly = " "

header_written = False

overall_training_times_set = set()
overall_prediction_times_set = set()
overall_prediction_accuracy_set = set()

nb_config = 0
row1 = next(readConfigCSV) 
config_param_with_unit = row1[1]

for row in readConfigCSV:
	config_id = row[0]
	config_value = row[1]
	monitoring_dir = sys.argv[1]+"/monitoring/" + dataset + "/" + ml_algo_familly + "/" + ml_algo + "/platform-" + config_id
	
	run_id = 0
	training_time =''
	prediction_time = ''
	prediction_accuracy = ''
	nb_training_records = ''
	nb_prediction_records = ''

	training_times_set = set()
	prediction_times_set = set()
	prediction_accuracy_set = set()
	
	at_least_one_successful_run = False
	
	try:
		for filename in os.listdir(monitoring_dir):
			if filename.startswith('run-'):
			    monitoring_dir2 = monitoring_dir+"/"+filename
			    #check if run was successful or not
			    if "error.txt" in os.listdir(monitoring_dir2):
			    	success_run = False
			    else:
			    	if (overall_or_tasks == "overall"):
			    		monitoring_subdir = monitoring_dir2+"/applicative-metrics.csv"
			    	else:
			    		print("TODO")
			    	
			    	success_run = False
			    	for subfilename in os.listdir(monitoring_subdir):
			    		#double-check that run was successful
			    		if subfilename.startswith('_SUCCESS'):
			    			success_run=True
			    			at_least_one_successful_run = True
	    				elif subfilename.startswith('part-'):
	    					monitoring_file = monitoring_subdir + "/" + subfilename
	    					readMonitoringFile=open(monitoring_file,"rt")
	    					readMonitoringCSV = csv.reader(readMonitoringFile,delimiter=",")
	    					next(readMonitoringCSV)
	    					for metric_value_row in readMonitoringCSV:
	    					    if metric_value_row[0] == "fitTime":
	    					        training_time = metric_value_row[1]
	    					        training_times_set.add(float(training_time))
	    					    elif metric_value_row[0] == "transformTime":
	    					    	prediction_time = metric_value_row[1]
	    					    	prediction_times_set.add(float(prediction_time))
	    					    elif metric_value_row[0] == "trainCount":
	    					        nb_training_records = metric_value_row[1]
	    					    elif metric_value_row[0] == "testCount":
	    					        nb_prediction_records = metric_value_row[1]
	    					    elif ((ml_algo_familly == "clustering") and (metric_value_row[0] == "silhouette")) or ((ml_algo_familly == "classification") and (metric_value_row[0] == "f1")) or ((ml_algo_familly == "regression") and (metric_value_row[0] == "r2")):
	    					        prediction_accuracy = metric_value_row[1]
	    					        prediction_accuracy_set.add(float(prediction_accuracy))
			    if success_run == True:
			    	if header_written == False:
			    		header_written = True
			    					    		
			    		output_file = sys.argv[1] +"/" + sys.argv[6] + "/Config-Outputs-" + dataset + "-" + ml_algo + "-" + config_param + ".csv"
			    		writeFile = open(output_file, "wt")
			    		writeCSV = csv.writer(writeFile,delimiter=";")
			    		writeCSV.writerow(["Dataset", "ML Algorithm", "ML Algorithm Family", "Platform Id", "Rund Id", config_param_with_unit,
			    		"Traning time (ms)", "Prediction time (ms)", "Prediction accuracy - " + accuracy_output,
			    		"#Tarining records", "#Prediction records",
			    		"Mean training time over multiple runs of same config (ms)", "Coefficient of variation of training time over multiple runs of same config", 
			    		"Mean training time over multiple configurations (ms)", "Coefficient of variation of training time over over multiple configurations",
			    		"Mean prediction time over multiple runs of same config (ms)", "Coefficient of variation of prediction time over multiple runs of same config", 
			    		"Mean prediction time over multiple configurations (ms)", "Coefficient of variation of prediction time over over multiple configurations", 
			    		"Mean prediction accuracy over multiple runs of same config", "Coefficient of variation of prediction accuracy over multiple runs of same config", 
			    		"Mean prediction accuracy over multiple configurations (ms)", "Coefficient of variation of prediction accuracy over over multiple configurations"])
			    				    		
			    	writeCSV.writerow([dataset, ml_algo, ml_algo_familly, config_id, run_id, config_value, training_time, prediction_time, prediction_accuracy, int(float(nb_training_records)), int(float(nb_prediction_records)), "", "", "", "", "", "", "", "", "", "", "", ""])
	    				
			    run_id += 1
			    
	except FileNotFoundError as fnfe:
		print(fnfe)
		sys.exit()


	if at_least_one_successful_run == True:
		# calculate mean and coefficient of variation over various runs on the same configuration value
		mean_training_time = statistics.mean(training_times_set)
		mean_prediction_time = statistics.mean(prediction_times_set)
		mean_prediction_accuracy = statistics.mean(prediction_accuracy_set)
		
		overall_training_times_set.add(mean_training_time)
		overall_prediction_times_set.add(mean_prediction_time)
		overall_prediction_accuracy_set.add(mean_prediction_accuracy)
		
		if len(training_times_set) > 1:
			stdev_training_time = statistics.stdev(training_times_set)
		else:
			stdev_training_time = 0
		if len(prediction_times_set) > 1:
			stdev_prediction_time = statistics.stdev(prediction_times_set)
		else:
			stdev_prediction_time = 0
		if len(prediction_accuracy_set) > 1:
			stdev_prediction_accuracy = statistics.stdev(prediction_accuracy_set)
		else:
			stdev_prediction_accuracy = 0
	
		coeff_variation_training_time = stdev_training_time / mean_training_time
		coeff_variation_prediction_time = stdev_prediction_time / mean_prediction_time
		coeff_variation_prediction_accuracy = stdev_prediction_accuracy / mean_prediction_accuracy
		
		writeCSV.writerow([dataset, ml_algo, ml_algo_familly, config_id, (run_id-1), config_value, "", "", "", "", "", 
		mean_training_time, coeff_variation_training_time, "", "", 
		mean_prediction_time, coeff_variation_prediction_time, "", "", 
		mean_prediction_accuracy, coeff_variation_prediction_accuracy, "", ""])
	
	nb_config += 1

if at_least_one_successful_run == True:
	overall_mean_training_time = statistics.mean(overall_training_times_set)
	overall_mean_prediction_time = statistics.mean(overall_prediction_times_set)
	overall_mean_prediction_accuracy = statistics.mean(overall_prediction_accuracy_set)
	
	if len(overall_training_times_set) > 1:
		overall_stdev_training_time = statistics.stdev(overall_training_times_set)
	else:
		overall_stdev_training_time = 0
	if len(overall_prediction_times_set) > 1:
		overall_stdev_prediction_time = statistics.stdev(overall_prediction_times_set)
	else:
		overall_stdev_prediction_time = 0
	if len(overall_prediction_accuracy_set) > 1:
		overall_stdev_prediction_accuracy = statistics.stdev(overall_prediction_accuracy_set)
	else:
		overall_stdev_prediction_accuracy = 0
	
	overall_coeff_variation_training_time = overall_stdev_training_time / overall_mean_training_time
	overall_coeff_variation_prediction_time = overall_stdev_prediction_time / overall_mean_prediction_time
	overall_coeff_variation_prediction_accuracy = overall_stdev_prediction_accuracy / overall_mean_prediction_accuracy
	
	writeCSV.writerow([dataset, ml_algo, ml_algo_familly, "", "", "", "", "", "", "", "", 
	"", "", overall_mean_training_time, overall_coeff_variation_training_time, 
	"", "", overall_mean_prediction_time, overall_coeff_variation_prediction_time, 
	"", "", overall_mean_prediction_accuracy, overall_coeff_variation_prediction_accuracy])
												
readConfigFile.close()
if header_written == True:
	writeFile.close()

#print("Produced output file: " + output_file)
