#!/usr/bin/env python
# -*- coding: utf-8 -*-
##################################################
## Merging results of correlations between Application Performance
## Spark Condiguration Parameters.
##################################################
## LACAN is licensed under the LGPL license
##################################################
## Author: Sara Bouchenak (Sara.Bouchenak@insa-lyon.fr)
## Copyright: Copyright 2019, LACAN
## License: LGPL
## Version: 1.0
## Date: May 23, 2019
## Email: Sara.Bouchenak@insa-lyon.fr
##################################################

##################################################
## Usage:
## python mergecorrel.py ../../../../../results/2019-05-26T11:06:34.518724 train overall 4 0.75 0.10
## python mergecorrel.py ../../../../../results/2019-05-26T11:06:34.518724 test overall 4 0.75 0.10
##################################################

import csv
import sys
import os
import pandas as pd

##################################################

def row_count(filename):
    with open(filename) as in_file:
        return sum(1 for _ in in_file)

def get_last_csv_row(filename):
	rf = open(filename, "rt")
	rCSV = csv.reader(rf, delimiter=";")
	r = ''
	for r in rCSV:
		readCSV = csv.reader(readFile,delimiter=",")
	return r

##################################################

correl_dir = sys.argv[1] + "/correlations"
train_or_test = sys.argv[2]
overall_or_tasks = sys.argv[3]
k = int(sys.argv[4])
correl_threshold = float(sys.argv[5])
coeff_var_threshold = float(sys.argv[6])

if train_or_test == 'train':
	if overall_or_tasks == 'overall':
		output_file = sys.argv[1] + "/traces/Correl-TrainingTime-Overall-TopK-Threshold.csv"
	elif overall_or_tasks == 'tasks':
		output_file = sys.argv[1] + "/traces/Correl-TrainingTime-Tasks-TopK-Threshold.csv"

elif train_or_test == 'test':
	if overall_or_tasks == 'overall':
		output_file = sys.argv[1] + "/traces/Correl-PredictionTime-Overall-TopK-Threshold.csv"
	elif overall_or_tasks == 'tasks':
		output_file = sys.argv[1] + "/traces/Correl-PredictionTime-Tasks-TopK-Threshold.csv"

tmp_output_file = output_file 

tmp_writeFile = open(tmp_output_file, "wt")
tmp_writeCSV = csv.writer(tmp_writeFile,delimiter=";")
tmp_writeCSV.writerow( ["Workload id","Dataset","ML Algorithm","ML Algorithm Family","Configuration Parameter","Correlation",
"Correlation higher than correl threshold("+str(correl_threshold)+")","Config param with high correlation (if any)",'#correlated config parameters',
"#Workloads without correlation","#Workloads correlated with one config param","#Workloads correlated with several config params","Coefficient of variation of perf",
"Coefficient of variation of perf higher than coeff threshold("+str(coeff_var_threshold)+")"])
counter = 0
nb_workloads_without_correl = 0
nb_workloads_with_one_correl = 0
nb_workloads_with_several_correl = 0

without_correl = ''
with_one_correl = ''
with_several_correl = ''

for dir, sub_dir, files in os.walk(correl_dir):
	for file in files:
		if file.endswith('pearson.csv'):
			res_file = os.path.join(dir, file)
			if (train_or_test) in res_file and (overall_or_tasks) in res_file:
				counter += 1
				if ('DDF' in res_file):
					dataset = "DDF"
				elif ('DGS' in res_file):
					dataset = "DGS"
				elif ('DSS' in res_file):
					dataset = "DSS"
				elif ('DDR' in res_file):
					dataset = "DDR"
				elif ('DHG' in res_file):
					dataset = "DHG"
				
				if ('BKM' in res_file):
					algo = "BKM"
					fam = "clustering"
				elif ('KM' in res_file):
					algo = "KM"
					fam = "clustering"
				elif ('GMM' in res_file):
					algo = "GMM"
					fam = "clustering"
				elif ('DT' in res_file):
					algo = "DT"
					fam = "classification"
				elif ('MLP' in res_file):
					algo = "MLP"
					fam = "classification"
				elif ('BLR' in res_file):
					algo = "BLR"
					fam = "classification"
				elif ('LR' in res_file):
					algo = "LR"
					fam = "regression"
				elif ('RFR' in res_file):
					algo = "RFR"
					fam = "regression"
				elif ('GBT' in res_file):
					algo = "GBT"
					fam = "regression"
													
				readFile = open(res_file, "rt")
				readCSV = csv.reader(readFile,delimiter=",")
				nb_correl_conf_param = 0
				i = 0
				for row in readCSV:
					
					config_param = row[0]
					
					config_output_file = sys.argv[1] + "/traces/Config-Outputs/" + dataset + "/" + fam + "/" + algo + "/Config-Outputs-" + dataset + "-" + algo + "-" + config_param + ".csv"
					#print(config_output_file)
					try:
						reader = csv.reader(open(config_output_file), delimiter=';')
						last_line_number = row_count(config_output_file)
						l_row = get_last_csv_row(config_output_file)
						if train_or_test == 'train':
							coeff_var_str = l_row[14]
							coeff_var = float(coeff_var_str)
						elif train_or_test == 'test':
							coeff_var_str = l_row[18]
							coeff_var = float(coeff_var_str)
						else:
							print("Error: should specified \'train\' or \'test\' ")
												
						if coeff_var >= coeff_var_threshold:
							coeff_var_high = coeff_var_str
						else:
							coeff_var_high = ''
							
						#print(coeff_var_str,coeff_var_high)

					except FileNotFoundError as fnfe:
						print(fnfe)		
				
					
					if i < k:
						#config_param = row[0]
						correlation_ch = row[1]
						correlation = float(correlation_ch)
												
						if correlation < 0:
							correlation = correlation * (-1)
							
						if correlation >= correl_threshold:
							correl_higher_thresh = correlation
							conf_param_high_correl = config_param
							nb_correl_conf_param += 1 

						else:
							correl_higher_thresh = "N/A"
							conf_param_high_correl = ""
							
						if i == (k-1):
							print_nb_correl_conf_param = str(nb_correl_conf_param)
							if nb_correl_conf_param == 0:
								without_correl = '1'
								with_one_correl = ''
								with_several_correl = ''
							elif nb_correl_conf_param == 1:
								without_correl = ''
								with_one_correl = '1'
								with_several_correl = ''
							else:
								without_correl = ''
								with_one_correl = ''
								with_several_correl = '1'
							
							if nb_correl_conf_param == 0:
								nb_workloads_without_correl += 1
							elif nb_correl_conf_param == 1:
								nb_workloads_with_one_correl += 1
							else:
								nb_workloads_with_several_correl += 1
									
						else:
							without_correl = ''
							with_one_correl = ''
							with_several_correl = ''
							print_nb_correl_conf_param = ""
							
						tmp_writeCSV.writerow([counter,dataset,algo,fam,config_param,correlation,correl_higher_thresh,
						conf_param_high_correl,print_nb_correl_conf_param,without_correl,with_one_correl,with_several_correl,coeff_var_str,coeff_var_high])

					else:
						break
					i += 1
												
				readFile.close()

tmp_writeFile.close()

