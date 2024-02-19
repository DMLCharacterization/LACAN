import pandas as pd
import numpy as np
import operator, sys, os
from scipy.stats import pearsonr


# argv[1] --> The platform      traces
# argv[2] --> The applicatives  traces
# argv[3] --> The configuration traces
# argv[4] --> The target        directory
# argv[5] --> The file that contains the platformIds for each configuration parameters. It should be in 
                # LACAN/LACAN:src/lacan/traces/workload_traces/platforms


def pearson_correlation(x, y):
    return pearsonr(x, y)[0]


# Read the traces
platform = pd.read_csv(sys.argv[1])
print("Platform Data read")
applicative = pd.read_csv(sys.argv[2])
print("applicative Data Read")
configuration = pd.read_csv(sys.argv[3])
print("Configuration Data Read")


# Filter down the traces to only the useful features
platform = platform[["dataset", "algorithm", "runId", "platformId", "duration", "family", "phase"]]
applicative = applicative[["transformTime", "fitTime", "dataset", "algorithm", "runId", "platformId"]]
configuration.drop(["spark.master", "spark.driver.cores", "spark.driver.memory"], inplace=True, axis=1, errors='ignore')

# Merge the traces into one complete dataframe
data = pd.merge(platform, applicative, on=['dataset', 'algorithm', 'runId', 'platformId'], how='inner')
print("First Join Done")
data = pd.merge(data,configuration, on=['platformId'], how='inner')
print("Second Join Done")

configurations = {}

with open(configurations_filepath, 'r') as f:
    for line in f.readlines():
        conf, values = line.split(',', 1)
        configurations[conf] = values.strip('\n ').split(',')



for t, p in [('duration', 'fit'), ('duration', 'transform'), ('fitTime', 'fit'), ('transformTime', 'transform')]:

    phaseFolder = 'train' if p == 'fit' else 'test'
    targetFolder = 'task-duration' if t == 'duration' else 'training-duration'

    for d in data.dataset.unique():
        for a in data.algorithm.unique():
            if d in ['drivface', 'drugs'] and a == 'GMM':
                continue
            print(d, a)
            res = []
            for c in data.columns:
                if c not in ['algorithm', 'family', 'dataset', 'phase', 'runId', 'platformId', 'duration', 'fitTime', 'transformTime']:
                    subdata = data[(data.dataset == d) & (data.algorithm == a) & (data.phase == p) & (data.platformId.isin(configurations[c]))]
                    x = list(subdata[c])
                    y = subdata[t]
                    res.append((c, pearson_correlation(x, y)))
            res = [(y[0], y[1]) for y in sorted([(x[0], x[1], abs(x[1])) for x in res], reverse=True, key=operator.itemgetter(2))]
            os.makedirs("{}/{}/{}/{}/{}".format(sys.argv[4], d, a, targetFolder, phaseFolder))
            with open("{}/{}/{}/{}/{}/pearson.csv".format(sys.argv[4], d, a, targetFolder, phaseFolder), "w") as f:
                f.write("\n".join(["{},{}".format(x[0], x[1]) for x in res]))




renamings = {'drivface':'DDF', 'drift': 'DGS', 'drugs': 'DDR', 'higgs': 'DHG', 'geomagnetic': 'DSS', 'KMeans': 'KM', 'BisectingKMeans': 'BKM', 'Tree': 'DT', 'Logistic': 'BLR', 'Linear': 'LR'}

for df in os.listdir(sys.argv[4]):
    for af in os.listdir("{}/{}".format(sys.argv[4], df)):
        os.rename("{}/{}/{}".format(sys.argv[4], df, af), "{}/{}/{}".format(sys.argv[4], df, renamings[af]))

    os.rename("{}/{}".format(sys.argv[4], df), "{}/{}".format(sys.argv[4], renamings[df]))
