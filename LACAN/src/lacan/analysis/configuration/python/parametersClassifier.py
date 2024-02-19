import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split 
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import f1_score, confusion_matrix
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import OneHotEncoder
from tpot import TPOTClassifier
import sys
import getopt
import cmd
import argparse



options, remainder = getopt.gnu_getopt(sys.argv[1:], 'c:a:d:l:h:i', ['correlation=', 
                                                             'applicative',
                                                             'dataset=',
                                                             'algorithm=',
                                                               'help=',
                                                               'prediction=',
                                                                 ])

correlation_input_file_path = '' #/tmp/regression_correlation.csv'
applicative_input_file_path = '' #/tmp/applicative.csv'
dataset_input_file_path = '' #/tmp/datasets.csv'
algorithm_input_file ='' #/tmp/algorithms.csv'
prediction_input_file='' 
correlation = pd.DataFrame() 
applicative = pd.DataFrame() 
dataset = pd.DataFrame() 
algorithm = pd.DataFrame()  
data = pd.DataFrame() 


class DefaultClassifier:        
    def predict(self,data):
        ret = [0 for x in range(data.shape[0])]
        return ret

classifier= {
    'spark.shuffle.compress':DefaultClassifier(),
    'spark.io.compression.codec':DefaultClassifier(),
    'spark.shuffle.file.buffer':DefaultClassifier(),
    'spark.storage.memoryFraction':DefaultClassifier(),
    'spark.shuffle.io.preferDirectBufs':DefaultClassifier(),
    'spark.rdd.compress':DefaultClassifier(),
    'spark.executor.memory':DefaultClassifier(),
    'spark.executor.cores':DefaultClassifier(),
    'spark.reducer.maxSizeInFlight':DefaultClassifier(),
    'spark.serializer':DefaultClassifier(),
    'spark.shuffle.spill.compress':DefaultClassifier(),
    'spark.executor.instances':DefaultClassifier(),
    'spark.locality.wait':DefaultClassifier(),
}



configurationID= {
    'spark.shuffle.compress':[0,12],
    'spark.io.compression.codec':[0,7],
    'spark.shuffle.file.buffer':[0,13,14],
    'spark.storage.memoryFraction':[0,17,18],
    'spark.shuffle.io.preferDirectBufs':[0,15],
    'spark.rdd.compress':[0,8],
    'spark.executor.memory':[0,5,6],
    'spark.executor.cores':[0,1,2],
    'spark.reducer.maxSizeInFlight':[0,10,9],
    'spark.serializer':[0,11],
    'spark.shuffle.spill.compress':[0,16],
    'spark.executor.instances':[0],
    'spark.locality.wait':[0],
}

label_encoder= {
    'category':'N/A'
}
correlation_threshold = 70 # sys.argv[1]
boost_threshlod = 5 # sys.argv[2]
top_k = 4 #sys.argv[3]

#The default Classifier



def readingFile(correlation_input_file_path,applicative_input_file_path,dataset_input_file_path,algorithm_input_file):

    #reading  data from files
    global correlation 
    correlation = pd.read_csv(correlation_input_file_path)
    global applicative 
    applicative = pd.read_csv(applicative_input_file_path)
    global dataset 
    dataset = pd.read_csv(dataset_input_file_path)
    global algorithm
    algorithm = pd.read_csv(algorithm_input_file)
    print(correlation['parameter'].unique())

def merginData(correlation,
    applicative,
    dataset,
    algorithm):
    #merging data
    data = pd.merge(correlation,applicative,on=['algorithm','dataset'])
    data = data.drop(columns=['r2','mae', 'rmse', 'mse','splitter', 'silhouette', 'weightedRecall','weightedPrecision', 'accuracy', 'f1','testCount','runId','trainCount','features','experimentId','family','transformTime'])
    data = data.groupby(['platformId','dataset','algorithm','parameter'],as_index=False).agg({'a':'first', 'b':'first', 'corr':'mean', 'fitTime':'mean'})
    data = pd.merge(data,dataset,on=['dataset'])
    data = pd.merge(data,algorithm,on=['algorithm'])
    return data
        
def dropna(data):
    return data.dropna()

def computing_pente(row):
    ret=0
    if (np.abs(row['corr']*100)> correlation_threshold):
         ret  =  np.abs(row['a']/row['b'])*100
    return ret

def correlation_filtering(data):
    data['pente'] = data.apply(computing_pente,axis=1) 
    return data

def boost_filtering(data):
    data['correlated'] =  data['pente'].apply(lambda x: '1' if x > boost_threshlod else '0')
    return data

def computing_classifier(data):
    parameterArray = data.parameter.unique()
    global classifier
    global label_encoder
    global configurationID
    tempData = data.drop(columns=['a','b','corr','pente','fitTime','float','string','category_type','dataset','algorithm']).copy() #List of columns ot delete that are not used in the classifier. 
    le = LabelEncoder()
   # tempData.to_csv(r'/tmp/saving.csv')
    tempData['category'] = le.fit_transform(tempData.category)
    for parameter in parameterArray:
        print("Processing " + parameter )
        usedData = tempData[(tempData['parameter'] == parameter)].copy()
        usedData = usedData[(usedData['platformId'].isin(configurationID[parameter]))]
        X = usedData.drop(columns=['correlated','platformId','parameter'])
        y = usedData['correlated'] # Label for our classifier, specify if yes or not a workload is impacted by the parameter
        #X['dataset'] = le.fit_transform(X.dataset)
        #X['algorithm'] = le.fit_transform(X.algorithm)
        X = X.astype(float)
        y = y.astype(float)
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=True, stratify=y.values,random_state=42)
        tpot = TPOTClassifier(generations=5, population_size=20, verbosity=2)
        tpot.fit(X_train, y_train)
        #print(tpot.score(X_test, y_test))
        #tpot.export(''+parameter+'classifier.py')
        #out = tpot.predict(X_test)
        classifier.update({parameter:tpot})
        label_encoder.update({'category':le})
    return classifier

def classify_from_file(file_path,top_k):
    global classifier
    tempData =  pd.read_csv(file_path)
    le = label_encoder['category']
    X = tempData.copy()
    X['category'] = le.transform(X.category)
    #print(classifier)
    for parameter, tempclassifier in classifier.items(): 
        y_pred = tempclassifier.predict(X)
        tempData[parameter] = y_pred
        #if(out[0] == 1):
        #    print(parameter)        
    tempData.to_csv(file_path+'out')

def get_opt(options):       
    global  correlation_input_file_path
    global applicative_input_file_path
    global dataset_input_file_path
    global algorithm_input_file
    global prediction_input_file
    ret =0
    for opt, arg in options:
        if opt in ('-h', '--help'):
            print('option,   arguments')
            print('-c,       --correlation')
            print('-a,       --applicative')
            print('-d,       --dataset')
            print('-l,       --algorithm')
            print('-h,       --help')
            return ret
        elif opt in ('-c', '--correlation'):
            correlation_input_file_path = arg
        elif opt in ('-a', '--applicative'):
            applicative_input_file_path = arg
        elif opt in ('-d', '--dataset'):
            dataset_input_file_path = arg
        elif opt in ('-l', '--algorithm'):
            algorithm_input_file = arg
            print(arg)
        elif opt in ('-i', '--prediction'):
            prediction_input_file = arg
            print(arg)
            
    ret =1
    return ret

class myCmd(cmd.Cmd):

    def do_path(self, line):
        print(line)
        classify_from_file(line,4)


    def do_EOF(self, line):
        return True
           
        
if __name__ == '__main__':
    ret=get_opt(options)
    if(ret==0):
        exit()
    readingFile(correlation_input_file_path,applicative_input_file_path,dataset_input_file_path,algorithm_input_file)
    data = merginData(correlation, applicative, dataset, algorithm)
    data = dropna(data)
    data = correlation_filtering(data)
    data = boost_filtering(data)
    classifier = computing_classifier(data)
    classify_from_file(prediction_input_file,4)
   # myCmd().cmdloop()

 
