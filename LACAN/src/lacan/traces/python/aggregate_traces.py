import os, re, logging
import pandas as pd

header = ['dataset', 'family', 'algorithm', 'platform', 'run', 'duration','schedulerDelay','executorRunTime','executorCpuTime','executorDeserializeTime','executorDeserializeCpuTime','resultSerializationTime','jvmGCTime','resultSize','peakExecutionMemory','bytesRead','shuffleFetchWaitTime','shuffleTotalBytesRead','shuffleTotalBlocksFetched','shuffleLocalBlocksFetched','shuffleRemoteBlocksFetched','shuffleWriteTime', 'shuffleBytesWritten', 'fitTime']

def workload_aggregation(input_dir, output_file):
    header = ['dataset', 'family', 'algorithm', 'platform', 'run', 'duration','schedulerDelay','executorRunTime','executorCpuTime','executorDeserializeTime','executorDeserializeCpuTime','resultSerializationTime','jvmGCTime','resultSize','peakExecutionMemory','bytesRead','shuffleFetchWaitTime','shuffleTotalBytesRead','shuffleTotalBlocksFetched','shuffleLocalBlocksFetched','shuffleRemoteBlocksFetched','shuffleWriteTime', 'shuffleBytesWritten', 'fitTime']

    workloads = []
    r = re.compile("part.*")

    for d in os.listdir(input_dir):
        for f in os.listdir('{}/{}'.format(input_dir, d)):
            for a in os.listdir('{}/{}/{}'.format(input_dir, d, f)):
                for p in os.listdir('{}/{}/{}/{}'.format(input_dir, d, f, a)):
                    try:
                        filename = os.listdir('{}/{}/{}/{}/{}/run-0/platform-fit-metrics.csv'.format(input_dir, d, f, a, p))
                        filename = list(filter(r.match, filename))[0]
                        platform_df = pd.read_csv('{}/{}/{}/{}/{}/run-0/platform-fit-metrics.csv/{}'.format(input_dir, d, f, a, p, filename))
                        platform_df.drop(['jobGroup', 'launchTime', 'finishTime', 'host', 'index', 'taskLocality', 'speculative', 'successful', 'gettingResultTime', 'numUpdatedBlockStatuses', 'diskBytesSpilled', 'memoryBytesSpilled', 'recordsRead', 'recordsWritten', 'bytesWritten', 'shuffleRecordsWritten', 'executorId', 'jobId', 'stageId'], axis=1, inplace=True, errors='ignore')
                        platform_df = platform_df.agg(['sum'])
                        res = platform_df.values.tolist()[0]

                        applicative_filename = os.listdir('{}/{}/{}/{}/{}/run-0/applicative-metrics.csv'.format(input_dir, d, f, a, p))
                        applicative_filename = list(filter(r.match, applicative_filename))[0]
                        applicative_df = pd.read_csv('{}/{}/{}/{}/{}/run-0/applicative-metrics.csv/{}'.format(input_dir, d, f, a, p, applicative_filename)).transpose()
                        applicative_df.columns = applicative_df.iloc[0]
                        res.append(applicative_df['fitTime'].values[1])

                        res = [d, f, a, p, '0'] + res
                        workloads.append(res)
                    except:
                        logging.error('{}/{}/{}/{} Does not exist'.format(d, f, a, p))
                print('{} Done'.format(a))
        print('\n###\n{} Done\n###\n'.format(d))

    with open(output_file, 'w') as wf:
        workloads.insert(0, header)
        # print(workloads)
        workloads = [','.join(list(map(lambda y: str(y), x))) for x in workloads]
        workloads = '\n'.join(workloads)
        wf.write(workloads)


def job_aggregation(input_dir, output_file):
    header = ['dataset', 'family', 'algorithm', 'platform', 'run', 'jobId', 'duration','schedulerDelay','executorRunTime','executorCpuTime','executorDeserializeTime','executorDeserializeCpuTime','resultSerializationTime','jvmGCTime','resultSize','peakExecutionMemory','bytesRead','shuffleFetchWaitTime','shuffleTotalBytesRead','shuffleTotalBlocksFetched','shuffleLocalBlocksFetched','shuffleRemoteBlocksFetched','shuffleWriteTime', 'shuffleBytesWritten', 'fitTime']

    jobs = []
    r = re.compile("part.*")

    for d in os.listdir(input_dir):
        for f in os.listdir('{}/{}'.format(input_dir, d)):
            for a in os.listdir('{}/{}/{}'.format(input_dir, d, f)):
                for p in os.listdir('{}/{}/{}/{}'.format(input_dir, d, f, a)):
                    try:
                        filename = os.listdir('{}/{}/{}/{}/{}/run-0/platform-fit-metrics.csv'.format(input_dir, d, f, a, p))
                        filename = list(filter(r.match, filename))[0]
                        platform_df = pd.read_csv('{}/{}/{}/{}/{}/run-0/platform-fit-metrics.csv/{}'.format(input_dir, d, f, a, p, filename))
                        platform_df.drop(['jobGroup', 'launchTime', 'finishTime', 'host', 'index', 'taskLocality', 'speculative', 'successful', 'gettingResultTime', 'numUpdatedBlockStatuses', 'diskBytesSpilled', 'memoryBytesSpilled', 'recordsRead', 'recordsWritten', 'bytesWritten', 'shuffleRecordsWritten', 'executorId', 'stageId'], axis=1, inplace=True, errors='ignore')
                        platform_df = platform_df.groupby(['jobId']).agg(['sum'])
                        res = platform_df.values.tolist()

                        applicative_filename = os.listdir('{}/{}/{}/{}/{}/run-0/applicative-metrics.csv'.format(input_dir, d, f, a, p))
                        applicative_filename = list(filter(r.match, applicative_filename))[0]
                        applicative_df = pd.read_csv('{}/{}/{}/{}/{}/run-0/applicative-metrics.csv/{}'.format(input_dir, d, f, a, p, applicative_filename)).transpose()
                        applicative_df.columns = applicative_df.iloc[0]

                        index = platform_df.index.tolist()
                        res = list(map(lambda x: [d, f, a, p, '0'] + [x[0]] + x[1] + [applicative_df['fitTime'].values[1]], list(zip(index, res))))
                        
                        for line in res:
                            jobs.append(line)

                    except:
                        logging.error('{}/{}/{}/{} Does not exist'.format(d, f, a, p))
                print('{} Done'.format(a))
        print('\n###\n{} Done\n###\n'.format(d))

    with open(output_file, 'w') as wf:
        jobs.insert(0, header)
        jobs = [','.join(list(map(lambda y: str(y), x))) for x in jobs]
        jobs = '\n'.join(jobs)
        wf.write(jobs)



if __name__ == "__main__":
    workload_aggregation('https://gitlab.liris.cnrs.fr/sbouchen/LACAN-Data/tree/master/collected-traces/2019-05-26T11:06:34.518724', '../workload_traces/workloads.csv')
