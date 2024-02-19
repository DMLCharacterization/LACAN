from functools import partial
from typing import Iterable, Callable, NamedTuple, Dict, TypeVar

import pandas as pd
import numpy as np


def load_metrics(path: str, **kwargs) -> pd.DataFrame:

    metrics = pd.read_csv(path, **kwargs)

    localities = {
        0: 'PROCESS_LOCAL',
        1: 'NODE_LOCAL',
        2: 'RACK_LOCAL',
        3: 'NO_PREF',
        4: 'ANY'
    }  # TODO

    metrics['taskLocality'] = pd.Categorical(metrics['taskLocality'].map(localities), categories=localities.values())

    for category in ['dataset', 'family', 'algorithm', 'host', 'jobGroup']:

        metrics[category] = pd.Categorical(metrics[category])

    #metrics = metrics.set_index(['experimentId', 'taskId'], drop=False, verify_integrity=True)

    return metrics.sort_index().reset_index(drop=True)


def preprocessing_metrics(metrics: pd.DataFrame) -> pd.DataFrame:

    metrics['duration'] = metrics['finishTime'] - metrics['launchTime']

    times = ['executorRunTime', 'executorDeserializeTime', 'resultSerializationTime', 'gettingResultTime']
    total = sum([metrics[time] for time in times])
    metrics['schedulerDelay'] = metrics['duration'] - total

    metrics['executorCpuUsage'] = metrics['executorCpuTime'] / metrics['executorRunTime']

    metrics['executorDeserializeCpuUsage'] = metrics['executorDeserializeCpuTime'] / metrics['executorDeserializeTime']

    metrics['shuffleWriteRate'] = metrics['shuffleBytesWritten'] / metrics['shuffleWriteTime']

    return metrics


Report = Dict[str, bool]


class Audit(NamedTuple):
    success: int
    failure: int
    report: Report


TYPE = TypeVar('TYPE')


def perform_audit(data: TYPE, rules: Iterable[Callable[[TYPE], bool]]) -> Audit:

    success, failure = 0, 0
    report = {}

    for rule in rules:

        result = rule(data)

        if result:
            success += 1
        else:
            failure += 1

        if isinstance(rule, partial):
            name = str(rule.func.__name__)
        else:
            name = str(rule.__name__)

        report[name] = result

    return Audit(success=success, failure=failure, report=report)


def print_audit(audit: Audit) -> None:

    print('-' * 47 + 'REPORT' + '-' * 47)

    for name, result in audit.report.items():

        if result:
            show = '+ SUCCESS'
        else:
            show = '- FAILURE'

        print('{result}: {name}'.format(name=name, result=show))

    print('-' * 100)

    total = audit.success + audit.failure

    print('success: {success} ({percentage:.2f}%)'.format(
        success=audit.success,
        percentage=audit.success / total * 100
    ))

    print('failure: {failure} ({percentage:.2f}%)'.format(
        failure=audit.failure,
        percentage=audit.failure / total * 100
    ))

    print('-' * 100)


def identifier_columns_are_categoricals(metrics: pd.DataFrame) -> bool:

    return (metrics.dtypes[metrics.columns[metrics.columns.str.contains('Id')]] == 'category').all()


def tasks_writing_for_shuffle_are_process_local(metrics: pd.DataFrame) -> bool:

    localities = metrics[metrics['shuffleWriteTime'] != 0]['taskLocality']

    return (localities.value_counts().drop('PROCESS_LOCAL') == 0).all()


def shuffle_bytes_written_is_equivalent_to_shuffle_records_written(metrics: pd.DataFrame) -> bool:

    return ((metrics['shuffleBytesWritten'] != 0) == (metrics['shuffleRecordsWritten'] != 0)).all()


def shuffle_bytes_written_is_equivalent_to_shuffle_write_time(metrics: pd.DataFrame) -> bool:

    return ((metrics['shuffleBytesWritten'] != 0) == (metrics['shuffleWriteTime'] != 0)).all()


def train_count_is_greater_than_or_equal_to_test_count(metrics: pd.DataFrame) -> bool:

    return (metrics['trainCount'] >= metrics['testCount']).all()


def tasks_are_successful(metrics: pd.DataFrame) -> bool:

    return metrics['successful'].all()


def tasks_fetching_shuffle_blocks_are_not_process_local(metrics: pd.DataFrame) -> bool:

    return ((metrics['shuffleTotalBlocksFetched'] != 0) == (metrics['taskLocality'] != 'PROCESS_LOCAL')).all()


def maximum_number_of_executors_per_node_is_respected(metrics: pd.DataFrame, number: int) -> bool:

    return metrics.groupby(['experimentId', 'host'])['executorId'].nunique().max() == number


def executor_run_time_is_greater_than_or_equal_to_cpu_time(metrics: pd.DataFrame) -> bool:

    return (metrics['executorRunTime'] >= metrics['executorCpuTime']).all()


def executor_deserialize_run_time_is_greater_than_or_equal_to_cpu_time(metrics: pd.DataFrame) -> bool:

    return (metrics['executorDeserializeTime'] >= metrics['executorDeserializeCpuTime']).all()


def number_of_nodes_is_respected(metrics: pd.DataFrame, number: int) -> bool:

    return metrics['host'].nunique() == number


def all_nodes_are_used(metrics: pd.DataFrame) -> bool:

    return (metrics.groupby('experimentId')['host'].nunique() == metrics['host'].nunique()).all()


def jobs_stages_and_tasks_are_sequential(metrics: pd.DataFrame) -> bool:

    def is_sequential(series: pd.Series) -> bool:

        series = series[series != series.shift()]

        return (np.diff(series) == 1).all()

    metrics = metrics.sort_values(['experimentId', 'launchTime', 'taskId']).groupby('experimentId')

    jobs = metrics['jobId'].apply(is_sequential)
    stages = metrics['stageId'].apply(is_sequential)
    tasks = metrics['taskId'].apply(is_sequential)

    return jobs.all() and stages.all() and tasks.all()


def minimum_f1_score_is_respected(metrics: pd.DataFrame, score: float) -> bool:

    return metrics['f1'].dropna().min() >= score


def minimum_r2_score_is_respected(metrics: pd.DataFrame, score: float) -> bool:

    return metrics['r2'].dropna().min() >= score


def minimum_number_of_algorithms_per_family_is_respected(metrics: pd.DataFrame, number: int) -> bool:

    return metrics.groupby('family')['algorithm'].nunique().min() >= number


def minimum_number_of_algorithms_per_dataset_is_respected(metrics: pd.DataFrame, number: int) -> bool:

    return metrics.groupby('dataset')['algorithm'].nunique().min() >= number


def minimum_number_of_datasets_per_algorithm_is_respected(metrics: pd.DataFrame, number: int) -> bool:

    return metrics.groupby('algorithm')['dataset'].nunique().min() >= number


def records_written_is_equivalent_to_bytes_written(metrics: pd.DataFrame) -> bool:

    return ((metrics['recordsWritten'] != 0) == (metrics['bytesWritten'] != 0)).all()


def records_read_is_equivalent_to_bytes_read(metrics: pd.DataFrame) -> bool:

    return ((metrics['recordsRead'] != 0) == (metrics['bytesRead'] != 0)).all()


def applicative_time_is_greater_than_or_equal_to_sum_durations_divided_by_number_executors(metrics: pd.DataFrame) -> bool:

    metrics = metrics.groupby(['experimentId'])

    time = metrics['transformTime'].first() + metrics['fitTime'].first()
    total = metrics['duration'].sum() / metrics['executorId'].nunique()

    return (time >= total).all()


def duration_is_greater_than_or_equal_to_the_sum_of_executor_cpu_deserialize_and_gc_time(metrics: pd.DataFrame) -> bool:

    times = ['executorCpuTime', 'executorDeserializeCpuTime', 'jvmGCTime']
    total = sum([metrics[time] for time in times])

    return (total <= metrics['duration']).all()


# TODO: shuffleTotalBytesRead, gettingResultTime ...

RULES = [
    # Dataset format
    train_count_is_greater_than_or_equal_to_test_count,
    jobs_stages_and_tasks_are_sequential,
    # identifier_columns_are_categoricals,

    # Execution
    tasks_are_successful,
    all_nodes_are_used,

    # Experimental setup
    partial(minimum_f1_score_is_respected, score=0.6),
    partial(minimum_r2_score_is_respected, score=0.6),
    partial(minimum_number_of_algorithms_per_family_is_respected, number=3),
    partial(minimum_number_of_algorithms_per_dataset_is_respected, number=9),
    partial(minimum_number_of_datasets_per_algorithm_is_respected, number=5),
    partial(number_of_nodes_is_respected, number=5),
    partial(maximum_number_of_executors_per_node_is_respected, number=1),

    # Spark domain
    records_written_is_equivalent_to_bytes_written,
    records_read_is_equivalent_to_bytes_read,

    applicative_time_is_greater_than_or_equal_to_sum_durations_divided_by_number_executors,
    duration_is_greater_than_or_equal_to_the_sum_of_executor_cpu_deserialize_and_gc_time,

    tasks_writing_for_shuffle_are_process_local,
    tasks_fetching_shuffle_blocks_are_not_process_local,

    shuffle_bytes_written_is_equivalent_to_shuffle_records_written,
    shuffle_bytes_written_is_equivalent_to_shuffle_write_time,

    executor_run_time_is_greater_than_or_equal_to_cpu_time,
    executor_deserialize_run_time_is_greater_than_or_equal_to_cpu_time,
]
