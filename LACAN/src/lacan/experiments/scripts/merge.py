#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re

from argparse import ArgumentParser
from itertools import filterfalse
from typing import Generator, Dict, Any, Iterable, Optional, NamedTuple

import pandas as pd

from utils import read_json, read_file, read_hadoop_csv


Summary = Dict[str, Any]


def read_summary(path: str) -> Summary:

    path = os.path.join(path, 'summary.json')

    return read_json(path)


Error = str


def read_error_if_exist(path: str) -> Optional[Error]:

    path = os.path.join(path, 'error.txt')

    if not os.path.isfile(path):
        return None

    return read_file(path)


class Experiment(NamedTuple):
    platform: pd.DataFrame
    applicative: pd.DataFrame
    summary: Summary
    success: bool
    low: pd.DataFrame


def read_experiment(path: str) -> Experiment:

    summary = read_summary(path)
    error = read_error_if_exist(path)

    if error is not None:
        success = False
    else:
        success = True

    platform = None
    applicative = None
    low = []

    if success:
        platform_fit = read_hadoop_csv(os.path.join(path, 'platform-fit-metrics.csv'))
        platform_transform = pd.DataFrame() #read_hadoop_csv(os.path.join(path, 'platform-transform-metrics.csv'))
        applicative = read_hadoop_csv(os.path.join(path, 'applicative-metrics.csv'))

        platform_fit['phase'] = 'fit'
        platform_transform['phase'] = 'transform'

        platform = pd.concat([platform_fit, platform_transform], sort=True)

        platform = platform.rename(columns={'index': 'taskId'})

        #system = os.path.join(path, 'system-metrics')
        #for node in sorted(os.listdir(system)):
        #    df = pd.read_csv(os.path.join(system, node, 'low_level_metrics.csv'))
        #    df['node'] = node
        #    low.append(df)

    return Experiment(
        platform=platform,
        applicative=applicative,
        summary=summary,
        success=success,
        low=None#pd.concat(low) if len(low) else pd.DataFrame()
    )


def walk_experiments(root: str):

    for path, _, files in os.walk(root):

        if 'summary.json' in files:

            yield path


def read_experiments(root: str) -> Generator[Experiment, None, None]:

    for path in walk_experiments(root):

        yield read_experiment(path)


EXPERIMENT_ID = 0


def collect_experiment(experiment: Experiment, name: str) -> Any:

    global EXPERIMENT_ID

    localities = {
        0: 'PROCESS_LOCAL',
        1: 'NODE_LOCAL',
        2: 'RACK_LOCAL',
        3: 'NO_PREF',
        4: 'ANY'
    }

    applicative = experiment.applicative
    metric = experiment.platform
    low = experiment.low
    summary = pd.DataFrame([experiment.summary]).drop(columns=['workflowId', 'experimentId', 'scenarioId'])

    platform = summary[['platformId', 'platform']]
    summary = summary.drop(columns=['platform'])

    platform = platform.groupby('platformId')['platform'].first().reset_index()
    platform = pd.concat([platform.drop(['platform'], axis=1), platform['platform'].apply(pd.Series)], axis=1)

    metric['taskLocality'] = metric['taskLocality'].map(localities)

    applicative = applicative.set_index('metric').T

    applicative['experimentId'] = EXPERIMENT_ID
    metric['experimentId'] = EXPERIMENT_ID
    summary['experimentId'] = EXPERIMENT_ID
    #low['experimentId'] = EXPERIMENT_ID

    EXPERIMENT_ID += 1

    applicative = applicative.set_index(['experimentId'])
    metric = metric.set_index(['experimentId'])
    summary = summary.set_index(['experimentId'])
    #low = low.set_index(['experimentId'])

    metr = metric.join(summary, how='inner')
    appl = applicative.join(summary, how='inner')
    plat = platform.drop_duplicates()
    #lows = low.join(summary, how='inner')

    if name == "metrics":
        return metr
    if name == "applicative":
        return appl
    if name == "configuration":
        return plat
    assert False


from tqdm import tqdm
from functools import partial

def merge_experiments(experiments: Iterable[Experiment], name: str) -> pd.DataFrame:
    
    merger = partial(collect_experiment, name=name)

    res = pd.concat(tqdm(map(merger, experiments)), sort=False)
    
    if name == "configuration":
        return res.drop_duplicates()
    
    return res


def save_metrics(path: str, metrics: pd.DataFrame, name: str) -> None:

    path = os.path.join(path, name + '.csv')

    metrics.to_csv(path, index=False)


def main(path: str, save: str, name: str) -> None:

    if name not in ["applicative", "configuration", "metrics"]:
        raise ValueError("Invalid name: " + name)

    experiments = read_experiments(path)

    success = filter(lambda experiment: experiment.success, experiments)
    failure = filterfalse(lambda experiment: experiment.success, experiments)

    if success:
        metrics = merge_experiments(success, name)
        print('save')
        save_metrics(save, metrics, name)


def parse() -> Dict[str, str]:

    parser = ArgumentParser()

    parser.add_argument(
        '--path', type=str, required=True,
        help="<Required> Directory containing the results of the experiments to merge."
    )

    parser.add_argument(
        '--save', type=str, required=True,
        help="<Required> Directory where to save the results of the merge."
    )

    parser.add_argument(
        '--name', type=str, required=True,
        help="<Required> Which file to create (applicative, configuration, metrics)."
    )

    return vars(parser.parse_args())


if __name__ == '__main__':

    main(**parse())
