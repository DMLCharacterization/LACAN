#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import json
import logging
import subprocess

from subprocess import CompletedProcess
from collections import namedtuple
from typing import Generator, Dict, Any, NamedTuple, Iterable, Optional, Union, Sequence
from enum import Enum
from datetime import datetime
from argparse import ArgumentParser
from itertools import product
from contextlib import contextmanager
from tqdm import tqdm

from utils import read_json, replace_all, write_file, write_json, first

Launcher = namedtuple('Launcher', ['timeout', 'runs', 'location', 'jar', 'mainclass', 'results'])
#class Launcher(NamedTuple):
#imeout: Optional[int]
#uns: int
#ocation: str
#ar: str
#ainclass: str
#esults: str

Dynamic = namedtuple('Dynamic', ['default', 'values'])
#class Dynamic(NamedTuple):
#efault: Any
#alues: Iterable[Any]


#Platforms = Dict[str, Dynamic]

Collector = namedtuple('Collector', ['start', 'stop', 'workers', 'script'])
#class Collector(NamedTuple):
#tart: str
#top: str
#orkers: str
#cript: str

Header = namedtuple('Header', ['launcher', 'platforms', 'execution', 'metrics', 'collector'])
#class Header(NamedTuple):
#auncher: Launcher
#latforms: Platforms
#xecution: Any
#etrics: Any
#ollector: Optional[Collector]


def read_header(path )  :

    config = read_json(path)

    launcher = Launcher(
        timeout=config['launcher'].get('timeout'),
        runs=config['launcher']['runs'],
        location=config['launcher']['location'],
        jar=config['launcher']['jar'],
        mainclass=config['launcher']['mainclass'],
        results=config['launcher']['results']
    )

    platforms = {
        key: Dynamic(default=val.get('default'), values=val.get('values', []))
        for key, val in config['platforms'].items()
    }

    collector = None
    if 'collector' in config:
        collector = Collector(
            start=config['collector']['start'],
            stop=config['collector']['stop'],
            workers=config['collector']['workers'],
            script=config['collector']['script']
        )

    return Header(
        launcher=launcher,
        platforms=platforms,
        execution=config['execution'],
        metrics=config['metrics'],
        collector=collector
    )


class Family(Enum):
    CLASSIFICATION = "classification"
    REGRESSION = "regression"
    CLUSTERING = "clustering"

Component = namedtuple('Component', ['name', 'types', 'description', 'details'])
#class Component(NamedTuple):
#ame: str
#ypes: Iterable[Family]
#escription: Dict[str, Any]
#etails: Any


def read_components(path )    :

    components = sorted(os.listdir(path))

    for component in components:

        config = read_json(os.path.join(path, component))

        name = config['name']
        types = sorted(config['types'])
        description = config.get('description')
        details = config['details']

        yield Component(
            name=name,
            types=types,
            description=description,
            details=details
        )

Configuration = namedtuple('Configuration', ['launcher', 'platforms', 'execution', 'metrics', 'datasets', 'splitters', 'algorithms', 'evaluators', 'collector'])
#class Configuration(NamedTuple):
#auncher: Launcher
#latforms: Platforms
#xecution: Any
#etrics: Any
#atasets: Iterable[Component]
#plitters: Iterable[Component]
#lgorithms: Iterable[Component]
#valuators: Iterable[Component]
#ollector: Optional[Collector]


def read_configuration(path )  :

    header = read_header(os.path.join(path, 'header.json'))
    datasets = list(read_components(os.path.join(path, 'datasets')))
    splitters = list(read_components(os.path.join(path, 'splitters')))
    algorithms = list(read_components(os.path.join(path, 'algorithms')))
    evaluators = list(read_components(os.path.join(path, 'evaluators')))

    return Configuration(
        datasets=datasets,
        splitters=splitters,
        algorithms=algorithms,
        evaluators=evaluators,
        launcher=header.launcher,
        platforms=header.platforms,
        execution=header.execution,
        metrics=header.metrics,
        collector=header.collector
    )

Workflow = namedtuple('Workflow', ['workflow_id', 'dataset', 'splitter', 'algorithm', 'evaluator', 'family'])
#class Workflow(NamedTuple):
#orkflow_id: int
#ataset: Component
#plitter: Component
#lgorithm: Component
#valuator: Component
#amily: Family


def generate_workflows(config )    :

    workflows = product(config.datasets, config.algorithms, config.splitters, config.evaluators)

    for workflow_id, (dataset, algorithm, splitter, evaluator) in enumerate(workflows):

        family = algorithm.types[0]

        if family in dataset.types and family in splitter.types and family in evaluator.types:

            yield Workflow(
                workflow_id=workflow_id,
                dataset=dataset,
                splitter=splitter,
                algorithm=algorithm,
                evaluator=evaluator,
                family=family
            )

Platform = namedtuple('Platform', ['platform_id', 'platform'])
#class Platform(NamedTuple):
#latform_id: int
#latform: Any


def generate_platforms(platforms )    :

    platform_id = 0

    defaults = {
        key: dynamic.default
        for key, dynamic in platforms.items()
        if dynamic.default is not None
    }

    yield Platform(platform_id=platform_id, platform=defaults)

    for key in sorted(platforms.keys()):

        for value in platforms[key].values:

            new = defaults.copy()
            new[key] = value

            platform_id += 1

            yield Platform(platform_id=platform_id, platform=new)

Scenario = namedtuple('Scenario', ['scenario_id', 'workflow', 'platform'])
#class Scenario(NamedTuple):
#cenario_id: int
#orkflow: Workflow
#latform: Platform


def generate_scenarios(config )    :

    scenarios = product(generate_workflows(config), generate_platforms(config.platforms))

    for scenario_id, (workflow, platform) in enumerate(scenarios):

        yield Scenario(scenario_id=scenario_id, workflow=workflow, platform=platform)

Experiment = namedtuple('Experiment', ['experiment_id', 'run_id', 'scenario'])
#class Experiment(NamedTuple):
#xperiment_id: int
#un_id: int
#cenario: Scenario


def generate_experiments(config )    :

    experiments = product(generate_scenarios(config), range(0, config.launcher.runs))

    for experiment_id, (scenario, run_id) in enumerate(experiments):

        yield Experiment(experiment_id=experiment_id, run_id=run_id, scenario=scenario)


Setup = Any  # pylint: disable=C0103


def make_setup(config , experiment )  :

    workflow = experiment.scenario.workflow
    platform = experiment.scenario.platform

    return {
        'platform': platform.platform,
        'execution': config.execution,
        'metrics':   config.metrics,
        'workflow': {
            **workflow.dataset.details,
            **workflow.splitter.details,
            **workflow.algorithm.details,
            **workflow.evaluator.details
        }
    }


def make_variables(experiment )   :

    dataset = experiment.scenario.workflow.dataset

    return {
        '${{dataset.{key}}}'.format(key=key): value for key, value in dataset.description.items()
    }


Summary = Dict[str, Any]


def make_summary(experiment )  :

    workflow = experiment.scenario.workflow
    platform = experiment.scenario.platform

    return {
        'experimentId': experiment.experiment_id,
        'runId':        experiment.run_id,
        'scenarioId':   experiment.scenario.scenario_id,
        'workflowId':   workflow.workflow_id,
        'dataset':      workflow.dataset.name,
        'algorithm':    workflow.algorithm.name,
        'splitter':     workflow.splitter.name,
        'family':       workflow.family,
        'platformId':   platform.platform_id,
        'platform':     platform.platform
    }


def make_location(experiment )  :

    workflow = experiment.scenario.workflow
    platform = experiment.scenario.platform

    return os.path.join(
        workflow.dataset.name,
        workflow.family,
        workflow.algorithm.name,
        'platform-{key}'.format(key=platform.platform_id),
        'run-{key}'.format(key=experiment.run_id)
    )


def make_root(config )  :

    return os.path.join(config.launcher.location, datetime.now().isoformat())


def save_summary(path , summary )  :

    path = os.path.join(path, 'summary.json')

    write_json(path, summary)


Report = CompletedProcess


def save_report(path , report )  :

    path = os.path.join(path, 'report.txt')

    write_file(path, str(report))


def save_error(path , error )  :

    path = os.path.join(path, 'error.txt')

    write_file(path, repr(error))

class ScriptError(Exception):
    pass


def run_script(args  , timeout  = None, shell  = False, out=None)  :

    FNULL = open(os.devnull, 'w')

    process = subprocess.run(args, timeout=timeout, check=True, stdout=FNULL, stderr=FNULL, shell=shell)

    if process.returncode != 0:
        raise ScriptError(str(process))

    return process



def submit_experiment(config , setup )  :

    return run_script([
        'spark-submit',
        '--driver-memory', '25g',
        '--driver-cores', '2',
        '--num-executors', '14',
        '--class', config.launcher.mainclass,
        config.launcher.jar,
        '--config', json.dumps(setup)
    ], timeout=config.launcher.timeout)


def retrieve_results(config , workdir )  :

    run_script([
        '/opt/hadoop-2.7.7/bin/hdfs', 'dfs', '-copyToLocal',
        os.path.join(config.launcher.results, 'applicative-metrics.csv'),
        workdir
    ])

    run_script([
        '/opt/hadoop-2.7.7/bin/hdfs', 'dfs', '-copyToLocal',
        os.path.join(config.launcher.results, 'platform-fit-metrics.csv'),
        workdir
    ])

    run_script([
        '/opt/hadoop-2.7.7/bin/hdfs', 'dfs', '-copyToLocal',
        os.path.join(config.launcher.results, 'platform-transform-metrics.csv'),
        workdir
    ])


def start_collect(collector )  :

    run_script([collector.start, collector.script, collector.workers])


def stop_collect(collector , path )  :

    run_script([collector.stop, collector.script, collector.workers, path])


@contextmanager
def collect(collector , path ):

    if collector is not None:
        start_collect(collector)

    yield None

    if collector is not None:
        stop_collect(collector, path)


def main(path )  :

    config = read_configuration(path)

    experiments = list(generate_experiments(config))

    root = make_root(config)

    for experiment in tqdm(experiments, dynamic_ncols=True):

        setup = make_setup(config, experiment)

        workdir = os.path.join(root, make_location(experiment))

        if not os.path.isdir(workdir):
            os.makedirs(workdir)

        variables = make_variables(experiment)
        variables['${workdir}'] = workdir

        setup = replace_all(variables, setup)

        summary = make_summary(experiment)
        save_summary(workdir, summary)

        try:

            start_collect(config.collector)

            report = submit_experiment(config, setup)
            retrieve_results(config, workdir)

            stop_collect(config.collector , workdir )

            #save_report(workdir, report)

        except Exception as err:

            logging.warning("Job at %s failed - Received %s", workdir, err.__class__.__name__)

            save_error(workdir, err)

            try:
                stop_collect(config.collector , workdir )
            except:
                pass


def parse()   :

    parser = ArgumentParser()

    parser.add_argument(
        '--path', type=str, required=True,
        help="<Required> Directory containing the configurations of the experiments to launch."
    )

    return vars(parser.parse_args())


if __name__ == '__main__':

    main(**parse())
