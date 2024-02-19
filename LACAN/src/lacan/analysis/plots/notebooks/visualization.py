import math

from typing import Iterable, Any, Dict, Optional, Callable, Union
from itertools import product
from tqdm import tqdm

from IPython.display import SVG
from graphviz import Source

from sklearn.tree import export_graphviz, DecisionTreeClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.metrics.cluster import silhouette_score
from sklearn.model_selection import cross_val_score
from sklearn.pipeline import make_pipeline

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns


def svg_tree(tree: DecisionTreeClassifier, rounded: bool = True, filled: bool = True, **kwargs) -> SVG:

    graph = Source(export_graphviz(tree, out_file=None, rounded=rounded, filled=filled, **kwargs))

    return SVG(graph.pipe('svg'))


def stick(data: pd.DataFrame, categories: Iterable[str]) -> pd.DataFrame:

    series = [data[category] for category in categories]

    def formatter(array: Iterable[Any]) -> str:
        return '-'.join(map(str, array))

    category = map(formatter, product(*map(lambda serie: serie.cat.categories, series)))
    values = map(formatter, zip(*series))

    data[formatter(categories)] = pd.Categorical(values, categories=category)

    return data


def plot_distributions(data: pd.DataFrame, cumulative: bool = False, size: int = 6, dropna: bool = False, gridsize: int = 100, **kwargs) -> None:

    number = math.sqrt(len(data.columns))
    row = math.ceil(number)
    col = math.ceil(number)

    plt.figure(figsize=(col * size, row * size))

    for i, feature in enumerate(tqdm(data.columns, leave=False)):

        plt.subplot(row, col, i + 1)  # TODO

        values = data[feature].dropna() if dropna else data[feature]

        sns.kdeplot(values, cumulative=cumulative, gridsize=gridsize, **kwargs)


def multidistplot(
        data: pd.DataFrame,
        feature: str,
        hue: str,
        legend_out: bool = False,
        height: float = 3,
        aspect: float = 1,
        hist: bool = True,
        legend: bool = True,
        kde_kws: Optional[Dict[str, Any]] = None
) -> None:

    grid = sns.FacetGrid(data, hue=hue, legend_out=legend_out, height=height, aspect=aspect)
    grid.map(sns.distplot, feature, hist=hist, kde_kws=kde_kws)

    if legend:
        grid.add_legend()


def plot_correlations(data: pd.DataFrame, method: str = 'pearson', cmap: str = 'RdBu_r', **kwargs) -> None:

    correlations = data.corr(method=method)

    mask = np.zeros_like(correlations, dtype=np.bool)
    mask[np.triu_indices_from(mask)] = True

    sns.heatmap(correlations, mask=mask, cmap=cmap, vmax=1, center=0, vmin=-1, square=True, **kwargs)


def select(data: pd.DataFrame, selection: Dict[str, Any]) -> pd.DataFrame:

    return data.loc[data[selection.keys()].isin(selection.values()).all(axis=1)]


def ml_workload(
        data: pd.DataFrame,
        labels: Optional[str],
        estimator: Any,
        scoring: Union[str, Callable[[Any, Any, Any], float], Callable[[Any, Any], float]],
        cv: int = 5,
        scaler: Any = StandardScaler()
) -> Iterable[float]:

    X = pd.get_dummies(data.drop(columns=labels if labels is not None else [])).astype(float)
    y = data[labels] if labels is not None else None

    steps = [scaler, estimator] if scaler is not None else [estimator]
    pipeline = make_pipeline(*steps)

    if scoring == 'silhouette_euclidean':

        if labels is not None:
            raise ValueError('labels are not needed for silhouette_euclidean')

        def scorer(clf: Any, features: Any) -> float:
            return silhouette_score(features, clf.fit_predict(features), metric='euclidean')

        scoring = scorer

    return cross_val_score(estimator=pipeline, X=X, y=y, cv=cv, scoring=scoring, n_jobs=-1)
