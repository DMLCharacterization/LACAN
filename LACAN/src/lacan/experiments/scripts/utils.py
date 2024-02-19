import os
import json

from typing import List, Dict, Any, Union, Callable, TypeVar, Iterable

import pandas as pd


def get_hadoop_file_parts(path: str) -> Iterable[str]:

    file_names = os.listdir(path)

    parts = filter(lambda file_name: file_name.startswith('part-'), file_names)

    return parts


def is_hadoop_file(path: str) -> bool:

    if not os.path.exists(path):
        raise ValueError("File doesn't exist")

    if not os.path.isdir(path):
        return False

    if not os.path.isfile(os.path.join(path, '_SUCCESS')):
        return False

    return True


def read_hadoop_csv(path: str) -> pd.DataFrame:

    if not is_hadoop_file(path):
        raise ValueError("File must be an Hadoop file")

    parts = list(get_hadoop_file_parts(path))

    if len(parts) > 1:
        raise ValueError("File must not have multiple parts")

    if not parts:
        raise ValueError("No parts to read from")

    part = parts[0]

    file = os.path.join(path, part)

    return pd.read_csv(file)


def read_file(path: str) -> str:

    with open(path, 'r') as file:

        return file.read()


def read_json(path: str) -> Any:

    with open(path, 'r') as file:

        return json.load(file)


def replace_all(dictionary: Dict[str, Any], obj: Any) -> Any:

    for old, new in dictionary.items():

        obj = replace(old, new, obj)

    return obj


def replace(old: Any, new: Any, obj: Any) -> Any:

    if obj == old:
        return new

    if isinstance(obj, list):
        return replace_seq(old, new, obj)

    if isinstance(obj, dict):
        return replace_dictionary(old, new, obj)

    if isinstance(obj, str):
        return replace_string(old, new, obj)

    return obj


def replace_string(old: Any, new: Any, string: str) -> Union[str, Any]:

    if string == old:
        return new

    return string.replace(old, str(new))


def replace_dictionary(old: Any, new: Any, dictionary: Dict[Any, Any]) -> Dict[Any, Any]:

    dictionary = dictionary.copy()

    for key, value in dictionary.items():

        if value == old:
            dictionary[key] = new
        else:
            dictionary[key] = replace(old, new, value)

    return dictionary


def replace_seq(old: Any, new: Any, seq: List[Any]) -> List[Any]:

    seq = seq.copy()

    for i, value in enumerate(seq):

        if value == old:
            seq[i] = new
        else:
            seq[i] = replace(old, new, value)

    return seq


def transform_file(source: str, target: str, func: Callable[[str], str]) -> None:

    with open(source, 'r') as old, open(target, 'w+') as new:

        for line in old:

            new.write(func(line))


def transform_files(source: str, target: str, func: Callable[[str], str]) -> None:

    filenames = os.listdir(source)

    for filename in filenames:

        transform_file(os.path.join(source, filename), os.path.join(target, filename), func)


def write_file(path: str, string: str) -> None:

    with open(path, 'w') as file:

        file.write(string)


def write_json(path: str, obj: Any) -> None:

    with open(path, 'w') as file:

        json.dump(obj, file)


TYPE = TypeVar('TYPE')


def first(iterable: Iterable[TYPE]) -> TYPE:

    return next(iter(iterable))
