# Copyright 2018 HTCondor Team, Computer Sciences Department,
# University of Wisconsin-Madison, WI.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Any, List, Tuple, Iterator, Dict, Callable
import logging

import gzip
import json
from pathlib import Path

import cloudpickle
import htcondor


logger = logging.getLogger(__name__)


def save_object(obj: Any, path: Path) -> None:
    """Serialize a Python object (including "objects", like functions) to a file at the given ``path``."""
    with gzip.open(path, mode="wb") as file:
        cloudpickle.dump(obj, file)


def load_object(path: Path) -> Any:
    """Deserialize an object from the file at the given ``path``."""
    with gzip.open(path, mode="rb") as file:
        return cloudpickle.load(file)


def load_objects(path: Path) -> Iterator[Any]:
    """Deserialize a stream of objects from the file at the given ``path``."""
    with gzip.open(path, mode="rb") as file:
        while True:
            yield cloudpickle.load(file)
