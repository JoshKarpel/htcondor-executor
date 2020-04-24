#!/usr/bin/env python3

# Copyright 2020 HTCondor Team, Computer Sciences Department,
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

import os
import shutil
import sys
import socket
import datetime
import gzip
import textwrap
import traceback
import subprocess
import getpass
from pathlib import Path

TRANSFER_DIR = "_htcondor_executor_transfer"


# import cloudpickle goes in the functions that need it directly
# so that errors are raised later


def get_node_info():
    try:
        user = getpass.getuser()
    except:
        user = None

    return (
        socket.getfqdn(),
        socket.gethostbyname(socket.gethostname()),
        datetime.datetime.utcnow(),
        user,
    )


def print_node_info(node_info):
    print("Landed on execute node {} ({}) at {} as {}".format(*node_info))


def get_python_info():
    return (
        sys.executable,
        f"{'.'.join(str(x) for x in sys.version_info[:3])} {sys.version_info[3]}",
        pip_freeze(),
    )


def print_python_info(python_info):
    executable, version, packages = python_info
    print("Python executable is {} (version {})".format(executable, version))
    print("with installed packages")
    print("\n".join("  {}".format(line) for line in packages.splitlines()))


def pip_freeze() -> str:
    return (
        subprocess.run(
            [sys.executable, "-m", "pip", "freeze", "--disable-pip-version-check"],
            stdout=subprocess.PIPE,
        )
        .stdout.decode("utf-8")
        .strip()
    )


def print_dir_contents(contents):
    print("Working directory contents:")
    for path in contents:
        print("  " + str(path))


def print_run_info(task_id, func, args, kwargs):
    s = "\n".join(
        (
            "Running task {}".format(task_id),
            "  {}".format(func),
            "with args",
            "  {}".format(args),
            "and kwargs",
            "  {}".format(kwargs),
        )
    )

    print(s)


def load_object(path):
    import cloudpickle

    with gzip.open(path, mode="rb") as file:
        return cloudpickle.load(file)


def load_fn_args_kwargs():
    return load_object(Path("fn_args_kwargs"))


def save_objects(objects, path):
    import cloudpickle

    with gzip.open(path, mode="wb") as file:
        for obj in objects:
            cloudpickle.dump(obj, file)


def save_output(status, result_or_error, transfer_dir):
    save_objects([status, result_or_error], transfer_dir / "output")


def clean_and_remake_dir(dir: Path):
    if dir.exists():
        shutil.rmtree(dir)
    dir.mkdir()


def main(task_id):
    node_info = get_node_info()
    print_node_info(node_info)
    print()

    scratch_dir = Path.cwd()
    transfer_dir = scratch_dir / TRANSFER_DIR
    transfer_dir.mkdir(exist_ok=True)

    contents = list(scratch_dir.iterdir())
    print_dir_contents(contents)
    print()

    try:
        python_info = get_python_info()
        print_python_info(python_info)
    except Exception as e:
        print(e)
    print()

    try:
        fn, args, kwargs = load_fn_args_kwargs()

        print_run_info(task_id, fn, args, kwargs)

        print("\n----- TASK OUTPUT START -----\n")

        result_or_error = fn(*args, **kwargs)
        status = "OK"

        print("\n-----  TASK OUTPUT END  -----\n")
    except Exception as e:
        print("\n-------  TASK ERROR  --------\n")

        result_or_error = e
        status = "ERR"

        traceback.print_exc(file=sys.stderr)

    clean_and_remake_dir(transfer_dir)
    save_output(status, result_or_error, transfer_dir)

    print(
        "Finished executing task {} at {}".format(task_id, datetime.datetime.utcnow())
    )


if __name__ == "__main__":
    main(task_id=sys.argv[1])
