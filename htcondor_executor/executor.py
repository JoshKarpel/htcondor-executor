from typing import Optional, Union

import datetime
import uuid
import time
import queue
import shutil
import threading
import tempfile
from pathlib import Path
from concurrent.futures import Executor, Future, wait
from concurrent.futures._base import FINISHED
from multiprocessing.pool import ApplyResult

import htcondor
import classad

from . import htio, state

RUN_SCRIPT = Path(__file__).parent / "run" / "run.py"


class HTCondorExecutor(Executor):
    def __init__(self, dir=None, max_jobs=100, persist_data_after_close=False):
        self._parent_dir = dir
        self._dir = None

        self.max_jobs = max_jobs
        self._pool = (None,) * max_jobs

        self.persist_data_after_close = persist_data_after_close

        self._tracker = state.StateTracker(self)

        self.tasks = {}
        self.task_queue = queue.Queue()

        self._shutdown = False

        self._submit_worker = threading.Thread(
            target=_submit_worker, args=(self,), daemon=True
        )
        self._tracking_worker = threading.Thread(
            target=_tracking_worker, args=(self,), daemon=True
        )

    def submit(self, fn, *args, **kwargs):
        f = Future()
        t = Task(self, f, fn, args, kwargs)

        self.tasks[t.task_id] = t
        self.task_queue.put(t)

        return f

    def apply_async(
        self, fn, args=None, kwargs=None, callback=None, error_callback=None
    ):
        args = args or ()
        kwargs = kwargs or {}

        f = self.submit(fn, *args, **kwargs)
        r = ApplyResultAdapter(f)

        f.add_done_callback(
            lambda f: callback(f.result())
            if f._exception is None
            else error_callback(f.exception())
        )

        return r

    def shutdown(self, wait: bool = True) -> None:
        print("shutting down")

        # TODO: remove jobs, wait for them to finish!
        # when the tracker goes into shutdown, it must still wait for existing jobs
        # to finish or be removed before exiting

        self._shutdown = True

        if wait:
            self._submit_worker.join()
            self._tracking_worker.join()

        print("shutdown success")

    @property
    def _event_log_path(self):
        return self._dir / "events"

    @property
    def _run_script_path(self):
        return self._dir / "run.py"

    def __enter__(self):
        self.__dir = tempfile.TemporaryDirectory(
            prefix="htcondor-executor_", dir=self._parent_dir
        )
        self.__dir.__enter__()
        self._dir = Path(self.__dir.name)

        self._event_log_path.touch(exist_ok=True)

        shutil.copy2(RUN_SCRIPT, self._run_script_path)

        self._submit_worker.start()
        self._tracking_worker.start()

        return super().__enter__()

    def __exit__(self, *args):
        if not self.persist_data_after_close:
            self._dir.__exit__(*args)
        return super().__exit__(*args)


def _submit_worker(executor: HTCondorExecutor):
    while not executor._shutdown:
        _do_submit_work(executor)
    print("submit worker shutting down")


def _do_submit_work(executor):
    try:
        t = executor.task_queue.get(timeout=1)
    except queue.Empty:
        return

    t.write_files()

    # https://github.com/python/cpython/blob/master/Lib/concurrent/futures/_base.py#L478
    if not t.future.set_running_or_notify_cancel():
        return

    schedd = htcondor.Schedd()
    with schedd.transaction() as txn:
        cluster_id = t.submit_description().queue(txn, 1,)

    t.job_id = state.JobID(cluster_id, 0)


def _tracking_worker(executor: HTCondorExecutor):
    while not executor._shutdown:
        executor._tracker._read_events(timeout=1)

    print("tracking worker shutting down")


TRANSFER_DIR = "_htcondor_executor_transfer"


class Task:
    def __init__(self, executor, future, fn, args, kwargs):
        self.task_id = uuid.uuid4()
        self.job_id = None

        self.executor = executor
        self.future = future

        self.fn = fn
        self.args = args
        self.kwargs = kwargs

    def __repr__(self):
        return f"Task(task_id = {self.task_id}, job_id = {self.job_id})"

    @property
    def dir(self):
        return self.executor._dir / str(self.task_id)

    @property
    def output_path(self):
        return self.dir / "output"

    @property
    def stdout_path(self):
        return self.dir / "stdout"

    @property
    def stderr_path(self):
        return self.dir / "stderr"

    def write_files(self):
        self.dir.mkdir(parents=True, exist_ok=True)
        htio.save_object(
            (self.fn, self.args, self.kwargs), self.dir / "fn_args_kwargs",
        )

    def submit_description(self):
        return htcondor.Submit(
            {
                "universe": "vanilla",
                "executable": self.executor._run_script_path.as_posix(),
                "arguments": classad.quote(str(self.task_id)),
                "JobBatchName": classad.quote(str(self.task_id)),
                "log": self.executor._event_log_path.as_posix(),
                "submit_event_notes": classad.quote(str(self.task_id)),
                "stdout": "stdout",
                "stderr": "stderr",
                "should_transfer_files": "YES",
                "when_to_transfer_output": "ON_EXIT_OR_EVICT",
                "transfer_input_files": "fn_args_kwargs",
                "transfer_output_files": f"{TRANSFER_DIR}/",
                "on_exit_hold": "ExitCode =!= 0",
                "initialdir": self.dir.as_posix(),
                "+TaskID": classad.quote(str(self.task_id)),
                "+IsExecutorJob": "True",
            }
        )


class ApplyResultAdapter:
    def __init__(self, future):
        self.future = future

    def __repr__(self):
        return f"{self.__class__.__name__}(future={self.future})"

    def get(self, timeout=None):
        return self.future.result(timeout=timeout)

    def ready(self):
        return self.future.done()

    @property
    def successful(self):
        if not self.ready():
            raise ValueError(f"{repr(self)} is not ready yet!")
        return self.future._exception is FINISHED

    def wait(self, timeout=None):
        wait(self.future, timeout=timeout)
