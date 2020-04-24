# Copyright 2019 HTCondor Team, Computer Sciences Department,
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

from typing import Dict, Tuple, Mapping
import logging

import uuid
import collections

import htcondor
import classad

from . import htio, utils

logger = logging.getLogger(__name__)


class JobStatus(utils.StrEnum):
    """
    An enumeration of the possible statuses that a map component can be in.
    These are mostly identical to the HTCondor job statuses of the same name.
    """

    UNKNOWN = "UNKNOWN"
    UNMATERIALIZED = "UNMATERIALIZED"
    IDLE = "IDLE"
    RUNNING = "RUNNING"
    REMOVED = "REMOVED"
    COMPLETED = "COMPLETED"
    HELD = "HELD"
    SUSPENDED = "SUSPENDED"
    ERRORED = "ERRORED"

    @classmethod
    def display_statuses(cls) -> Tuple["JobStatus", ...]:
        return (
            cls.HELD,
            cls.ERRORED,
            cls.IDLE,
            cls.RUNNING,
            cls.COMPLETED,
        )


JOB_EVENT_STATUS_TRANSITIONS = {
    htcondor.JobEventType.SUBMIT: JobStatus.IDLE,
    htcondor.JobEventType.JOB_EVICTED: JobStatus.IDLE,
    htcondor.JobEventType.JOB_UNSUSPENDED: JobStatus.IDLE,
    htcondor.JobEventType.JOB_RELEASED: JobStatus.IDLE,
    htcondor.JobEventType.SHADOW_EXCEPTION: JobStatus.IDLE,
    htcondor.JobEventType.JOB_RECONNECT_FAILED: JobStatus.IDLE,
    htcondor.JobEventType.JOB_TERMINATED: JobStatus.COMPLETED,
    htcondor.JobEventType.EXECUTE: JobStatus.RUNNING,
    htcondor.JobEventType.JOB_HELD: JobStatus.HELD,
    htcondor.JobEventType.JOB_SUSPENDED: JobStatus.SUSPENDED,
    htcondor.JobEventType.JOB_ABORTED: JobStatus.REMOVED,
}


class JobID:
    def __init__(self, cluster, proc):
        self.cluster = cluster
        self.proc = proc

    def __repr__(self):
        return f"JobID(cluster = {self.cluster}, proc = {self.proc})"

    def __str__(self):
        return f"{self.cluster}.{self.proc}"

    def __eq__(self, other):
        return (self.cluster, self.proc) == (other.cluster, other.proc)

    def __hash__(self):
        return hash((self.__class__, self.cluster, self.proc))


class JobStateTracker:
    def __init__(self, executor):
        self.executor = executor

        self._event_reader = None  # delayed until _read_events is called

        self._jobid_to_taskid: Dict[JobID, executor.TaskID] = {}

        self._task_statuses = collections.defaultdict(lambda: JobStatus.UNMATERIALIZED)

    @property
    def task_statuses(self) -> Mapping[JobID, JobStatus]:
        self._read_events()
        return dict(self._task_statuses)

    @property
    def _event_log_path(self):
        return self.executor._event_log_path

    def _read_events(self, timeout=0):
        if self._event_reader is None:
            self._event_reader = htcondor.JobEventLog(
                self._event_log_path.as_posix()
            ).events(timeout)

        for event in self._event_reader.events(timeout):
            # skip the late materialization submit event
            if event.proc == -1:
                continue

            job_id = JobID(event.cluster, event.proc)

            if event.type is htcondor.JobEventType.SUBMIT:
                self._jobid_to_taskid[job_id] = uuid.UUID(
                    classad.unquote(event["LogNotes"])
                )

            # this lookup is safe because the SUBMIT event always comes first
            task_id = self._jobid_to_taskid[job_id]
            task = self.executor.tasks[task_id]

            if event.type is htcondor.JobEventType.JOB_HELD:
                raise Exception("job held")

            new_status = JOB_EVENT_STATUS_TRANSITIONS.get(event.type, None)

            if new_status is not None:
                if new_status is self._task_statuses[task_id]:
                    logger.warning(
                        f"{task} of executor {self.executor} tried to transition into the state it is already in ({new_status})"
                    )
                else:
                    self._task_statuses[task_id] = new_status

                    if new_status is JobStatus.COMPLETED:
                        x = htio.load_objects(task.output_path)
                        status = next(x)
                        output = next(x)
                        print(f"{task} finished with {status}, {output}")

                        if status == "OK":
                            task.future.set_result(output)
                        elif status == "ERR":
                            task.future.set_exception(output)
                        else:
                            raise Exception(f"bad task status {status}")
