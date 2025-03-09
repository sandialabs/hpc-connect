import logging
import os
import time
from concurrent.futures import CancelledError

import flux  # type: ignore
from flux import Flux  # type: ignore
from flux.job import FluxExecutor  # type: ignore
from flux.job import FluxExecutorFuture  # type: ignore
from flux.job import Jobspec  # type: ignore
from flux.job import JobspecV1  # type: ignore

from ..hookspec import hookimpl
from ..submit import HPCProcess
from ..submit import HPCScheduler
from ..submit import HPCSubmissionFailedError
from ..submit import Job
from ..util import set_executable

logger = logging.getLogger("hpc_connect")


class FluxProcess(HPCProcess):
    JOB_TIMEOUT_CODE = 66
    fh = Flux()

    def __init__(self, job: Job, future: FluxExecutorFuture) -> None:
        super().__init__(job=job)
        self.fut: FluxExecutorFuture = future
        self.jobid: int | None = None

        def set_returncode(fut: FluxExecutorFuture):
            try:
                info = flux.job.result(self.fh, fut.jobid())
                self.returncode = info.returncode
            except (CancelledError, Exception):
                self.returncode = 1

        def set_jobid(fut: FluxExecutorFuture):
            try:
                self.jobid = fut.jobid()
                logger.info(f"submitted job {self.jobid} for test {self.job.name}")
            except CancelledError:
                self.returncode = 1
            except Exception as e:
                raise HPCSubmissionFailedError(e)

        self.fut.add_jobid_callback(set_jobid)
        self.fut.add_done_callback(set_returncode)

    def poll(self) -> int | None:
        return self.returncode

    def cancel(self, returncode: int) -> None:
        self.returncode = returncode
        logger.info(f"Canceling job {self.jobid}.")
        try:
            flux.job.cancel(self.fh, self.jobid)
        except OSError:
            logger.debug(f"Job {self.jobid} is inactive, cannot cancel.")
            pass
        except Exception:
            logger.error(f"Failed to cancel job {self.jobid}.")


class FluxScheduler(HPCScheduler):
    """Setup and submit jobs to the Flux scheduler"""

    name = "flux"
    shell = "/bin/sh"
    flux: FluxExecutor | None = FluxExecutor()
    fluxHandle = Flux()

    def __init__(self) -> None:
        super().__init__()
        self.exclusive: bool = False

    @property
    def supports_subscheduling(self) -> bool:
        return True

    @staticmethod
    def matches(name: str | None) -> bool:
        return name is not None and name.lower() == "flux"

    @property
    def submission_template(self) -> str:
        if "HPCC_FLUX_SUBMIT_TEMPLATE" in os.environ:
            return os.environ["HPCC_FLUX_SUBMIT_TEMPLATE"]
        return "flux.sh.in"

    def submit(self, job: Job) -> HPCProcess:
        os.makedirs(os.path.dirname(job.script), exist_ok=True)
        with open(job.script, "w") as fh:
            self.write_submission_script(job, fh)
        set_executable(job.script)

        jobspec = self.create_jobspec(job)
        fut = self.flux.submit(jobspec)  # type: ignore

        return FluxProcess(job=job, future=fut)

    def create_jobspec(self, job: Job) -> Jobspec:
        jobspec = JobspecV1.from_command(
            command=[job.script],
            num_tasks=job.tasks,
            num_nodes=self.nodes_required(job.tasks),
            cores_per_task=job.cpus_per_task,
            gpus_per_task=job.gpus_per_task,
            exclusive=self.exclusive,
        )
        jobspec.setattr("system.job.name", job.name)
        jobspec.stdout = job.output
        jobspec.stderr = job.error or job.output
        jobspec.duration = job.time_limit_in_seconds(60)
        jobspec.environment = dict(os.environ)
        if job.variables:
            jobspec.environment.update(job.variables)

        return jobspec

    def shutdown(self, returncode: int = 0):
        super().shutdown(returncode)
        with self.lock:
            if self.flux:
                time.sleep(0.25)
                if flux.job:
                    jobList = flux.job.JobList(
                        self.fluxHandle, filters=["running", "pending"]
                    ).jobs()
                    nJobs = len(jobList)
                    if nJobs > 0:
                        logger.warning(f"{nJobs} active jobs remain after shutdown")
                self.flux.shutdown(wait=False, cancel_futures=True)
                self.flux = None

    def submit_and_wait(
        self,
        *jobs,
        sequential: bool = True,
        timeout: float | None = None,
        polling_frequency: float | None = None,
    ) -> None:
        self.exclusive = sequential
        super().submit_and_wait(
            *jobs,
            sequential=sequential,
            timeout=timeout,
            polling_frequency=polling_frequency,
        )


@hookimpl
def hpc_connect_scheduler():
    return FluxScheduler
