# hpc_connect

`hpc_connect` is a Python package that provides abstract interfaces to High-Performance Computing (HPC) schedulers and launchers. A default shell scheduler and MPI launcher is provided. Users can extend the functionality by subclassing the provided interfaces.

## Features

- **Abstract Interfaces**: Provides base classes for creating custom HPC schedulers and launchers.
- **Default Implementations**: Includes a default shell scheduler and an MPI launcher for immediate use.
- **Extensibility**: Easily create and register custom launchers and schedulers.

## Installation

You can install `hpc_connect` using pip:

```bash
python3 -m pip install "hpc-connect git+ssh://git@cee-gitlab.sandia.gov/ascic-test-infra/playground/hpc-connect"
```

## Usage

### Scheduler

```python
import hpc_connect
hpc_connect.set(scheduler="shell")
scheduler = hpc_connect.scheduler
with open("submit.sh", "w") as fh:
    scheduler.write_submission_script(["echo 'Hello, world!'"], fh)
schduler.submit_and_wait("submit.sh")
```

### Launcher

```console
hpc-launch --backend=mpi -n 4 echo 'Hello, world!'
```

## User defined scheduler

```python
from hpc_connect import HPCScheduler

class MyScheduler(HPCScheduler):

    name = "my-scheduler"

    @staticmethod
    def matches(name: Optional[str]) -> bool:
        # logic to determine if this scheduler matches ``name``

    def write_submission_script(
        self,
        script: list[str],
        file: TextIO,
        *,
        tasks: int,
        nodes: Optional[int] = None,
        job_name: Optional[str] = None,
        output: Optional[str] = None,
        error: Optional[str] = None,
        qtime: Optional[float] = None,
        variables: Optional[dict[str, Optional[str]]] = None,
    ) -> None:
        # write the submission script

    def submit_and_wait(
        self,
        script: str,
        *,
        job_name: Optional[str] = None,
        output: Optional[str] = None,
        error: Optional[str] = None,
    ) -> None:
        # submit script ``script`` and wait for it to finish
```
## User defined launcher

```python
from hpc_connect import HPCLauncher

class MyLauncher(HPCLauncher):
    name = "my-launcher"

    @staticmethod
    def matches(name: Optional[str]) -> bool:
        # logic to determine if this launcher matches ``name``

    def launch(
        self,
        *args_in: str,
        **kwargs_in: Any,
    ) -> int:
        # launch using *args_in
```

## Registering user defined launchers and schedulers

Custom launchers and schedulers must be registered in your `pyproject.toml` file using the `hpc_connect.launcher` and `hpc_connect.scheduler` entry points. Here's an example configuration:

```toml
[project]
name = "my_project"
version = "0.1.0"

[project.entry_points."hpc_connect.scheduler"]
my_scheduler = "my_odule:MyScheduler"

[project.entry_points."hpc_connect.launcher"]
my_launcher = "my_module:MyLauncher"
```
