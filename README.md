# hpc_connect

`hpc_connect` is a Python package that provides abstract interfaces to High-Performance Computing (HPC) schedulers and launchers. A default shell scheduler and MPI launcher is provided. Users can extend the functionality by subclassing the provided interfaces.

## Features

- **Abstract Interfaces**: Provides base classes for creating custom HPC schedulers and launchers.
- **Default Implementations**: Includes a default shell scheduler and an MPI launcher for immediate use.
- **Extensibility**: Easily create and register custom launchers and schedulers.

## Installation

You can install `hpc_connect` using pip:

```bash
python3 -m pip install "hpc-connect git+ssh://git@cee-gitlab.sandia.gov/ascic-test-infra/hpc-connect"
```

## Usage

### Scheduler

```python
import hpc_connect
backend = hpc_connect.get_backend("shell")
backend.submit("hello-world", ["echo 'Hello, world!'"], cpus=1)
```

### Launcher

```console
$ hpc-launch --backend=mpi -n 4 echo 'Hello, world!'
Hello, world!
Hello, world!
Hello, world!
Hello, world!
```

## User defined scheduler backend

```python
from hpc_connect import HPCBackend

class MyBackend(HPCBackend):

    name = "my-backend"

    @staticmethod
    def matches(name: str | None) -> bool:
        # logic to determine if this backend matches ``name``

    def submit(
        self,
        name: str,
        args: list[str],
        scriptname: str | None = None,
        qtime: float | None = None,
        submit_flags: list[str] | None = None,
        variables: dict[str, str | None] | None = None,
        output: str | None = None,
        error: str | None = None,
        nodes: int | None = None,
        cpus: int | None = None,
        gpus: int | None = None,
        **kwargs: Any,
    ) -> HPCProcess: ...
        # submit script ``script`` and return the HPCProcess


@hpc_connect.hookimpl
def hpc_connect_backend():
    return MyBackend
```

## User defined launcher

```python
from hpc_connect import HPCLauncher

class MyLauncher(HPCLauncher):
    name = "my-launcher"

    def __init__(self, config_file: str | None = None) -> None:
        # setup

    @staticmethod
    def matches(name: str | None) -> bool:
        # logic to determine if this launcher matches ``name``

    @property
    def executable(self) -> str:
        # return the string to the launcher's executable

@hpc_connect.hookimpl
def hpc_connect_launcher():
    return MyLauncher
```

## Registering user defined launchers and schedulers

Custom launchers and schedulers must be registered in your `pyproject.toml` file using the `hpc_connect` entry points. Here's an example configuration:

```toml
[project]
name = "my_project"
version = "0.1.0"

[project.entry_points.hpc_connect]
my_hpc_connect = "my_module"
```
