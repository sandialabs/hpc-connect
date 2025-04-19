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

## Registering user defined backend

Custom backends must be registered in your `pyproject.toml` file using the `hpc_connect` entry points. Here's an example configuration:

```toml
[project]
name = "my_project"
version = "0.1.0"

[project.entry_points.hpc_connect]
my_hpc_connect = "my_module"
```


## hpc-launch

A command-line interface for launching parallel applications using various HPC launchers.

### SYNOPSIS

```console
hpc-launch [mpi-options] <application> [application_options]
```

### Description

`hpc-launch` is a CLI tool that forwards arguments to configured backend launchers such as `mpiexec`, `mpirun`, and `jsrun`. It provides a unified command structure for launching parallel applications, allowing users to execute their applications without needing to remember the specific syntax for each launcher.

### Configuration

The behavior of `hpc-launch` is determined by a configuration file in YAML format. The default configuration file contains the following structure:

```yaml
hpc_connect:
  launch:
    vendor: openmpi
    exec: mpiexec
    numproc_flag: -n
    default_options: []
    default_local_options: []
    mappings: {}
```

#### Configuration Parameters

- vendor: The MPI implementation vendor (e.g., openmpi, mpich).
- exec: The command to execute the launcher (e.g., mpiexec, mpirun).
- numproc_flag: The flag used to specify the number of processes (e.g., -n).
- default_options: A list of default options passed to the launcher.
- default_local_options: A list of options specific to local execution.
- mappings: A dictionary for additional mappings or configurations, where command-line flags can be replaced with their corresponding values.

### Examples

hpc-launch translates the command given on the command line `hpc-launch [mpi-options] <application> [application options]`  to `<exec> <default_options> [mapped mpi-options] <application> [application options]`, where the mapped `mpi-options` are mapped according to the mappings in the configuration.  The default mapping is `-n: <numproc_flag>`.
