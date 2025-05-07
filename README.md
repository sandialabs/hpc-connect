# hpc_connect

`hpc_connect` is a Python package that provides abstract interfaces to High-Performance Computing (HPC) schedulers and launchers. A default shell scheduler and MPI launcher is provided. Users can extend the functionality by subclassing the provided interfaces.

## Features

- **Abstract Interfaces**: Provides base classes for creating custom HPC schedulers and launchers.
- **Default Implementations**: Includes a default shell scheduler and an MPI launcher for immediate use.
- **Extensibility**: Easily create and register custom launchers and schedulers.

## Installation

You can install `hpc_connect` using pip:

```bash
python3 -m pip install hpc-connect
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
hpc-launch [mpi-options] <application> [application-options]
```

### Description

`hpc-launch` is a command line tool that forwards arguments to configured backend launchers such as `mpiexec`, `mpirun`, and `jsrun`.  `hpc-launch` works by translating the command given by

```console
hpc-launch [mpi-options] <application> [application options]
```

to

```console
<exec> <default-options> [mapped mpi-options] <application> [application-options]
```

where `default-options` and `mapped mpi-options` are replaced according to the mappings in the configuration.

`hpc-launch` provides a unified command structure for launching parallel applications, allowing users to execute their applications without needing to remember the specific syntax for each launcher.

### Configuration

The behavior of `hpc-launch` is determined by a YAML configuration file. The default configuration is:

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

Configuration variables can also be specified through environment variables named `HPCC_LAUNCH_NAME` where `NAME` is any one of the configuration variables given above.  Variables defined in the environment take precedent over variables defined in the configuration file.

### Examples

Perhaps the base way to describe the behavior of `hpc-connect` is through example

#### Example 1

Given the configuration

```yaml
hpc_connect:
  launch:
    vendor: mpich
    exec: mpiexec
    numproc_flag: -np
```

the command line

```console
hpc-launch -n 4 my-app ...
```

becomes

```console
mpiexec -np 4 my-app ...
```

> NOTE: the flag `-n` was replaced by `numproc_flag=-np`

> NOTE: the default flag mapping is `{'-n': '-np'}`

#### Example 2

Given the configuration

```yaml
hpc_connect:
  launch:
    vendor: mpich
    exec: mpiexec
    numproc_flag: -np
    default_options: --bind-to none
```

the command line

```console
hpc-launch -n 4 my-app ...
```

becomes

```console
mpiexec --bind-to none -np 4 my-app ...
```

#### Example 3

Given the configuration

```yaml
hpc_connect:
  launch:
    vendor: mpich
    exec: mpiexec
    numproc_flag: -np
    mappings:
      '--foo': '--bar'
      '--spam': '--eggs'
```

the command line

```console
hpc-launch -n 4 --foo=on --spam yummy my-app ...
```

becomes

```console
mpiexec --bind-to none -np 4 --bar=on --eggs yummy my-app ...
```

#### Example 4

Given the configuration

```yaml
hpc_connect:
  launch:
    vendor: mpich
    exec: mpiexec
    numproc_flag: -np
    default_options: --bind-to core --map-by ppr:%(np)d:numa
```

the command line

```console
hpc-launch -n 4 my-app ...
```

becomes

```console
mpiexec --bind-to core --map-by ppr:4:numa -np 4 my-app ...
```

#### Example 5

Given the configuration

```yaml
hpc_connect:
  launch:
    vendor: mpich
    exec: mpiexec
    numproc_flag: -np
    mappings:
      '--account': SUPPRESS
      '--clusters': SUPPRESS
```

the command line

```console
hpc-launch -n 4 --account=[MASKED] --clusters=my-cluster my-app ...
```

becomes

```console
mpiexec -np 4 my-app ...
```

#### Example 6

Given the configuration

```yaml
hpc_connect:
  launch:
    vendor: mpich
    exec: mpiexec
    numproc_flag: -np
    default_options: --bind-to core --map-by ppr:%(np)d:numa
    local_options: -H localhost
```

the command line

```console
hpc-launch -n 4 app-1 app-1-options. : -n 5 app-2 app-2-options
```

becomes

```console
mpiexec --bind-to core --map-by ppr:9:numa -H localhost -np 4 my-app -1 app-1-options : -H localhost -np 5 app-2 app-2-options
```

#### Example 7

Given the configuration

```yaml
hpc_connect:
  launch:
    vendor: schedmd
    exec: srun
```

the command line

```console
hpc-launch -n 4 app-1 app-1-options : -n 5 app-2 app-2-options
```

becomes

```console
srun -n9 --multi-prog launch-multi-prog.conf
```

where

```console
$ cat launch-multi-prog.conf
0-3 app-1 app-1-options
4-8 app-2 app-2-options
```
