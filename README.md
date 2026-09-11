# hpc-connect

`hpc_connect` is a lightweight, configurable Python interface to HPC schedulers
and program launchers. It provides a single, scheduler-agnostic API for
submitting batch jobs and launching parallel applications, so the same code can
run against Slurm, PBS, Flux, a remote host, or the local machine simply by
changing configuration.

Support for each scheduler is provided by a backend plugin. The following
backends ship with the package:

| Backend      | Type string          | Notes                                             |
| ------------ | -------------------- | ------------------------------------------------- |
| Local        | `local`              | Runs jobs directly on the current machine         |
| Slurm        | `slurm`              | `sbatch`/`sacct` submission, `srun` or MPI launch |
| PBS          | `pbs`                | PBS Pro submission                                |
| Flux         | `flux`               | Native-Python Flux backend                        |
| Flux (shell) | `flux.sh`            | Flux backend driven through the shell             |
| Remote       | `remote_subprocess`  | Dispatches to a remote host over a subprocess     |

## Installation

```console
python3 -m pip install hpc-connect
```

Requires Python 3.10 or newer.

## Concepts

`hpc_connect` is built around a few small pieces:

- **`Backend`** — a scheduler abstraction selected from configuration. A backend
  knows the machine's resources (nodes, sockets, CPUs, GPUs) and hands out a
  submission manager and a launcher.
- **`JobSpec`** — a declarative, backend-agnostic description of a single
  submission: the commands to run, the resources requested, a time limit,
  environment, and I/O.
- **submission manager** — translates a `JobSpec` into a real scheduler
  submission and returns a `Future`.
- **`Future`** — a handle to a submitted job. It exposes the job id, return
  code, completion metadata, and start/jobid/done callbacks, and can be waited
  on individually or with `as_completed`.
- **launcher** — builds and runs the parallel-launch command line
  (`mpiexec`, `srun`, …) for an application.

## Python usage

### Selecting a backend

```python
import hpc_connect

# Select by configured instance name, by backend type, or fall back to the
# single configured backend / the configured default.
backend = hpc_connect.get_backend("slurm")

print(backend.describe())  # human-readable resource summary
print(backend.node_count)  # discovered node count
print(backend.count_per_node("cpu"))
```

`get_backend(name)` resolves `name` against the configuration in this order:

1. a backend entry whose `name` matches,
2. otherwise a backend entry whose `type` matches,
3. otherwise `name` is treated as a backend type with no per-entry overrides.

Called with no argument, it uses the configured default `backend`; if none is
set but exactly one backend is configured, that one is used; otherwise the call
is ambiguous and raises.

Use `hpc_connect.backends()` to list the registered backend types.

### Submitting a job

Describe the work with a `JobSpec`, submit it through the backend's submission
manager, and wait on the returned `Future`:

```python
import hpc_connect

backend = hpc_connect.get_backend("slurm")

job = hpc_connect.JobSpec(
    name="solve",
    commands=["./solver --input deck.yaml"],
    nodes=2,
    cpus=64,
    gpus=0,
    time_limit=1800.0,  # seconds
    env={"OMP_NUM_THREADS": "4"},
    output="solve.out",
    error="solve.err",
)

future = backend.submission_manager().submit(job)

# Observe lifecycle events (each fires once, immediately if already reached).
future.add_jobid_callback(lambda f: print("queued as", f.jobid))
future.add_jobstart_callback(lambda f: print("running"))
future.add_done_callback(lambda f: print("done", f.returncode))

returncode = future.result()  # blocks until the job finishes
info = future.proc_info()  # scheduler/process metadata
```

`JobSpec` is immutable; derive variants with `job.with_updates(...)` or
`job.with_dependencies([...])`. Backends that report `supports_dependencies()`
honor `dependencies` (e.g. Slurm `afterany`).

Use `submission_manager().popen(job)` to obtain the underlying process directly
instead of a `Future`, when you want to manage polling yourself.

### Waiting on many jobs

```python
from hpc_connect import as_completed

futures = [backend.submission_manager().submit(j) for j in jobs]
for future in as_completed(futures, timeout=3600):
    print(future.jobid, future.returncode)
```

`as_completed` yields futures as they finish and cancels any still pending if a
timeout or exception occurs.

### Launching an application

A launcher builds the scheduler-appropriate parallel-launch command line and
runs it:

```python
backend = hpc_connect.get_backend()
launcher = backend.launcher()
result = launcher(["-n", "16", "./my_app", "--flag"], echo=True)
print(result.returncode)
```

The command line is assembled roughly as:

```
<exec> <default_options> [user options] <pre_options> <application> [app options]
```

The launcher infers the process count from `-n`/`-np` (or the backend's
`numproc_flag`) and supports MPMD job specifications (segments separated by
`:`).

### Sizing resources

Backends expose helpers for planning allocations from a machine's discovered
topology:

```python
backend.node_count  # total nodes
backend.count_per_node("gpu")  # GPUs per node
backend.nodes_required(cpu=256)  # nodes needed for 256 CPU tasks
backend.resource_view(ranks=128)  # {np, ranks, nodes, sockets, ranks_per_socket}
```

## Command-line tools

Two entry points are installed:

### `hpcc`

`hpcc` is the front-end wrapper. It exposes subcommands and applies
configuration overrides:

```console
hpcc --info                            # show overview / configuration help
hpcc --version
hpcc -c backend:slurm launch -- ./my_app --flag
```

Configuration can be overridden inline with `-c path:to:key:value`.

### `hpc-launch`

`hpc-launch` is a convenience shim equivalent to `hpcc launch`. It launches an
application through the selected backend's launch configuration:

```console
hpc-launch [launch options] <application> [application options]
hpc-launch --dryrun -n 16 ./my_app     # print the command line without running
```

## Configuration

Configuration is YAML under a top-level `hpc_connect:` key. The minimal case
just names a default backend:

```yaml
hpc_connect:
  backend: slurm
```

The full shape is:

```yaml
hpc_connect:
  debug: false

  # Optional default backend for the CLI and get_backend() with no argument.
  backend: this-site

  backends:
    - name: this-site         # optional instance name (defaults to `type`)
      type: slurm             # required backend type (plugin id)
      config: {}              # backend-specific settings

      # How applications are launched under this backend.
      launch:
        type: srun            # e.g. "mpi" or "srun"
        exec: srun            # launch executable (backend may default this)
        numproc_flag: -n      # flag preceding the process count
        default_options: []   # options placed before user arguments
        pre_options: []       # options placed immediately before the application
        variables: {}         # environment overrides (name -> value)
        mpmd:
          global_options: []
          local_options: []

      # How jobs are submitted under this backend.
      submit:
        default_options: []   # options applied to every submission
        polling_interval: 15.0
```

### Configuration scopes

Configuration is read and merged from three scopes (later scopes override
earlier ones):

1. **Site** — `sys.prefix/etc/hpc_connect/config.yaml`
   (or `$HPC_CONNECT_SITE_CONFIG`)
2. **Global** — `~/.config/hpc_connect.yaml`
   (or `$HPC_CONNECT_GLOBAL_CONFIG`, or `$XDG_CONFIG_HOME/hpc_connect/config.yaml`)
3. **Local** — `./hpc_connect.yaml`

### Environment variables

| Variable                   | Purpose                                                     |
| -------------------------- | ----------------------------------------------------------- |
| `HPC_CONNECT_DEBUG`        | Enable debug logging (`yes`/`true`/`1`/`on`)                |
| `HPC_CONNECT_LOG_LEVEL`    | Set the log level explicitly (e.g. `INFO`, `DEBUG`)         |
| `HPC_CONNECT_SITE_CONFIG`  | Override the site configuration file path                   |
| `HPC_CONNECT_GLOBAL_CONFIG`| Override the global configuration file path                 |
| `HPC_CONNECT_HOSTFILE`     | JSON file mapping host globs to resource specs (local backend) |
| `HPC_CONNECT_HOSTNAME`     | Hostname used to match entries in `HPC_CONNECT_HOSTFILE`    |

### Example configurations

Local machine with an MPICH-style launcher:

```yaml
hpc_connect:
  backend: local
  backends:
    - type: local
      launch:
        type: mpi
        exec: mpiexec
        numproc_flag: -np
```

Slurm with `srun`:

```yaml
hpc_connect:
  backend: slurm
  backends:
    - type: slurm
      launch:
        type: srun
        exec: srun
```

## Extending: writing a backend plugin

Backends are discovered through the `hpc_connect` entry-point group, so a new
scheduler can be added from a separate distribution without modifying
`hpc_connect`. A plugin registers a `Backend` subclass via the
`hpc_connect_backend` hook:

```python
import hpc_connect


class MyBackend(hpc_connect.Backend):
    type = "mybackend"

    @classmethod
    def default_config(cls) -> dict: ...

    @property
    def resource_specs(self) -> list[dict]: ...

    @property
    def valid_launchers(self) -> set[str]: ...

    def submission_manager(self) -> hpc_connect.HPCSubmissionManager: ...

    def launcher(self) -> hpc_connect.HPCLauncher: ...


@hpc_connect.hookimpl
def hpc_connect_backend():
    return MyBackend
```

Expose it in the plugin's `pyproject.toml`:

```toml
[project.entry-points."hpc_connect"]
mybackend = "my_package"
```

The bundled backends (`hpcc_slurm`, `hpcc_pbs`, `hpcc_flux`, `hpcc_remote`) are
registered the same way and serve as reference implementations.

## License

Distributed under the MIT License. See `COPYRIGHT` and `LICENSE` for details.
