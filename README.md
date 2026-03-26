# hpc_connect

`hpc_connect` provides Python interfaces for HPC backends (local, Slurm, PBS, Flux, …) and launch (MPI-style mpiexec/mpirun, Slurm srun, etc.). Backends are provided via plugins; configuration selects a backend and optionally overrides its defaults.
Install

```console
python3 -m pip install hpc-connect
```

## Python usage

```python
import hpc_connect
# Select by backend instance name or backend type (see config below)
backend = hpc_connect.get_backend("slurm")
```

If you have multiple configured backends, pass an explicit name/type or set hpc_connect.backend in YAML.

## Configuration

`Config` is YAML under the `hpc_connect`: key.

Minimal config

```yaml
hpc_connect:
  backend: slurm
```

Full config shape

```yaml
hpc_connect:
  debug: false

  # Optional default backend selector for CLI / get_backend() with no argument
  backend: this-site

  backends:
    - name: this-site        # optional instance name
      type: slurm            # required backend type (plugin id)
      config: {}             # backend-specific settings (plugin-defined)

      # optional: launch settings for this backend instance
      launch:
        type: srun           # e.g. "mpi" or "srun"
        exec: srun           # optional; adapter may provide a default
        numproc_flag: -n
        default_options: []
        pre_options: []
        variables: {}        # string->string map (if adapters use it)
        mpmd:
          global_options: []
          local_options: []

      # optional: submit/poll tuning (backend-defined semantics)
      submit:
        default_options: []
        polling_interval: 2.0
```

Backend selection rules

`get_backend("X")` selects:

- a backend entry with name == "X", else
- a backend entry with type == "X", else
- treats "X" as a backend type with no per-entry overrides.

If no argument is provided, it uses `hpc_connect.backend` if set; otherwise, if exactly one backend entry exists, it selects that; else it errors (ambiguous).

## hpc-launch (CLI)

`hpc-launch` runs an application using the selected backend and its launch configuration:

```console
hpc-launch [launcher-options] <application> [application-options]
```

The command line is constructed roughly as:

```console
<exec> <default_options> [launcher-options] <pre_options> <application> [application-options]
```

MPMD is supported via launch.mpmd.* when enabled by the launcher type.

### Examples

Local + MPICH-style -np

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

Slurm + srun

```yaml
hpc_connect:
  backend: slurm
  backends:
    - type: slurm
      launch:
        type: srun
        exec: srun
```
