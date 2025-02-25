import pluggy

hookspec = pluggy.HookspecMarker("hpc_connect")
hookimpl = pluggy.HookimplMarker("hpc_connect")


@hookspec
def hpc_connect_scheduler():
    """HPC scheduler implementation"""


@hookspec
def hpc_connect_launcher():
    """HPC launcher implementation"""
