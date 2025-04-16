import pluggy

from . import hookspec
from .impl import builtin


def factory() -> pluggy.PluginManager:
    manager = pluggy.PluginManager("hpc_connect")
    manager.add_hookspecs(hookspec)
    for module in builtin:
        manager.register(module)
    manager.load_setuptools_entrypoints("hpc_connect")
    return manager


manager = factory()
