# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT
import sys
import warnings
from typing import TYPE_CHECKING

import pluggy

from . import hookspec
from . import local

if TYPE_CHECKING:
    from .backend import Backend
    from .config import Config
    from .launch import HPCLauncher

warnings.simplefilter("once", DeprecationWarning)


class HPCConnectPluginManager(pluggy.PluginManager):
    def __init__(self):
        super().__init__(hookspec.project_name)
        self.add_hookspecs(hookspec)
        self.register(local)
        self.load_setuptools_entrypoints(hookspec.project_name)

    def consider_plugin(self, name: str) -> None:
        assert isinstance(name, str), f"module name as text required, got {name!r}"
        if name.startswith("no:"):
            self.unregister(name=name[3:])
            self.set_blocked(name[3:])
        else:
            self.import_plugin(name)

    def import_plugin(self, name: str) -> None:
        """Import a plugin with ``name``."""
        assert isinstance(name, str), f"module name as text required, got {name!r}"

        if self.is_blocked(name) or self.get_plugin(name) is not None:
            return

        try:
            __import__(name)
        except ImportError as e:
            msg = f"Error importing plugin {name!r}: {e.args[0]}"
            raise ImportError(msg).with_traceback(e.__traceback__) from e
        else:
            mod = sys.modules[name]
            if mod in self._name2plugin.values():
                other = next(k for k, v in self._name2plugin.items() if v == mod)
                msg = f"Plugin {name} already registered under the name {other}"
                raise PluginAlreadyImportedError(msg)
            self.register(mod, name)


_pm: HPCConnectPluginManager | None = None


def pm() -> HPCConnectPluginManager:
    global _pm
    if _pm is None:
        _pm = HPCConnectPluginManager()
    return _pm


def get_backend(config: "Config | None" = None) -> "Backend":
    from .config import Config

    config = config or Config.from_defaults()
    backend: Backend = pm().hook.hpc_connect_backend(config=config)
    if backend is not None:
        backend.validate()
        return backend
    raise ValueError(f"No backend for {config.backend}")


def get_launcher(config: "Config | None" = None) -> "HPCLauncher":
    from .config import Config

    config = config or Config.from_defaults()
    backend = get_backend(config=config)
    launcher: "HPCLauncher" = backend.launcher()
    if launcher is not None:
        return launcher
    raise ValueError(f"No matching launcher manager for {config.launch.exec!r}")


class PluginAlreadyImportedError(Exception): ...
