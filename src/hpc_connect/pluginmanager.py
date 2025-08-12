# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT
import pluggy


class HPCConnectPluginManager(pluggy.PluginManager):
    def __init__(self):
        from . import hookspec
        from . import launch
        from . import submit

        super().__init__("hpc_connect")
        self.add_hookspecs(hookspec)
        for module in submit.plugins:
            self.register(module)
        for module in launch.plugins:
            self.register(module)
        self.load_setuptools_entrypoints("hpc_connect")
