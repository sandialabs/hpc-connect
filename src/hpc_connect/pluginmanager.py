# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT
import pluggy


class HPCConnectPluginManager(pluggy.PluginManager):
    def __init__(self):
        from . import discover
        from . import hookspec

        super().__init__("hpc_connect")
        self.add_hookspecs(hookspec)
        self.register(discover)
        self.load_setuptools_entrypoints("hpc_connect")
