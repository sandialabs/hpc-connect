# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

from typing import Type

import hpc_connect

from .backend import SlurmBackend


@hpc_connect.hookimpl
def hpc_connect_backend() -> "Type[hpc_connect.Backend]":
    return SlurmBackend
