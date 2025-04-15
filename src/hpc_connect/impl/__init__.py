# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

from . import flux_hl
from . import pbs
from . import shell
from . import slurm

builtin = [flux_hl, pbs, shell, slurm]
