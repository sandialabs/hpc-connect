# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import os
from time import sleep
import canary

canary.directives.parameterize("it", list(range(20)))
canary.directives.keywords("long")

def test():
    self = canary.get_instance()
    print(f"running dummy test: it = {self.parameters.it}")
    t = 1 if os.getenv("GITLAB_CI") else 10
    sleep(t)


if __name__ == "__main__":
    test()
