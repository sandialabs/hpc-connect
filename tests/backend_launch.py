# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import hpc_connect


def test_backend_build_launch_argv_local_matches_launcher(capfd):
    backend = hpc_connect.get_backend("local")
    args = ["-n", "4", "-flag", "file", "executable", "--option"]

    argv = backend.build_launch_argv(args)
    launcher = backend.launcher()
    launcher(args)
    out = capfd.readouterr().out.strip()

    assert out == " ".join(argv)


def test_backend_launch_uses_backend_owned_path(capfd):
    backend = hpc_connect.get_backend("local")
    args = ["-n", "2", "executable"]

    backend.launch(args)
    out = capfd.readouterr().out.strip()
    assert out == " ".join(backend.build_launch_argv(args))
