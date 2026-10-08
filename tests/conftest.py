# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import os

import pytest

import hpc_connect.config as config_mod


@pytest.fixture(scope="session", autouse=True)
def add_mock_path():
    save_env = os.environ.copy()
    mock = os.path.join(os.path.dirname(__file__), "mock")
    assert os.path.exists(mock), mock
    os.environ["PATH"] = f"{mock}:{os.environ['PATH']}"
    yield
    os.environ.clear()
    os.environ.update(save_env)


@pytest.fixture(scope="function", autouse=True)
def reset_env():
    save_env = os.environ.copy()
    config_mod._config = None
    for key in list(os.environ.keys()):
        if key.startswith("HPC_CONNECT_"):
            os.environ.pop(key)
    yield
    config_mod._config = None
    os.environ.clear()
    os.environ.update(save_env)


@pytest.fixture
def clear_config_cache():
    def clear() -> None:
        config_mod._config = None

    return clear
