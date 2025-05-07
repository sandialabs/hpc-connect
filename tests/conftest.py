# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import os

import pytest


@pytest.fixture(scope="session", autouse=True)
def add_mock_path():
    save_env = os.environ.copy()
    mock = os.path.join(os.path.dirname(__file__), "mock")
    assert os.path.exists(mock), mock
    os.environ["PATH"] = f"{mock}:{os.environ['PATH']}"
    yield
    os.environ.clear()
    os.environ.update(save_env)
