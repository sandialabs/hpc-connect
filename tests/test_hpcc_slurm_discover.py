from types import SimpleNamespace

import pytest

from hpcc_slurm.discover import read_sinfo
from hpcc_slurm.discover import safe_loads
from hpcc_slurm.discover import strip_gres_suffix
from hpcc_slurm.discover import strip_gres_suffixes


def test_read_sinfo_parses_first_data_line(monkeypatch):
    fake_stdout = """SOCKETS CORES THREADS CPUS NODES GRES
2 64 1 128 16 gpu:a40:1(S:0-1)
2 64 1 128 32 gpu:100:4(S:0-1)
2 16+ 1 32+ 1445 (null)
"""

    def mock_which(cmd):
        assert cmd == "sinfo"
        return "/usr/bin/sinfo"

    def mock_run(args, check, encoding, capture_output):
        assert args == ["/usr/bin/sinfo", "-o", "%X %Y %Z %c %D %G"]
        assert check is True
        assert encoding == "utf-8"
        assert capture_output is True
        return SimpleNamespace(stdout=fake_stdout)

    monkeypatch.setattr("hpcc_slurm.discover.shutil.which", mock_which)
    monkeypatch.setattr("hpcc_slurm.discover.subprocess.run", mock_run)

    result = read_sinfo()

    assert result == {
        "type": "node",
        "count": 16,
        "resources": [
            {
                "type": "socket",
                "count": 2,
                "resources": [
                    {
                        "type": "cpu",
                        "count": 64,
                    },
                ],
            },
            {
                "type": "gpu",
                "count": 1,
                "gres": "a40",
            },
        ],
        "additional_properties": {
            "/usr/bin/sinfo -o '%X %Y %Z %c %D %G'": "2 64 1 128 16 gpu:a40:1(S:0-1)",
            "sockets_per_node": 2,
            "cores_per_socket": 64,
            "threads_per_core": 1,
            "cpus_per_node": 128,
            "gres": "gpu:a40:1",
        },
    }


@pytest.mark.parametrize(
    "token,expected",
    [
        ("(null)", None),
        ("112", 112),
        ("32+", 32),
        ("gpu:h100:4", "gpu:h100:4"),
        ("gpu:a40:1(S:0-1)", "gpu:a40:1"),
    ],
)
def test_safe_loads(token, expected):
    assert safe_loads(token) == expected


@pytest.mark.parametrize(
    "gres_field,expected",
    [
        ("gpu:a40:1(S:0-1),nic:mlx5:1(S:0)", "gpu:a40:1,nic:mlx5:1"),
        ("gpu:h100:4, nic:mlx5:2(S:0)", "gpu:h100:4,nic:mlx5:2"),
    ],
)
def test_strip_gres_suffixes(gres_field, expected):
    assert strip_gres_suffixes(gres_field) == expected


@pytest.mark.parametrize(
    "gres,expected",
    [
        ("gpu:a40:1(S:0-1)", "gpu:a40:1"),
        ("gpu:a40:1(S:0-1,3-5)", "gpu:a40:1"),
        ("nic:mlx5:1(S:0)", "nic:mlx5:1"),
        ("mem:128G(S:0-3)", "mem:128G"),
        ("fpga:2(S:0-1)", "fpga:2"),
    ],
)
def test_strip_gres_suffix_affinity(gres, expected):
    assert strip_gres_suffix(gres) == expected


@pytest.mark.parametrize(
    "gres",
    [
        "gpu:h100:4",
        "nic:mlx5:2",
        "mem:64G",
    ],
)
def test_strip_gres_suffix_no_affinity(gres):
    assert strip_gres_suffix(gres) == gres
