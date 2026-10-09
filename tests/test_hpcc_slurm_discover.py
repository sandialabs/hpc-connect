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
        assert args == ["/usr/bin/sinfo", "-e", "-o", "%X %Y %Z %c %D %G"]
        assert check is True
        assert encoding == "utf-8"
        assert capture_output is True
        return SimpleNamespace(stdout=fake_stdout)

    monkeypatch.setattr("hpcc_slurm.discover.shutil.which", mock_which)
    monkeypatch.setattr("hpcc_slurm.discover.subprocess.run", mock_run)

    result = read_sinfo()

    assert result == [
        {
            "type": "node",
            "count": 16,
            "resources": [{"type": "cpu", "count": 128}, {"type": "gpu", "count": 1, "gres": "a40"}],
            "additional_properties": {
                "/usr/bin/sinfo -e -o '%X %Y %Z %c %D %G'": "2 64 1 128 16 gpu:a40:1(S:0-1)",
                "sockets_per_node": 2,
                "cores_per_socket": 64,
                "threads_per_core": 1,
                "cpus_per_node": 128,
                "gres": "gpu:a40:1",
            },
        },
        {
            "type": "node",
            "count": 32,
            "resources": [{"type": "cpu", "count": 128}, {"type": "gpu", "count": 4, "gres": "100"}],
            "additional_properties": {
                "/usr/bin/sinfo -e -o '%X %Y %Z %c %D %G'": "2 64 1 128 32 gpu:100:4(S:0-1)",
                "sockets_per_node": 2,
                "cores_per_socket": 64,
                "threads_per_core": 1,
                "cpus_per_node": 128,
                "gres": "gpu:100:4",
            },
        },
        {
            "type": "node",
            "count": 1445,
            "resources": [{"type": "cpu", "count": 32}],
            "additional_properties": {
                "/usr/bin/sinfo -e -o '%X %Y %Z %c %D %G'": "2 16+ 1 32+ 1445 (null)",
                "sockets_per_node": 2,
                "cores_per_socket": 16,
                "threads_per_core": 1,
                "cpus_per_node": 32,
                "gres": "None",
            },
        },
    ]


def _mock_sinfo(monkeypatch, stdout: str) -> None:
    monkeypatch.setattr("hpcc_slurm.discover.shutil.which", lambda cmd: "/usr/bin/sinfo")
    monkeypatch.setattr(
        "hpcc_slurm.discover.subprocess.run", lambda *a, **k: SimpleNamespace(stdout=stdout)
    )


def test_read_sinfo_default_uses_cpus_per_node(monkeypatch):
    """By default the CPU count is the %c value, ignoring threads_per_core."""
    _mock_sinfo(monkeypatch, "SOCKETS CORES THREADS CPUS NODES GRES\n2 64 2 128 16 (null)\n")

    result = read_sinfo()
    assert result is not None
    cpu = next(r for r in result[0]["resources"] if r["type"] == "cpu")
    assert cpu["count"] == 128  # %c, not 2*64*2
    assert result[0]["additional_properties"]["threads_per_core"] == 2
    assert result[0]["additional_properties"]["cpus_per_node"] == 128


def test_read_sinfo_allow_hyperthreading_multiplies_threads(monkeypatch):
    """With allow_hyperthreading the CPU count is sockets*cores*threads."""
    _mock_sinfo(monkeypatch, "SOCKETS CORES THREADS CPUS NODES GRES\n2 64 2 128 16 (null)\n")

    result = read_sinfo(allow_hyperthreading=True)
    assert result is not None
    cpu = next(r for r in result[0]["resources"] if r["type"] == "cpu")
    assert cpu["count"] == 256  # 2 * 64 * 2
    # Raw sinfo values are preserved in metadata.
    assert result[0]["additional_properties"]["cpus_per_node"] == 128
    assert result[0]["additional_properties"]["threads_per_core"] == 2


def test_read_sinfo_allow_hyperthreading_falls_back_when_factors_missing(monkeypatch):
    """A (null) factor makes the thread product undefined; fall back to %c."""
    # THREADS is (null) -> cannot compute sockets*cores*threads.
    _mock_sinfo(monkeypatch, "SOCKETS CORES THREADS CPUS NODES GRES\n2 16+ (null) 32+ 1445 (null)\n")

    result = read_sinfo(allow_hyperthreading=True)
    assert result is not None
    cpu = next(r for r in result[0]["resources"] if r["type"] == "cpu")
    assert cpu["count"] == 32  # falls back to %c


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


@pytest.mark.parametrize("gres", ["gpu:h100:4", "nic:mlx5:2", "mem:64G"])
def test_strip_gres_suffix_no_affinity(gres):
    assert strip_gres_suffix(gres) == gres
