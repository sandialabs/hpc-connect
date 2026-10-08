from hpcc_flux.discover import parse_resource_info
from hpcc_flux.discover import parse_resource_list
from hpcc_flux.discover import read_resource_info


def test_parse_resource_list_builds_multiple_node_groups():
    result = parse_resource_list("2|64|4|free|node[0-1]\n1|32|0|allocated|node2\n")

    assert result == [
        {
            "type": "node",
            "count": 2,
            "resources": [{"type": "cpu", "count": 32}, {"type": "gpu", "count": 2}],
            "additional_properties": {
                "state": "free",
                "nodelist": "node[0-1]",
                "flux_resource_list_line": "2|64|4|free|node[0-1]",
            },
        },
        {
            "type": "node",
            "count": 1,
            "resources": [{"type": "cpu", "count": 32}],
            "additional_properties": {
                "state": "allocated",
                "nodelist": "node2",
                "flux_resource_list_line": "1|32|0|allocated|node2",
            },
        },
    ]


def test_parse_resource_list_rejects_non_uniform_per_node_counts():
    assert parse_resource_list("2|65|0|free|node[0-1]\n") is None


def test_read_resource_info_prefers_resource_list(monkeypatch):
    def mock_which(cmd):
        assert cmd == "flux"
        return "/usr/bin/flux"

    def mock_check_output(args, encoding):
        assert encoding == "utf-8"
        if args == [
            "/usr/bin/flux",
            "resource",
            "list",
            "-s",
            "all",
            "-n",
            "-o",
            "{nnodes}|{ncores}|{ngpus}|{state}|{nodelist}",
        ]:
            return "2|64|4|free|node[0-1]\n1|32|0|allocated|node2\n"
        raise AssertionError(args)

    monkeypatch.setattr("hpcc_flux.discover.shutil.which", mock_which)
    monkeypatch.setattr("hpcc_flux.discover.subprocess.check_output", mock_check_output)

    result = read_resource_info()

    assert result is not None
    assert [entry["count"] for entry in result] == [2, 1]


def test_read_resource_info_uses_totals_only_for_single_node(monkeypatch):
    def mock_which(cmd):
        assert cmd == "flux"
        return "/usr/bin/flux"

    def mock_check_output(args, encoding):
        assert encoding == "utf-8"
        if args == [
            "/usr/bin/flux",
            "resource",
            "list",
            "-s",
            "all",
            "-n",
            "-o",
            "{nnodes}|{ncores}|{ngpus}|{state}|{nodelist}",
        ]:
            raise RuntimeError("unexpected")
        if args == ["/usr/bin/flux", "resource", "info"]:
            return "1 Nodes, 32 Cores, 2 GPUs"
        raise AssertionError(args)

    monkeypatch.setattr("hpcc_flux.discover.shutil.which", mock_which)

    def failing_list_then_info(args, encoding):
        if args[2] == "list":
            raise subprocess.CalledProcessError(returncode=1, cmd=args)
        return mock_check_output(args, encoding)

    import subprocess

    monkeypatch.setattr("hpcc_flux.discover.subprocess.check_output", failing_list_then_info)

    result = read_resource_info()

    assert result == [
        {
            "type": "node",
            "count": 1,
            "resources": [{"type": "cpu", "count": 32}, {"type": "gpu", "count": 2}],
            "additional_properties": {"flux_resource_info": "1 Nodes, 32 Cores, 2 GPUs"},
        }
    ]


def test_read_resource_info_rejects_multi_node_totals_only(monkeypatch):
    def mock_which(cmd):
        assert cmd == "flux"
        return "/usr/bin/flux"

    import subprocess

    def mock_check_output(args, encoding):
        assert encoding == "utf-8"
        if args[2] == "list":
            raise subprocess.CalledProcessError(returncode=1, cmd=args)
        if args == ["/usr/bin/flux", "resource", "info"]:
            return "2 Nodes, 64 Cores, 4 GPUs"
        raise AssertionError(args)

    monkeypatch.setattr("hpcc_flux.discover.shutil.which", mock_which)
    monkeypatch.setattr("hpcc_flux.discover.subprocess.check_output", mock_check_output)

    assert read_resource_info() is None


def test_parse_resource_info():
    assert parse_resource_info("1 Nodes, 32 Cores, 2 GPUs") == {"nodes": 1, "cpu": 32, "gpu": 2}


def test_flux_discovery_does_not_invent_socket_resources():
    result = parse_resource_list("2|64|4|free|node[0-1]\n")

    assert result is not None
    assert result[0]["resources"] == [{"type": "cpu", "count": 32}, {"type": "gpu", "count": 2}]
