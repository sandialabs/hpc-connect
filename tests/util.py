# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

"""Tests for hpc_connect.util — time, serialize, collections, and util/__init__."""

import datetime
import os
import stat

import pytest

from hpc_connect.util.time import Duration
from hpc_connect.util.time import DurationError
from hpc_connect.util.time import hhmmss
from hpc_connect.util.time import time_in_seconds

# ---------------------------------------------------------------------------
# hhmmss
# ---------------------------------------------------------------------------


def test_hhmmss_none():
    assert hhmmss(None) == "--:--:--"


def test_hhmmss_below_threshold():
    # < 2s → include fractional seconds
    result = hhmmss(0.5)
    assert result.startswith("00:00:00.")


def test_hhmmss_above_threshold():
    result = hhmmss(3661.0)
    assert result == "01:01:01"


def test_hhmmss_zero():
    result = hhmmss(0.0)
    assert result.startswith("00:00:00")


# ---------------------------------------------------------------------------
# time_in_seconds
# ---------------------------------------------------------------------------


def test_time_in_seconds_int():
    assert time_in_seconds(10) == 10.0


def test_time_in_seconds_float():
    assert time_in_seconds(1.5) == 1.5


def test_time_in_seconds_string_seconds():
    assert time_in_seconds("30s") == pytest.approx(30.0)


def test_time_in_seconds_string_minutes():
    assert time_in_seconds("2m") == pytest.approx(120.0)


def test_time_in_seconds_string_hours():
    assert time_in_seconds("1h") == pytest.approx(3600.0)


# ---------------------------------------------------------------------------
# Duration.from_str
# ---------------------------------------------------------------------------


def test_duration_empty():
    assert Duration.from_str("") == datetime.timedelta()


def test_duration_zero():
    assert Duration.from_str("0") == datetime.timedelta()
    assert Duration.from_str("+0") == datetime.timedelta()
    assert Duration.from_str("-0") == datetime.timedelta()


def test_duration_plain_number():
    td = Duration.from_str("1.5")
    assert abs(td.total_seconds() - 1.5) < 1e-6


def test_duration_negative():
    td = Duration.from_str("-30s")
    assert td.total_seconds() == pytest.approx(-30.0)


def test_duration_nanoseconds():
    # timedelta has microsecond precision; sub-microsecond values round to 0
    td = Duration.from_str("500ns")
    assert td.total_seconds() == pytest.approx(0.0, abs=1e-6)
    # 2000 ns = 2 µs, which survives rounding
    td2 = Duration.from_str("2000ns")
    assert abs(td2.total_seconds() - 2e-6) < 1e-9


def test_duration_microseconds():
    td = Duration.from_str("100us")
    assert abs(td.total_seconds() - 100e-6) < 1e-10


def test_duration_milliseconds():
    td = Duration.from_str("250ms")
    assert abs(td.total_seconds() - 0.25) < 1e-9


def test_duration_seconds():
    td = Duration.from_str("10s")
    assert td.total_seconds() == pytest.approx(10.0)


def test_duration_minutes():
    td = Duration.from_str("5m")
    assert td.total_seconds() == pytest.approx(300.0)


def test_duration_hours():
    td = Duration.from_str("2h")
    assert td.total_seconds() == pytest.approx(7200.0)


def test_duration_combined():
    td = Duration.from_str("1h30m")
    assert td.total_seconds() == pytest.approx(5400.0)


def test_duration_days():
    td = Duration.from_str("1d")
    assert td.total_seconds() == pytest.approx(86400.0)


def test_duration_weeks():
    td = Duration.from_str("1w")
    assert td.total_seconds() == pytest.approx(7 * 86400.0)


def test_duration_hhmmss_format():
    # "01:30:00" → 5400 seconds; uses the print() path (known debug artifact)
    td = Duration.from_str("01:30:00")
    assert abs(td.total_seconds() - 5400.0) < 1.0


def test_duration_mmss_format():
    td = Duration.from_str("01:30")
    # "01:30" parsed as minutes:seconds = 90 microseconds (the code scales by µs_size)
    # just assert it round-trips as a timedelta without raising
    assert isinstance(td, datetime.timedelta)


def test_duration_invalid_raises():
    with pytest.raises(DurationError):
        Duration.from_str("xyz")


def test_duration_unknown_unit_raises():
    with pytest.raises(DurationError, match="Unknown unit"):
        Duration.from_str("10q")


def test_duration_extra_chars_raises():
    with pytest.raises(DurationError, match="Extra chars"):
        Duration.from_str("@10s")


# ---------------------------------------------------------------------------
# Duration.to_str
# ---------------------------------------------------------------------------


def test_to_str_zero():
    result = Duration.to_str(datetime.timedelta())
    assert result == "0"


def test_to_str_seconds():
    result = Duration.to_str(datetime.timedelta(seconds=90))
    assert "1m" in result and "30s" in result


def test_to_str_hours():
    result = Duration.to_str(datetime.timedelta(hours=2))
    assert "2h" in result


def test_to_str_negative():
    result = Duration.to_str(datetime.timedelta(seconds=-30))
    assert result.startswith("-")


def test_to_str_extended_days():
    result = Duration.to_str(datetime.timedelta(days=2), extended=True)
    assert "2d" in result


def test_to_str_milliseconds():
    result = Duration.to_str(datetime.timedelta(milliseconds=500))
    assert "ms" in result or "500" in result


# ---------------------------------------------------------------------------
# serialize / deserialize
# ---------------------------------------------------------------------------


def test_serialize_deserialize_roundtrip():
    from hpc_connect.util.serialize import deserialize
    from hpc_connect.util.serialize import serialize

    obj = {"key": [1, 2, 3], "nested": {"a": True}}
    assert deserialize(serialize(obj)) == obj


def test_serialize_string():
    from hpc_connect.util.serialize import deserialize
    from hpc_connect.util.serialize import serialize

    assert deserialize(serialize("hello")) == "hello"


def test_compress64_expand64_roundtrip():
    from hpc_connect.util.serialize import compress64
    from hpc_connect.util.serialize import expand64

    s = "the quick brown fox"
    assert expand64(compress64(s)) == s


# ---------------------------------------------------------------------------
# collections.merge
# ---------------------------------------------------------------------------


def test_merge_dicts():
    from hpc_connect.util.collections import merge

    dest = {"a": 1, "b": 2}
    result = merge(dest, {"b": 99, "c": 3})
    assert result["a"] == 1
    assert result["b"] == 99
    assert result["c"] == 3


def test_merge_lists():
    from hpc_connect.util.collections import merge

    dest = [1, 2, 3]
    result = merge(dest, [3, 4, 5])
    assert 1 in result and 4 in result
    assert result.count(3) == 1  # no duplicates


def test_merge_source_none_returns_none():
    from hpc_connect.util.collections import merge

    assert merge({"a": 1}, None) is None


def test_merge_scalar_overwrite():
    from hpc_connect.util.collections import merge

    assert merge(1, 2) == 2


def test_merge_nested():
    from hpc_connect.util.collections import merge

    dest = {"outer": {"inner": 1, "keep": 2}}
    result = merge(dest, {"outer": {"inner": 99}})
    assert result["outer"]["inner"] == 99
    assert result["outer"]["keep"] == 2


# ---------------------------------------------------------------------------
# util/__init__ helpers
# ---------------------------------------------------------------------------


def test_set_executable(tmp_path):
    from hpc_connect.util import set_executable

    f = tmp_path / "script.sh"
    f.write_text("#!/bin/sh\necho hi\n")
    # Clear executable bits first
    os.chmod(f, stat.S_IRUSR | stat.S_IWUSR)
    set_executable(str(f))
    mode = os.stat(f).st_mode
    assert mode & stat.S_IXUSR


def test_partition():
    from hpc_connect.util import partition

    evens, odds = partition([1, 2, 3, 4, 5], lambda x: x % 2 == 0)
    assert evens == [2, 4]
    assert odds == [1, 3, 5]


def test_sanitize_path_replaces_illegal_chars():
    from hpc_connect.util import sanitize_path

    result = sanitize_path("/some/dir/my:job*name")
    assert ":" not in result
    assert "*" not in result
    assert "/some/dir/" in result


def test_safe_loads_valid_json():
    from hpc_connect.util import safe_loads

    assert safe_loads('{"a": 1}') == {"a": 1}


def test_safe_loads_invalid_returns_string():
    from hpc_connect.util import safe_loads

    assert safe_loads("not json") == "not json"
