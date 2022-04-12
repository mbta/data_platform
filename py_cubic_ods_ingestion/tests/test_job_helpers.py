"""
Testing module for `job_helpers.py`.
"""

import json
import pytest

from py_cubic_ods_ingestion import job_helpers


def test_removeprefix() -> None:
    """
    Testing removing of prefix from string.
    """

    # test removing prefix
    assert "cubic_ods_qlik/" == job_helpers.removeprefix("local/cubic_ods_qlik/", "local/")

    # test prefix not found, hence not removed
    assert "local/cubic_ods_qlik/" == job_helpers.removeprefix("local/cubic_ods_qlik/", "not_found/")


def test_parse_args() -> None:
    """
    Testing parsing of the arguments from JSON blobs to dicts.
    """

    # test passing invalid json blob for 'env_arg' raises error
    with pytest.raises(json.JSONDecodeError):
        job_helpers.parse_args("", "{}")

    # test passing invalid json blob for 'input_arg' raises error
    with pytest.raises(json.JSONDecodeError):
        job_helpers.parse_args("{}", "")

    # test passing valid, but empty, json blobs
    assert ({}, {}) == job_helpers.parse_args("{}", "{}")

    # test invalid keys are ignored
    # assert ({}, {}) == job_helpers.parse_args("{\"key\":\"invalid\"}", "{\"key\":\"invalid\"}")


def test_get_table_name_suffix() -> None:
    """
    Testing table name adjustment for change tracking prefixes.
    """

    # without ct
    assert "" == job_helpers.get_table_name_suffix("cubic_ods_qlik/EDW.TEST/")

    # with ct
    assert "__ct" == job_helpers.get_table_name_suffix("cubic_ods_qlik/EDW.TEST__ct/")
