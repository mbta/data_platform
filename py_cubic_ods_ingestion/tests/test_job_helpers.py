from py_cubic_ods_ingestion import job_helpers


def test_removeprefix() -> None:
    # test removing prefix
    assert "cubic_ods_qlik/" == job_helpers.removeprefix("local/cubic_ods_qlik/", "local/")

    # test prefix not found, hence not removed
    assert "local/cubic_ods_qlik/" == job_helpers.removeprefix("local/cubic_ods_qlik/", "not_found/")


def test_parse_args() -> None:
    # test passing invalid json blob for 'env'
    assert ({}, {}) == job_helpers.parse_args("", "{}")

    # test passing invalid json blob for 'input'
    assert ({}, {}) == job_helpers.parse_args("{}", "")

    # test passing valid, but empty, json blobs
    assert ({}, {}) == job_helpers.parse_args("{}", "{}")

    # test invalid keys are ignored
    # assert ({}, {}) == job_helpers.parse_args("{\"key\":\"invalid\"}", "{\"key\":\"invalid\"}")
