from htcondor_executor import HTCondorExecutor


def test_execute_directory_is_closed_after_context():
    with HTCondorExecutor() as pool:
        assert pool._dir.exists()

    assert pool._dir._closed
