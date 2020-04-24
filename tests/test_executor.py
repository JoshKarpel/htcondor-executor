from htcondor_executor import HTCondorExecutor


def test_submit():
    with HTCondorExecutor() as pool:
        assert pool.submit(lambda x: 2 * x, 2).result(timeout=20) == 4
