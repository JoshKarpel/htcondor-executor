from htcondor_executor import HTCondorExecutor

import dask
import dask.array as da
import numpy as np


def test_works_as_dask_executor():
    with HTCondorExecutor() as pool:
        with dask.config.set(pool = pool):
            x = da.sum(da.ones(5)) ** 2
            y = x.compute()

    assert y == 25
