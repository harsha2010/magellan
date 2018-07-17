from pyspark import SparkConf, SQLContext, HiveContext
from pyspark.sql import DataFrame
import pytest
from base.spark.extensions import *

import os

pending = pytest.mark.xfail

ROOT = os.path.abspath(os.path.join(__file__, '../..'))

@pytest.fixture(scope="session")
def sparkContext():

    conf = SparkConf() \
        .setAppName('py.test')

    sc = SparkContext(conf=conf)

    # disable logging

    sc.setLogLevel("OFF")

    return sc


@pytest.fixture(scope="session")
def sqlContext(sparkContext):
    return SQLContext(sparkContext)


@pytest.fixture(scope="session")
def hiveContext(sparkContext):
    return HiveContext(sparkContext)


def dfassert(left, right, useSet=False, skipExtraColumns=False):
    if not isinstance(right, DataFrame):
        right = left.sql_ctx.createDataFrame(right)

    if skipExtraColumns:
        columns = list(set(left.columns) & set(right.columns))
        left = left[columns]
        right = right[columns]

    assert sorted(left.columns) == sorted(right.columns)

    def _orderableColumns(df):
        return [col for col in df.columns if df[col].dataType.typeName() != 'array']

    left = left[sorted(left.columns)]
    right = right[sorted(right.columns)]

    converter = set if useSet else list

    orderedLeft = left.orderBy(*_orderableColumns(left)) if _orderableColumns(left) else left
    orderedRight = right.orderBy(*_orderableColumns(right)) if _orderableColumns(right) else right

    assert converter(orderedLeft.collect()) == converter(orderedRight.collect())
