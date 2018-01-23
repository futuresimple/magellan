import os

import pytest
from pyspark.sql import DataFrame, SparkSession

from magellan.utils import inject_rules

pending = pytest.mark.skip

ROOT = os.path.abspath(os.path.join(__file__, '../..'))

MAGELLAN_PACKAGE = 'harsha2010:magellan:1.0.5-s_2.11'


@pytest.fixture(scope="session")
def spark():
    submit_args = []
    submit_args += ['--packages', MAGELLAN_PACKAGE]
    submit_args.append('pyspark-shell')
    os.environ['PYSPARK_SUBMIT_ARGS'] = " ".join(submit_args)

    spark_session = SparkSession.builder \
        .appName('py.test') \
        .getOrCreate()

    inject_rules(spark_session)
    print "spark session ready"

    return spark_session


def dfassert(left, right, use_set=False, skip_extra_columns=False):
    if not isinstance(right, DataFrame):
        right = left.sql_ctx.createDataFrame(right)

    if skip_extra_columns:
        columns = list(set(left.columns) & set(right.columns))
        left = left[columns]
        right = right[columns]

    assert sorted(left.columns) == sorted(right.columns)

    def _orderable_columns(df):
        return df.columns
        # return [col for col in df.columns if df[col].dataType.typeName() != 'array']

    left = left[sorted(left.columns)]
    right = right[sorted(right.columns)]

    converter = set if use_set else list

    ordered_left = left.orderBy(*_orderable_columns(left)) if _orderable_columns(left) else left
    ordered_right = right.orderBy(*_orderable_columns(right)) if _orderable_columns(right) else right

    assert converter(ordered_left.collect()) == converter(ordered_right.collect())
