import os
import pytest
from mylib.lib import (
    extract,
    load,
    query,
    data_transform,
    start_spark,
    end_spark,
)


@pytest.fixture(scope="module")
def spark():
    spark = start_spark("TestApp")
    yield spark
    end_spark(spark)


def test_extract():
    file_path = extract()
    assert os.path.exists(file_path) is True


def test_load(spark):
    df = load(spark)
    assert df is not None


def test_query(spark):
    df = load(spark)
    result = query(spark, df)
    assert result is None


def test_data_transform(spark):
    df = load(spark)
    result = data_transform(df)
    assert result is None


if __name__ == "__main__":
    test_extract()
    test_load(spark)
    test_query(spark)
    test_data_transform(spark)
