"""
Test configurations, setups, and fixtures
"""

from botocore.stub import Stubber
from py_cubic_ingestion import job_helpers
from pyspark.sql import SparkSession
from pyspark.sql.session import SparkSession as SparkSessionType
from typing import Iterator
import pytest


# fixtures
@pytest.fixture(name="spark_session")
def fixture_spark_session() -> SparkSessionType:
    """
    Creates Spark session for use in tests

    Returns
    -------
    DataFrame
        Spark Session available to use in tests
    """

    spark = SparkSession.builder.master("local").appName("test").getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    return spark


@pytest.fixture(name="glue_client")
def fixture_glue_client() -> Iterator[Stubber]:
    """
    Override Glue client with a Stubber
    """

    with Stubber(job_helpers.glue_client) as stubber:
        yield stubber
        stubber.assert_no_pending_responses()
