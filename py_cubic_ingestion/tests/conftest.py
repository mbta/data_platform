"""
Test configurations, setups, and fixtures
"""

from botocore.stub import Stubber
from mypy_boto3_glue.client import GlueClient
from pyspark.sql import SparkSession
from pyspark.sql.session import SparkSession as SparkSessionType
from typing import Iterator, Tuple
import boto3
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


@pytest.fixture(name="glue_client_stubber")
def fixture_glue_client_stubber() -> Iterator[Tuple[GlueClient, Stubber]]:
    """
    Generate stubber for Glue client
    """

    glue_client = boto3.client("glue", region_name="test")

    with Stubber(glue_client) as stubber:
        yield (glue_client, stubber)
        stubber.assert_no_pending_responses()
