"""
Test setup for the consumer. Run from the Project 2 venv (it has Spark, Delta and lusitania):

    cd C:\\Projects\\LusitaniaPipelines
    pytest tests

One Delta-enabled Spark session for all tests (Project 2's get_spark). DELTA_JARS can point to
local jar files instead, for machines where Spark cannot download them (comma-separated paths).
"""
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "streaming"))   # `import consumer`
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)


@pytest.fixture(scope="session")
def spark():
    jars = os.environ.get("DELTA_JARS")
    if jars:
        from pyspark.sql import SparkSession
        session = (SparkSession.builder.master("local[2]").appName("tests")
                   .config("spark.jars", jars)
                   .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                   .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                   .config("spark.sql.session.timeZone", "UTC")
                   .config("spark.ui.enabled", "false").getOrCreate())
    else:
        from lusitania.spark import get_spark
        session = get_spark("tests", cores="2")
    session.conf.set("spark.sql.shuffle.partitions", "2")
    yield session
    session.stop()
