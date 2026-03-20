"""Shared pytest fixtures for PySpark tests."""

import os
import sys
import shutil
import tempfile

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


@pytest.fixture(scope="session")
def spark():
    """Session-scoped local SparkSession for all tests."""
    python_path = sys.executable
    os.environ["PYSPARK_PYTHON"] = python_path
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_path
    session = (
        SparkSession.builder
        .master("local[*]")
        .appName("XMLGenerator-Tests")
        .config("spark.sql.warehouse.dir", tempfile.mkdtemp())
        .config("spark.driver.extraJavaOptions", "-Dderby.system.home=" + tempfile.mkdtemp())
        .config("spark.ui.enabled", "false")
        .config("spark.pyspark.python", python_path)
        .config("spark.pyspark.driver.python", python_path)
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def tmp_dir():
    """Per-test temporary directory, cleaned up after test."""
    d = tempfile.mkdtemp()
    yield d
    shutil.rmtree(d, ignore_errors=True)
