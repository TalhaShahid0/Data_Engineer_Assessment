"""Shared test setup.

Tests run the real pipeline against a separate database (jaffle_test) using the
small hand-checked dataset in tests/fixtures/good. Every test starts from an
empty schema (reset=True), so tests are independent of each other.
"""
import copy
import os
import shutil
import sys
import tempfile
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

# Keep test runs out of the project's log file (must be set before src is imported).
os.environ["PIPELINE_LOG_FILE"] = str(Path(tempfile.gettempdir()) / "jaffle_test_pipeline.log")

from src.pipeline.db import get_connection, load_config  # noqa: E402
from src.pipeline.run_pipeline import run_pipeline  # noqa: E402

TEST_DB = "jaffle_test"
GOOD_FIXTURES = PROJECT_ROOT / "tests" / "fixtures" / "good"


@pytest.fixture(scope="session")
def test_config(tmp_path_factory):
    """Project config pointed at the test database and a temporary outputs folder."""
    config = copy.deepcopy(load_config())
    config["database"]["dbname"] = TEST_DB
    config["paths"]["outputs"] = str(tmp_path_factory.mktemp("outputs"))  # absolute path

    admin = get_connection(config, dbname="postgres")
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (TEST_DB,))
        if cur.fetchone() is None:
            cur.execute(f"CREATE DATABASE {TEST_DB}")
    admin.close()
    return config


@pytest.fixture
def db(test_config):
    """Connection to the test database for checking results."""
    conn = get_connection(test_config)
    conn.autocommit = True
    yield conn
    conn.close()


@pytest.fixture
def data_dir(tmp_path):
    """A fresh copy of the good fixture files; tests may add or change files in it."""
    folder = tmp_path / "data"
    shutil.copytree(GOOD_FIXTURES, folder)
    return folder


@pytest.fixture
def run(test_config):
    """Run the pipeline on a data folder. The first run in a test should use reset=True."""
    def _run(folder, reset=False, simulate_failure=None):
        return run_pipeline(test_config, data_dir=folder, reset=reset,
                            simulate_failure=simulate_failure)
    return _run


def query(conn, sql, params=None):
    """Return all rows of a query."""
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def scalar(conn, sql, params=None):
    """Return the single value of a query."""
    return query(conn, sql, params)[0][0]
