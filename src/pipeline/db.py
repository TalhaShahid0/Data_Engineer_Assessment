"""Configuration and PostgreSQL helpers shared by every pipeline step."""
from pathlib import Path

import psycopg2
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"


def load_config(path=CONFIG_PATH):
    """Read config/config.yaml and return it as a dict."""
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_connection(config, dbname=None):
    """Open a connection to PostgreSQL.

    The password is never passed in code: libpq reads it from the
    PGPASSWORD environment variable (or a pgpass file).
    autocommit is off, so every step runs inside a transaction that the
    caller commits or rolls back.
    """
    db = config["database"]
    return psycopg2.connect(
        host=db["host"],
        port=db["port"],
        dbname=dbname or db["dbname"],
        user=db["user"],
    )


def run_sql_file(conn, path, params=None):
    """Execute every statement in a .sql file on an open connection.

    When params is given, the file may use %(name)s placeholders; any
    literal percent sign in that file must then be written as %%.
    """
    sql = Path(path).read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(sql, params)


def fetch_all(conn, query, params=None):
    """Run a query and return all rows as a list of tuples."""
    with conn.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchall()
