import os
import duckdb
from src.common.paths import PROJECT_ROOT

def get_conn():
    db_path = os.getenv("DUCKDB_PATH", "./lakehouse/gold.duckdb")
    full = (PROJECT_ROOT / db_path).resolve()
    return duckdb.connect(str(full))