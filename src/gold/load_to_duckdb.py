from src.common.logger import get_logger
from src.common.db import get_conn
from src.common.paths import GOLD_DIR

logger = get_logger("gold_duckdb_loader")

GOLD_TABLES = [
    ("fact_daily_active_users", "daily_active_users.parquet"),
    ("fact_daily_revenue_gross", "daily_revenue_gross.parquet"),
    ("fact_daily_revenue_net", "daily_revenue_net.parquet"),
    ("fact_mrr_daily", "mrr_daily.parquet"),
    ("fact_weekly_cohort_retention", "weekly_cohort_retention.parquet"),
    ("fact_cac_by_channel", "cac_by_channel.parquet"),
    ("fact_ltv_per_user", "ltv_per_user.parquet"),
    ("fact_ltv_cac_ratio", "ltv_cac_ratio.parquet"),
]


def main():
    con = get_conn()
    con.execute("CREATE SCHEMA IF NOT EXISTS analytics;")

    for table_name, file_name in GOLD_TABLES:
        path = (GOLD_DIR / file_name).as_posix()

        con.execute(f"DROP TABLE IF EXISTS analytics.{table_name};")
        con.execute(
            f"""
            CREATE TABLE analytics.{table_name} AS
            SELECT * FROM read_parquet('{path}');
            """
        )

        cnt = con.execute(f"SELECT COUNT(*) FROM analytics.{table_name};").fetchone()[0]
        logger.info(f"Loaded table=analytics.{table_name} rows={cnt} source={path}")

    con.close()
    logger.info("DONE loading gold parquet -> duckdb")


if __name__ == "__main__":
    main()