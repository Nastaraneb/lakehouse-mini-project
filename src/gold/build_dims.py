import pandas as pd

from src.common.db import get_conn
from src.common.logger import get_logger
from src.common.paths import SILVER_DIR

logger = get_logger("gold_dims")


def main():
    con = get_conn()
    con.execute("CREATE SCHEMA IF NOT EXISTS analytics;")

    events_path = SILVER_DIR / "events_clean.parquet"
    subs_path = SILVER_DIR / "subscriptions_clean.parquet"
    mkt_path = SILVER_DIR / "marketing_spend_clean.parquet"

    events = pd.read_parquet(events_path)
    subs = pd.read_parquet(subs_path)
    mkt = pd.read_parquet(mkt_path)

    logger.info(f"Read events rows={len(events)}")
    logger.info(f"Read subscriptions rows={len(subs)}")
    logger.info(f"Read marketing rows={len(mkt)}")

    dates = set()

    if "event_ts_utc" in events.columns:
        ev_dates = pd.to_datetime(events["event_ts_utc"], errors="coerce").dt.date.dropna().unique()
        dates.update(ev_dates)

    if "date" in mkt.columns:
        mk_dates = pd.to_datetime(mkt["date"], errors="coerce").dt.date.dropna().unique()
        dates.update(mk_dates)

    if "start_date" in subs.columns:
        st = pd.to_datetime(subs["start_date"], errors="coerce").dt.date.dropna().unique()
        dates.update(st)

    if "end_date" in subs.columns:
        en = pd.to_datetime(subs["end_date"], errors="coerce").dt.date.dropna().unique()
        dates.update(en)

    dim_date = pd.DataFrame({"date": sorted(dates)})
    dim_date["year"] = pd.to_datetime(dim_date["date"]).dt.year
    dim_date["month"] = pd.to_datetime(dim_date["date"]).dt.month
    dim_date["day"] = pd.to_datetime(dim_date["date"]).dt.day
    dim_date["weekday"] = pd.to_datetime(dim_date["date"]).dt.weekday
    dim_date["week_start_monday"] = (
        pd.to_datetime(dim_date["date"]) - pd.to_timedelta(pd.to_datetime(dim_date["date"]).dt.weekday, unit="D")
    ).dt.date

    con.execute("DROP TABLE IF EXISTS analytics.dim_date;")
    con.register("dim_date_df", dim_date)
    con.execute("CREATE TABLE analytics.dim_date AS SELECT * FROM dim_date_df;")

    con.execute("DROP TABLE IF EXISTS analytics.dim_channel;")
    con.register("mkt_df", mkt)
    con.execute("CREATE TABLE analytics.dim_channel AS SELECT DISTINCT channel FROM mkt_df ORDER BY channel;")

    con.execute("DROP TABLE IF EXISTS analytics.dim_plan;")
    con.register("subs_df", subs)
    con.execute(
        """
        CREATE TABLE analytics.dim_plan AS
        SELECT DISTINCT plan_id, price, currency
        FROM subs_df
        ORDER BY plan_id;
        """
    )

    con.execute("DROP TABLE IF EXISTS analytics.dim_user;")
    con.register("events_df", events)
    con.execute(
        """
        CREATE TABLE analytics.dim_user AS
        SELECT
            user_id,
            MIN(CASE WHEN event_type='signup' THEN acquisition_channel END) AS acquisition_channel
        FROM events_df
        WHERE user_id IS NOT NULL AND TRIM(CAST(user_id AS VARCHAR)) <> ''
        GROUP BY user_id
        ORDER BY user_id;
        """
    )

    dim_counts = con.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM analytics.dim_date) AS dim_date_rows,
            (SELECT COUNT(*) FROM analytics.dim_channel) AS dim_channel_rows,
            (SELECT COUNT(*) FROM analytics.dim_plan) AS dim_plan_rows,
            (SELECT COUNT(*) FROM analytics.dim_user) AS dim_user_rows
        """
    ).fetchone()

    logger.info(
        f"Built dims: dim_date={dim_counts[0]} dim_channel={dim_counts[1]} dim_plan={dim_counts[2]} dim_user={dim_counts[3]}"
    )

    con.close()


if __name__ == "__main__":
    main()