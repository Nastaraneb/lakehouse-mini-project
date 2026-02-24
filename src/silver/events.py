import pandas as pd
from dateutil import parser, tz
from src.common.logger import get_logger
logger = get_logger("silver_events")

from src.common.paths import BRONZE_DIR, SILVER_DIR, QUARANTINE_DIR


def parse_to_utc(ts_text):
    if ts_text is None:
        return None

    s = str(ts_text).strip()
    if s == "" or s.lower() in ["nan", "none"]:
        return None

    try:
        dt = parser.parse(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=tz.UTC)
        dt_utc = dt.astimezone(tz.UTC)
        return dt_utc.replace(tzinfo=None)
    except Exception:
        return None


def main():
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(BRONZE_DIR / "events_raw.parquet")

    needed = ["event_id", "user_id", "event_type", "timestamp", "schema_version",
              "amount", "currency", "tax", "refers_to_event_id", "_source_line_no"]
    for c in needed:
        if c not in df.columns:
            df[c] = None

    df["event_ts_utc"] = df["timestamp"].apply(parse_to_utc)
    df["amount_num"] = pd.to_numeric(df["amount"], errors="coerce")

    bad_mask = (
        df["event_id"].isna()
        | (df["event_id"].astype(str).str.strip() == "")
        | df["event_ts_utc"].isna()
    )

    bad_df = df[bad_mask].copy()
    good_df = df[~bad_mask].copy()

    good_df["_source_line_no"] = pd.to_numeric(good_df["_source_line_no"], errors="coerce").fillna(0)
    good_df = good_df.sort_values(
        ["event_id", "event_ts_utc", "_source_line_no"],
        ascending=[True, False, False],
    )
    good_df = good_df.drop_duplicates(subset=["event_id"], keep="first") #event_id (string) — supposed to be unique but is not always. so I fixed it!

    good_df.to_parquet(SILVER_DIR / "events_clean.parquet", index=False)
    bad_df.to_parquet(QUARANTINE_DIR / "events_rejected_silver.parquet", index=False)

    logger.info(f"Silver events saved: {len(good_df)}")
    logger.info(f"Silver rejected saved: {len(bad_df)}")


if __name__ == "__main__":
    main()