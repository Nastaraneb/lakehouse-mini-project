import pandas as pd

from src.common.logger import get_logger
from src.common.paths import BRONZE_DIR, SILVER_DIR, QUARANTINE_DIR

logger = get_logger("silver_subscriptions")


def _ensure_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA
    return df


def main():
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(BRONZE_DIR / "subscriptions_raw.parquet")

    needed = [
        "subscription_id", "user_id", "plan_id", "price", "currency",
        "start_date", "end_date", "status", "created_at"
    ]
    df = _ensure_cols(df, needed)

    df["subscription_id"] = df["subscription_id"].astype(str).str.strip()
    df["user_id"] = df["user_id"].astype(str).str.strip()
    df["plan_id"] = df["plan_id"].astype(str).str.strip()
    df["currency"] = df["currency"].astype(str).str.strip()
    df["status"] = df["status"].astype(str).str.strip().str.lower()

    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    df["start_dt"] = pd.to_datetime(df["start_date"], errors="coerce").dt.normalize()
    df["end_dt"] = pd.to_datetime(df["end_date"], errors="coerce").dt.normalize()
    df["created_at_ts"] = pd.to_datetime(df["created_at"], errors="coerce")

    bad_mask = (
        df["subscription_id"].isna() | (df["subscription_id"] == "") |
        df["user_id"].isna() | (df["user_id"] == "") |
        df["price"].isna() |
        df["start_dt"].isna() |
        df["created_at_ts"].isna()
    )

    bad_df = df.loc[bad_mask].copy()
    good_df = df.loc[~bad_mask].copy()

    good_df = good_df.sort_values(["subscription_id", "created_at_ts"], ascending=[True, False])
    good_df = good_df.drop_duplicates(subset=["subscription_id"], keep="first")

    tmp = good_df.copy()
    tmp["end_eff"] = tmp["end_dt"].fillna(pd.Timestamp("2100-01-01"))
    tmp = tmp.sort_values(["user_id", "start_dt", "created_at_ts"], ascending=[True, True, False])

    tmp["prev_end_eff"] = tmp.groupby("user_id")["end_eff"].shift(1)
    tmp["is_overlap"] = tmp["prev_end_eff"].notna() & (tmp["start_dt"] <= tmp["prev_end_eff"])

    tmp["gap_days"] = (tmp["start_dt"] - tmp["prev_end_eff"]).dt.days - 1
    tmp["reactivated"] = tmp["gap_days"].fillna(0).gt(0)

    overlap_df = tmp.loc[tmp["is_overlap"]].copy()
    good_df = tmp.loc[~tmp["is_overlap"]].copy()

    overlap_count = len(overlap_df)
    reactivation_count = int(good_df["reactivated"].sum())

    keep_cols = [
        "subscription_id", "user_id", "plan_id", "price", "currency",
        "status", "created_at", "created_at_ts",
        "start_dt", "end_dt", "gap_days", "reactivated"
    ]
    overlap_df = _ensure_cols(overlap_df, keep_cols)[keep_cols].copy()
    good_df = _ensure_cols(good_df, keep_cols)[keep_cols].copy()

    good_df = good_df.rename(columns={"start_dt": "start_date", "end_dt": "end_date"})
    overlap_df = overlap_df.rename(columns={"start_dt": "start_date", "end_dt": "end_date"})

    bad_keep = [
        "subscription_id", "user_id", "plan_id", "price", "currency",
        "start_date", "end_date", "status", "created_at", "created_at_ts",
        "gap_days", "reactivated"
    ]
    bad_df = _ensure_cols(bad_df, bad_keep).copy()
    bad_df["gap_days"] = pd.NA
    bad_df["reactivated"] = pd.NA
    bad_df = bad_df[bad_keep].copy()

    bad_df = pd.concat([bad_df, overlap_df[bad_keep]], ignore_index=True)

    good_df.to_parquet(SILVER_DIR / "subscriptions_clean.parquet", index=False)
    bad_df.to_parquet(QUARANTINE_DIR / "subscriptions_rejected_silver.parquet", index=False)

    logger.info(f"Silver subscriptions overlaps quarantined: {overlap_count}")
    logger.info(f"Silver subscriptions reactivations flagged: {reactivation_count}")
    logger.info(f"Silver subscriptions saved: {len(good_df)}")
    logger.info(f"Silver subscriptions rejected: {len(bad_df)}")


if __name__ == "__main__":
    main()