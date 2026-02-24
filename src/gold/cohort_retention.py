import pandas as pd

from src.common.logger import get_logger
from src.common.paths import GOLD_DIR, SILVER_DIR

logger = get_logger("gold_retention")


def week_start_monday(ts: pd.Series) -> pd.Series:
    
    dt = pd.to_datetime(ts, errors="coerce")
    return (dt - pd.to_timedelta(dt.dt.weekday, unit="D")).dt.normalize()


def main():
    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    events_path = SILVER_DIR / "events_clean.parquet"
    df = pd.read_parquet(events_path)
    logger.info(f"Read events: path={events_path} rows={len(df)}")

    # Basic cleaning
    df["event_type"] = df["event_type"].astype(str).str.strip().str.lower()
    df["user_id"] = df["user_id"].astype(str).str.strip()
    df = df[~df["user_id"].str.lower().isin(["", "none", "nan"])].copy()

    # finding signup timestamp per user 
    signups = df[df["event_type"] == "signup"].copy()
    signups = signups.dropna(subset=["event_ts_utc"])
    signup_per_user = (
        signups.groupby("user_id", as_index=False)["event_ts_utc"]
        .min()
        .rename(columns={"event_ts_utc": "signup_ts"})
    )
    signup_per_user["cohort_week"] = week_start_monday(signup_per_user["signup_ts"])

    cohort_sizes = (
        signup_per_user.groupby("cohort_week", as_index=False)["user_id"]
        .nunique()
        .rename(columns={"user_id": "cohort_size"})
    )

    logger.info(f"Signup users: {len(signup_per_user)} cohorts: {len(cohort_sizes)}")

    # "active events" for retention means the following!
    active_types = {"login", "page_view", "purchase", "trial_start", "trial_convert"}

    activity = df[df["event_type"].isin(active_types)].copy()
    activity = activity.dropna(subset=["event_ts_utc"])
    activity["activity_week"] = week_start_monday(activity["event_ts_utc"])

    activity = activity.merge(signup_per_user[["user_id", "cohort_week"]], on="user_id", how="inner")

    activity["week_index"] = ((activity["activity_week"] - activity["cohort_week"]).dt.days // 7).astype(int)

    activity = activity[activity["week_index"] >= 0].copy()

    active_counts = (
        activity.groupby(["cohort_week", "week_index"], as_index=False)["user_id"]
        .nunique()
        .rename(columns={"user_id": "active_users"})
        .sort_values(["cohort_week", "week_index"])
    )

    out = active_counts.merge(cohort_sizes, on="cohort_week", how="left")
    out["retention_rate"] = out["active_users"] / out["cohort_size"]

    out_path = GOLD_DIR / "weekly_cohort_retention.parquet"
    out.to_parquet(out_path, index=False)
    logger.info(f"Wrote: {out_path} rows={len(out)}")


if __name__ == "__main__":
    main()