import pandas as pd

from src.common.logger import get_logger
from src.common.paths import GOLD_DIR, SILVER_DIR

logger = get_logger("gold_cac")


def main():
    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    events = pd.read_parquet(SILVER_DIR / "events_clean.parquet")
    mkt = pd.read_parquet(SILVER_DIR / "marketing_spend_clean.parquet")

    logger.info(f"Read events rows={len(events)}")
    logger.info(f"Read marketing rows={len(mkt)}")

    # normalize
    events["event_type"] = events["event_type"].astype(str).str.strip().str.lower()
    events["user_id"] = events["user_id"].astype(str).str.strip()
    events["acquisition_channel"] = events["acquisition_channel"].astype(str).str.strip()

    # user -> channel from earliest signup
    signups = events[events["event_type"] == "signup"].copy()
    signups = signups[~signups["user_id"].str.lower().isin(["", "none", "nan"])]
    signups = signups.dropna(subset=["event_ts_utc"])

    signup_first = (
        signups.sort_values("event_ts_utc")
        .groupby("user_id", as_index=False)
        .first()[["user_id", "acquisition_channel"]]
    )

    # clean channel names
    signup_first["acquisition_channel"] = signup_first["acquisition_channel"].replace(
        {"": "Unknown", "none": "Unknown", "nan": "Unknown"}
    )

    # paid converters = users with at least one purchase
    purchases = events[events["event_type"] == "purchase"].copy()
    purchases = purchases[~purchases["user_id"].str.lower().isin(["", "none", "nan"])]

    converters = purchases[["user_id"]].drop_duplicates()
    converters = converters.merge(signup_first, on="user_id", how="left")
    converters["acquisition_channel"] = converters["acquisition_channel"].fillna("Unknown")

    conversions_by_channel = (
        converters.groupby("acquisition_channel", as_index=False)["user_id"]
        .nunique()
        .rename(columns={"user_id": "paid_conversions"})
        .rename(columns={"acquisition_channel": "channel"})
    )

    # marketing spend total by channel
    mkt["channel"] = mkt["channel"].astype(str).str.strip()
    mkt["spend"] = pd.to_numeric(mkt["spend"], errors="coerce").fillna(0)

    spend_by_channel = (
        mkt.groupby("channel", as_index=False)["spend"]
        .sum()
        .rename(columns={"spend": "total_spend"})
    )

    # CAC = spend / conversions
    out = spend_by_channel.merge(conversions_by_channel, on="channel", how="left")
    out["paid_conversions"] = out["paid_conversions"].fillna(0).astype(int)
    out["cac"] = out.apply(
        lambda r: (r["total_spend"] / r["paid_conversions"]) if r["paid_conversions"] > 0 else None,
        axis=1,
    )

    out = out.sort_values("total_spend", ascending=False)

    out_path = GOLD_DIR / "cac_by_channel.parquet"
    out.to_parquet(out_path, index=False)
    logger.info(f"Wrote: {out_path} rows={len(out)}")


if __name__ == "__main__":
    main()