import pandas as pd
from src.common.logger import get_logger
logger = get_logger("silver_marketing")

from src.common.paths import BRONZE_DIR, SILVER_DIR, QUARANTINE_DIR


def main():
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(BRONZE_DIR / "marketing_spend_raw.parquet")


    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["channel"] = df["channel"].astype(str).str.strip()
    df["spend_num"] = pd.to_numeric(df["spend"], errors="coerce")

    
    bad_mask = df["date"].isna() | df["spend_num"].isna() | (df["spend_num"] < 0) # One negative spend row is fixed!
    bad_df = df[bad_mask].copy()
    good_df = df[~bad_mask].copy()


    daily = (
        good_df.groupby(["date", "channel"], as_index=False)["spend_num"]
        .sum()
        .rename(columns={"spend_num": "spend"})
    )

    # fill missing days with 0 for each channel
    channels = sorted(daily["channel"].unique().tolist())
    min_day = daily["date"].min()
    max_day = daily["date"].max()

    all_days = pd.date_range(min_day, max_day, freq="D")
    grid = pd.MultiIndex.from_product([all_days, channels], names=["date", "channel"]).to_frame(index=False)

    daily_full = grid.merge(daily, on=["date", "channel"], how="left")
    daily_full["spend"] = daily_full["spend"].fillna(0.0)

    # write
    daily_full.to_parquet(SILVER_DIR / "marketing_spend_clean.parquet", index=False)
    bad_df.to_parquet(QUARANTINE_DIR / "marketing_spend_rejected_silver.parquet", index=False)

    logger.info(f"Silver marketing saved: {len(daily_full)}")
    logger.info(f"Silver marketing rejected: {len(bad_df)}")

if __name__ == "__main__":
    main()