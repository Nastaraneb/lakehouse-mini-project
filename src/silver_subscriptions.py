import pandas as pd

from src.paths import BRONZE_DIR, SILVER_DIR, QUARANTINE_DIR


def main():
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(BRONZE_DIR / "subscriptions_raw.parquet")

    needed = ["subscription_id", "user_id", "plan_id", "price", "currency",
              "start_date", "end_date", "status", "created_at"]
    for c in needed:
        if c not in df.columns:
            df[c] = None

    df["user_id"] = df["user_id"].astype(str).str.strip()
    df["plan_id"] = df["plan_id"].astype(str).str.strip()
    df["status"] = df["status"].astype(str).str.strip().str.lower()

    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce").dt.date
    df["end_date"] = pd.to_datetime(df["end_date"], errors="coerce").dt.date
    df["created_at_ts"] = pd.to_datetime(df["created_at"], errors="coerce")

    bad_mask = (
        df["subscription_id"].isna()
        | (df["subscription_id"].astype(str).str.strip() == "")
        | df["user_id"].isna()
        | (df["user_id"].astype(str).str.strip() == "")
        | df["price"].isna()
        | df["start_date"].isna()
        | df["created_at_ts"].isna()
    )
    bad_df = df[bad_mask].copy()
    good_df = df[~bad_mask].copy()

    good_df = good_df.sort_values(["subscription_id", "created_at_ts"], ascending=[True, False])
    good_df = good_df.drop_duplicates(subset=["subscription_id"], keep="first")

    good_df.to_parquet(SILVER_DIR / "subscriptions_clean.parquet", index=False)
    bad_df.to_parquet(QUARANTINE_DIR / "subscriptions_rejected_silver.parquet", index=False)

    print(" Silver subscriptions saved:", len(good_df))
    print(" Silver subscriptions rejected:", len(bad_df))


if __name__ == "__main__":
    main()