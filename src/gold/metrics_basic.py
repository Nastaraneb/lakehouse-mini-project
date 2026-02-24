import pandas as pd

from src.common.logger import get_logger
from src.common.paths import GOLD_DIR, SILVER_DIR

logger = get_logger("gold_basic")


def _clean_user_id(s: pd.Series) -> pd.Series:
    
    s = s.astype(str).str.strip()
    return s[~s.str.lower().isin(["", "none", "nan"])]


def daily_active_users(events: pd.DataFrame) -> pd.DataFrame:
    df = events.copy()

    
    df["event_date"] = pd.to_datetime(df["event_ts_utc"]).dt.date
    df["event_type"] = df["event_type"].astype(str).str.strip().str.lower()

    # "active user" means the following!
    active_types = {"login", "page_view", "purchase"}

    df = df[df["event_type"].isin(active_types)].copy()
    df = df[df["user_id"].notna()].copy()
    df["user_id"] = df["user_id"].astype(str).str.strip()
    df = df[~df["user_id"].str.lower().isin(["", "none", "nan"])]

    out = (
        df.groupby("event_date", as_index=False)["user_id"]
        .nunique()
        .rename(columns={"user_id": "daily_active_users"})
        .sort_values("event_date")
    )
    return out


def daily_revenue_gross(events: pd.DataFrame) -> pd.DataFrame:
    df = events.copy()
    df["event_date"] = pd.to_datetime(df["event_ts_utc"]).dt.date
    df["event_type"] = df["event_type"].astype(str).str.strip().str.lower()

    purchases = df[df["event_type"] == "purchase"].copy()

    
    purchases["amount_num"] = pd.to_numeric(purchases["amount_num"], errors="coerce").fillna(0)

    out = (
        purchases.groupby("event_date", as_index=False)["amount_num"]
        .sum()
        .rename(columns={"amount_num": "revenue_gross"})
        .sort_values("event_date")
    )
    return out


def daily_revenue_net(events: pd.DataFrame) -> pd.DataFrame:
    df = events.copy()
    df["event_date"] = pd.to_datetime(df["event_ts_utc"]).dt.date
    df["event_type"] = df["event_type"].astype(str).str.strip().str.lower()

    df = df[df["event_type"].isin(["purchase", "refund"])].copy()
    df["amount_num"] = pd.to_numeric(df["amount_num"], errors="coerce").fillna(0)

    # purchase = +amount, refund = -abs(amount)
    df["signed_amount"] = df.apply(
        lambda r: r["amount_num"] if r["event_type"] == "purchase" else -abs(r["amount_num"]),
        axis=1,
    )

    out = (
        df.groupby("event_date", as_index=False)["signed_amount"]
        .sum()
        .rename(columns={"signed_amount": "revenue_net"})
        .sort_values("event_date")
    )
    return out


def main():
    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    events_path = SILVER_DIR / "events_clean.parquet"
    events = pd.read_parquet(events_path)
    logger.info(f"Read events: path={events_path} rows={len(events)}")

    dau = daily_active_users(events)
    gross = daily_revenue_gross(events)
    net = daily_revenue_net(events)

    dau_path = GOLD_DIR / "daily_active_users.parquet"
    gross_path = GOLD_DIR / "daily_revenue_gross.parquet"
    net_path = GOLD_DIR / "daily_revenue_net.parquet"

    dau.to_parquet(dau_path, index=False)
    gross.to_parquet(gross_path, index=False)
    net.to_parquet(net_path, index=False)

    logger.info(f"Wrote: {dau_path} rows={len(dau)}")
    logger.info(f"Wrote: {gross_path} rows={len(gross)}")
    logger.info(f"Wrote: {net_path} rows={len(net)}")


if __name__ == "__main__":
    main()