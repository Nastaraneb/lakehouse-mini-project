import pandas as pd

from src.common.logger import get_logger
from src.common.paths import GOLD_DIR, SILVER_DIR

logger = get_logger("gold_ltv")


def main():
    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    events_path = SILVER_DIR / "events_clean.parquet"
    df = pd.read_parquet(events_path)
    logger.info(f"Read events: path={events_path} rows={len(df)}")

    df["event_type"] = df["event_type"].astype(str).str.strip().str.lower()
    df["user_id"] = df["user_id"].astype(str).str.strip()
    df = df[~df["user_id"].str.lower().isin(["", "none", "nan"])].copy()

    # Keep only purchase/refund
    df = df[df["event_type"].isin(["purchase", "refund"])].copy()

    df["amount_num"] = pd.to_numeric(df["amount_num"], errors="coerce").fillna(0)

    # purchase = +amount, refund = -abs(amount)
    df["signed_amount"] = df.apply(
        lambda r: r["amount_num"] if r["event_type"] == "purchase" else -abs(r["amount_num"]),
        axis=1,
    )

    ltv = (
        df.groupby("user_id", as_index=False)["signed_amount"]
        .sum()
        .rename(columns={"signed_amount": "ltv"})
        .sort_values("ltv", ascending=False)
    )

    out_path = GOLD_DIR / "ltv_per_user.parquet"
    ltv.to_parquet(out_path, index=False)
    logger.info(f"Wrote: {out_path} rows={len(ltv)}")


if __name__ == "__main__":
    main()