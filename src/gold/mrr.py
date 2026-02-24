import pandas as pd

from src.common.logger import get_logger
from src.common.paths import GOLD_DIR, SILVER_DIR

logger = get_logger("gold_mrr")


def main():
    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    subs_path = SILVER_DIR / "subscriptions_clean.parquet"
    subs = pd.read_parquet(subs_path)
    logger.info(f"Read subscriptions: path={subs_path} rows={len(subs)}")

    
    subs["start_date"] = pd.to_datetime(subs["start_date"]).dt.date
    subs["end_date"] = pd.to_datetime(subs["end_date"], errors="coerce").dt.date

    
    min_day = subs["start_date"].min()
    max_end = subs["end_date"].dropna().max()
    max_day = max_end if pd.notna(max_end) else min_day

    max_day = max(max_day, subs["start_date"].max())

    days = pd.date_range(min_day, max_day, freq="D").date
    out_rows = []

    
    for d in days:
        active = subs[(subs["start_date"] <= d) & ((subs["end_date"].isna()) | (subs["end_date"] >= d))]
        mrr = active["price"].sum()
        out_rows.append({"date": d, "mrr": float(mrr)})

    mrr_daily = pd.DataFrame(out_rows).sort_values("date")

    out_path = GOLD_DIR / "mrr_daily.parquet"
    mrr_daily.to_parquet(out_path, index=False)
    logger.info(f"Wrote: {out_path} rows={len(mrr_daily)}")


if __name__ == "__main__":
    main()