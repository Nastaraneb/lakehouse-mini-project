import pandas as pd

from src.common.logger import get_logger
from src.common.paths import GOLD_DIR, SILVER_DIR

logger = get_logger("gold_ltv_cac")


def main():
    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    # total LTV from ltv_per_user
    ltv_df = pd.read_parquet(GOLD_DIR / "ltv_per_user.parquet")
    ltv_df["ltv"] = pd.to_numeric(ltv_df["ltv"], errors="coerce").fillna(0)
    total_ltv = float(ltv_df["ltv"].sum())

    # total spend from marketing
    mkt = pd.read_parquet(SILVER_DIR / "marketing_spend_clean.parquet")
    mkt["spend"] = pd.to_numeric(mkt["spend"], errors="coerce").fillna(0)
    
    total_spend = float(mkt["spend"].sum())

    # paid conversions = unique users with at least one purchase
    events = pd.read_parquet(SILVER_DIR / "events_clean.parquet")
    events["event_type"] = events["event_type"].astype(str).str.strip().str.lower()
    events["user_id"] = events["user_id"].astype(str).str.strip()
    events = events[~events["user_id"].str.lower().isin(["", "none", "nan"])].copy()

    purchases = events[events["event_type"] == "purchase"]
    paid_conversions = int(purchases["user_id"].nunique())

    # CAC overall
    cac_overall = (total_spend / paid_conversions) if paid_conversions > 0 else None

    # ratio
    ratio = (total_ltv / cac_overall) if (cac_overall is not None and cac_overall != 0) else None

    out = pd.DataFrame(
        [{
            "total_ltv": total_ltv,
            "total_spend": total_spend,
            "paid_conversions": paid_conversions,
            "cac_overall": cac_overall,
            "ltv_cac_ratio": ratio,
        }]
    )

    out_path = GOLD_DIR / "ltv_cac_ratio.parquet"
    out.to_parquet(out_path, index=False)

    logger.info(
        f"Wrote: {out_path} "
        f"total_ltv={total_ltv:.2f} total_spend={total_spend:.2f} "
        f"paid_conversions={paid_conversions} cac_overall={cac_overall} ratio={ratio}"
    )


if __name__ == "__main__":
    main()