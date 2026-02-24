import pandas as pd
from src.common.logger import get_logger
logger = get_logger("bronze_marketing")

from src.common.paths import MARKETING_CSV, BRONZE_DIR


def main():
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(MARKETING_CSV)

    # Bronze: needs raw data but convert to str to avoide parquet issues
    df = df.astype(str)

    out_path = BRONZE_DIR / "marketing_spend_raw.parquet"
    df.to_parquet(out_path, index=False)

    logger.info(f"Bronze marketing saved: {len(df)}")


if __name__ == "__main__":
    main()