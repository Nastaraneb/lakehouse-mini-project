import pandas as pd

from src.paths import MARKETING_CSV, BRONZE_DIR


def main():
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(MARKETING_CSV)

    # Bronze: keep raw-ish, convert to string to avoid parquet type issues
    df = df.astype(str)

    out_path = BRONZE_DIR / "marketing_spend_raw.parquet"
    df.to_parquet(out_path, index=False)

    print(" Bronze marketing saved:", len(df))


if __name__ == "__main__":
    main()