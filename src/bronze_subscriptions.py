import json
import pandas as pd

from src.paths import SUBSCRIPTIONS_JSON, BRONZE_DIR


def main():
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    # subscriptions.json is a JSON array
    with open(SUBSCRIPTIONS_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)

    df = pd.DataFrame(data)

    # Bronze: keep raw-ish, convert to string to avoid parquet type issues
    df = df.astype(str)

    out_path = BRONZE_DIR / "subscriptions_raw.parquet"
    df.to_parquet(out_path, index=False)

    print(" Bronze subscriptions saved:", len(df))


if __name__ == "__main__":
    main()