import json
import pandas as pd

from src.paths import EVENTS_NDJSON, BRONZE_DIR, QUARANTINE_DIR


def main():
    # 1) make sure events are exist
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)

    good_rows = []   # good and without issue events
    bad_rows = []    # with issue events

    # 2) read ndjson line by line
    with open(EVENTS_NDJSON, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if line == "":
                continue

            try:
                event = json.loads(line)   # convert to text
                event["_source_line_no"] = line_no
                good_rows.append(event)

            except Exception as e:
                bad_rows.append({
                    "_source_line_no": line_no,
                    "_raw_line": line[:3000],
                    "_error": str(e)
                })

    # 3) convert List to DataFrame
    good_df = pd.DataFrame(good_rows)
    bad_df = pd.DataFrame(bad_rows)

    # Bronze trick: make everything string so Parquet doesn't fail on mixed types
    good_df = good_df.astype(str)

    # 4) save as Parquet
    good_df.to_parquet(BRONZE_DIR / "events_raw.parquet", index=False)
    bad_df.to_parquet(QUARANTINE_DIR / "events_bad_rows.parquet", index=False)

    print(" Bronze events saved:", len(good_df))
    print(" Bad rows saved:", len(bad_df))


if __name__ == "__main__":
    main()