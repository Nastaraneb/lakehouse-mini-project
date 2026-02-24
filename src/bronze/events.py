import json
import pandas as pd
from src.common.logger import get_logger
logger = get_logger("bronze_events")

from src.common.paths import EVENTS_NDJSON, BRONZE_DIR, QUARANTINE_DIR


def main():
    
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)

    good_rows = []  
    bad_rows = []    

    
    with open(EVENTS_NDJSON, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if line == "":
                continue

            try:
                event = json.loads(line)  
                event["_source_line_no"] = line_no
                good_rows.append(event)

            except Exception as e:
                bad_rows.append({
                    "_source_line_no": line_no,
                    "_raw_line": line[:3000],
                    "_error": str(e)
                })

    good_df = pd.DataFrame(good_rows)
    bad_df = pd.DataFrame(bad_rows)


    good_df = good_df.astype(str)


    good_df.to_parquet(BRONZE_DIR / "events_raw.parquet", index=False)
    bad_df.to_parquet(QUARANTINE_DIR / "events_bad_rows.parquet", index=False)

    logger.info(f"Bronze events saved: {len(good_df)}")
    logger.info(f"Bad rows saved: {len(bad_df)}")


if __name__ == "__main__":
    main()