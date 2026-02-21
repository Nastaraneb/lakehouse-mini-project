from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

DATA_DIR = PROJECT_ROOT / "data"

LAKEHOUSE_DIR = PROJECT_ROOT / "lakehouse"
BRONZE_DIR = LAKEHOUSE_DIR / "bronze"
SILVER_DIR = LAKEHOUSE_DIR / "silver"
GOLD_DIR = LAKEHOUSE_DIR / "gold"
QUARANTINE_DIR = LAKEHOUSE_DIR / "quarantine"

EVENTS_NDJSON = DATA_DIR / "events.ndjson"
SUBSCRIPTIONS_JSON = DATA_DIR / "subscriptions.json"
MARKETING_CSV = DATA_DIR / "marketing_spend.csv"