from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_DIR = Path(os.getenv("DATA_DIR", str(PROJECT_ROOT / "data"))).resolve()
LAKEHOUSE_DIR = Path(os.getenv("LAKEHOUSE_DIR", str(PROJECT_ROOT / "lakehouse"))).resolve()

BRONZE_DIR = LAKEHOUSE_DIR / "bronze"
SILVER_DIR = LAKEHOUSE_DIR / "silver"
GOLD_DIR = LAKEHOUSE_DIR / "gold"
QUARANTINE_DIR = LAKEHOUSE_DIR / "quarantine"

EVENTS_NDJSON = DATA_DIR / "events.ndjson"
SUBSCRIPTIONS_JSON = DATA_DIR / "subscriptions.json"
MARKETING_CSV = DATA_DIR / "marketing_spend.csv"