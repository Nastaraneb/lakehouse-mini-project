import time
from datetime import datetime, timezone

from src.common.logger import get_logger, start_run

from src.bronze import events as bronze_events
from src.bronze import subscriptions as bronze_subscriptions
from src.bronze import marketing as bronze_marketing

from src.silver import events as silver_events
from src.silver import marketing as silver_marketing
from src.silver import subscriptions as silver_subscriptions

from src.gold import metrics_basic as gold_basic
from src.gold import mrr as gold_mrr
from src.gold import cohort_retention as gold_retention
from src.gold import cac as gold_cac
from src.gold import ltv as gold_ltv
from src.gold import ltv_cac_ratio as gold_ratio
from src.gold import load_to_duckdb as gold_load_duckdb
from src.gold import build_dims as gold_build_dims

logger = get_logger("main_pipeline")


def _run_step(step_name: str, fn):
    t0 = time.time()
    logger.info(f"START step={step_name}")
    try:
        fn()
        dt = round(time.time() - t0, 3)
        logger.info(f"END step={step_name} duration_s={dt}")
    except Exception as e:
        dt = round(time.time() - t0, 3)
        logger.exception(f"FAILED step={step_name} duration_s={dt} error={e}")
        raise


class MainPipeline:
    def run_bronze(self):
        logger.info("START stage=bronze")
        _run_step("bronze_events", bronze_events.main)
        _run_step("bronze_subscriptions", bronze_subscriptions.main)
        _run_step("bronze_marketing", bronze_marketing.main)
        logger.info("END stage=bronze")

    def run_silver(self):
        logger.info("START stage=silver")
        _run_step("silver_events", silver_events.main)
        _run_step("silver_marketing", silver_marketing.main)
        _run_step("silver_subscriptions", silver_subscriptions.main)
        logger.info("END stage=silver")

    def run_gold(self):
        logger.info("START stage=gold")
        _run_step("gold_metrics_basic", gold_basic.main)
        _run_step("gold_mrr", gold_mrr.main)
        _run_step("gold_cohort_retention", gold_retention.main)
        _run_step("gold_cac", gold_cac.main)
        _run_step("gold_ltv", gold_ltv.main)
        _run_step("gold_ltv_cac_ratio", gold_ratio.main)
        _run_step("gold_load_to_duckdb", gold_load_duckdb.main)
        _run_step("gold_build_dims", gold_build_dims.main)
        logger.info("END stage=gold")

    def run_all(self):
        logger.info("START pipeline")
        self.run_bronze()
        self.run_silver()
        self.run_gold() 
        logger.info("END pipeline")


def main():
    run_window = datetime.now(timezone.utc).strftime("%Y-%m-%d")  
    run_info = start_run(run_window=run_window, pipeline_name="lakehouse-mini-project")
    logger.info(f"RUN START {run_info}")
    MainPipeline().run_all()

    logger.info(f"RUN END {run_info}")


if __name__ == "__main__":
    main()