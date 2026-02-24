from src.common.logger import get_logger

from src.gold.metrics_basic import main as run_basic
from src.gold.mrr import main as run_mrr
from src.gold.cohort_retention import main as run_retention
from src.gold.cac import main as run_cac
from src.gold.ltv import main as run_ltv
from src.gold.ltv_cac_ratio import main as run_ratio

logger = get_logger("gold_runner")


def main():
    logger.info("START gold")

    logger.info("START gold step=metrics_basic")
    run_basic()
    logger.info("END gold step=metrics_basic")

    logger.info("START gold step=mrr")
    run_mrr()
    logger.info("END gold step=mrr")

    logger.info("START gold step=cohort_retention")
    run_retention()
    logger.info("END gold step=cohort_retention")

    logger.info("START gold step=cac")
    run_cac()
    logger.info("END gold step=cac")

    logger.info("START gold step=ltv")
    run_ltv()
    logger.info("END gold step=ltv")

    logger.info("START gold step=ltv_cac_ratio")
    run_ratio()
    logger.info("END gold step=ltv_cac_ratio")

    logger.info("END gold")