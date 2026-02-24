from src.common.db import get_conn
from src.common.logger import get_logger

logger = get_logger("gold_views")


def main():
    con = get_conn()
    con.execute("CREATE SCHEMA IF NOT EXISTS analytics;")

    con.execute("DROP VIEW IF EXISTS analytics.vw_daily_kpis;")
    con.execute(
        """
        CREATE VIEW analytics.vw_daily_kpis AS
        SELECT
            d.date,
            d.year,
            d.month,
            d.week_start_monday,
            dau.daily_active_users,
            rg.revenue_gross,
            rn.revenue_net
        FROM analytics.fact_daily_active_users dau
        JOIN analytics.dim_date d
          ON d.date = CAST(dau.event_date AS DATE)
        LEFT JOIN analytics.fact_daily_revenue_gross rg
          ON rg.event_date = dau.event_date
        LEFT JOIN analytics.fact_daily_revenue_net rn
          ON rn.event_date = dau.event_date;
        """
    )

    con.execute("DROP VIEW IF EXISTS analytics.vw_cac_by_channel;")
    con.execute(
        """
        CREATE VIEW analytics.vw_cac_by_channel AS
        SELECT
            c.channel,
            f.paid_conversions,
            f.total_spend,
            f.cac
        FROM analytics.fact_cac_by_channel f
        JOIN analytics.dim_channel c
          ON c.channel = f.channel;
        """
    )
    con.execute("DROP VIEW IF EXISTS analytics.vw_mrr_daily;")
    con.execute(
        """
        CREATE VIEW analytics.vw_mrr_daily AS
        SELECT
            d.date,
            d.year,
            d.month,
            m.mrr
        FROM analytics.fact_mrr_daily m
        JOIN analytics.dim_date d
          ON d.date = CAST(m.date AS DATE);
        """
    )

    con.execute("DROP VIEW IF EXISTS analytics.vw_user_ltv;")
    con.execute(
        """
        CREATE VIEW analytics.vw_user_ltv AS
        SELECT
            u.user_id,
            u.acquisition_channel,
            ltv.ltv
        FROM analytics.fact_ltv_per_user ltv
        JOIN analytics.dim_user u
          ON u.user_id = ltv.user_id;
        """
    )

    con.execute("DROP VIEW IF EXISTS analytics.vw_weekly_retention;")
    con.execute(
        """
        CREATE VIEW analytics.vw_weekly_retention AS
        SELECT
            cohort_week,
            week_index,
            cohort_size,
            active_users,
            retention_rate
        FROM analytics.fact_weekly_cohort_retention;
        """
    )
    
    con.execute("DROP VIEW IF EXISTS analytics.vw_ltv_cac_ratio;")
    con.execute(
        """
        CREATE VIEW analytics.vw_ltv_cac_ratio AS
        SELECT * FROM analytics.fact_ltv_cac_ratio;
        """
    )

    con.close()
    logger.info("DONE building analytics views")


if __name__ == "__main__":
    main()