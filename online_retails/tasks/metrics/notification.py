from prefect import task, get_run_logger
from utilx import push_to_prometheus
import pandas as pd
import utilx.clickhouse_client as ch

@task
def anomaly_unit_price_count():
    """Count of anomalies in unit price"""
    logger = get_run_logger()
    logger.info(f"Counting anomaly_unit_price_count...")

    client = ch.get_client()
    metrics_df : pd.DataFrame = client.query_df("""
        SELECT DISTINCT anomaly_unit_price_count FROM quality.data_quality order by check_date desc limit 1
    """)
    
    count = metrics_df.iloc[0]['anomaly_unit_price_count']

    push_to_prometheus.guage('anomaly_unit_price_count', count)

    logger.info(f"anomaly_unit_price_count: {count}")

@task
def anomaly_quantity_count():
    """Count of anomalies in quantity"""
    logger = get_run_logger()
    logger.info(f"Counting anomaly_quantity_count...")

    client = ch.get_client()
    metrics_df : pd.DataFrame = client.query_df("""
        SELECT DISTINCT anomaly_quantity_count FROM quality.data_quality order by check_date desc limit 1
    """)
    
    count = metrics_df.iloc[0]['anomaly_quantity_count']

    push_to_prometheus.guage('anomaly_quantity_count', count)

    logger.info(f"anomaly_quantity_count: {count}")    

@task
def missing_customer_id_ratio():
    """Ratio of missing customer IDs"""
    logger = get_run_logger()
    logger.info(f"Calculating missing_customer_id_ratio...")

    client = ch.get_client()
    metrics_df : pd.DataFrame = client.query_df("""
        SELECT DISTINCT missing_customer_id_ratio FROM quality.data_quality order by check_date desc limit 1
    """)
    
    ratio = metrics_df.iloc[0]['missing_customer_id_ratio']

    push_to_prometheus.guage('missing_customer_id_ratio', ratio)

    logger.info(f"missing_customer_id_ratio: {ratio}")

@task
def latest_min_total_amount():
    """Latest minimum total amount in sales"""
    logger = get_run_logger()
    logger.info(f"Calculating latest_min_total_amount...")

    client = ch.get_client()
    metrics_df : pd.DataFrame = client.query_df("""
        SELECT DISTINCT min_total_amount as min_total_amount FROM quality.sales_summary order by sales_date desc limit 1
    """)
    
    min_total_amount = metrics_df.iloc[0]['min_total_amount']

    push_to_prometheus.guage('latest_min_total_amount', min_total_amount)

    logger.info(f"latest_min_total_amount: {min_total_amount}")

@task
def latest_max_total_amount():
    """Latest maximum total amount in sales"""
    logger = get_run_logger()
    logger.info(f"Calculating latest_max_total_amount...")

    client = ch.get_client()
    metrics_df : pd.DataFrame = client.query_df("""
        SELECT DISTINCT max_total_amount as max_total_amount FROM quality.sales_summary order by sales_date desc limit 1
    """)
    
    max_total_amount = metrics_df.iloc[0]['max_total_amount']

    push_to_prometheus.guage('latest_max_total_amount', max_total_amount)

    logger.info(f"latest_max_total_amount: {max_total_amount}")

@task
def latest_median_total_amount():
    """Latest median total amount in sales"""
    logger = get_run_logger()
    logger.info(f"Calculating latest_median_total_amount...")

    client = ch.get_client()
    metrics_df : pd.DataFrame = client.query_df("""
        SELECT DISTINCT median_total_amount as median_total_amount FROM quality.sales_summary order by sales_date desc limit 1
    """)
    
    median_total_amount = metrics_df.iloc[0]['median_total_amount']

    push_to_prometheus.guage('latest_median_total_amount', median_total_amount)

    logger.info(f"latest_median_total_amount: {median_total_amount}")

@task
def latest_avg_total_amount():
    """Latest average total amount in sales"""
    logger = get_run_logger()
    logger.info(f"Calculating latest_avg_total_amount...")

    client = ch.get_client()
    metrics_df : pd.DataFrame = client.query_df("""
        SELECT DISTINCT avg_total_amount as avg_total_amount FROM quality.sales_summary order by sales_date desc limit 1
    """)
    
    avg_total_amount = metrics_df.iloc[0]['avg_total_amount']

    push_to_prometheus.guage('latest_avg_total_amount', avg_total_amount)

    logger.info(f"latest_avg_total_amount: {avg_total_amount}")

@task
def latest_sales_volume():
    """Latest sales volume"""
    logger = get_run_logger()
    logger.info(f"Calculating latest_sales_volume...")

    client = ch.get_client()
    metrics_df : pd.DataFrame = client.query_df("""
        SELECT DISTINCT volume as volume FROM quality.sales_summary order by sales_date desc limit 1
    """)
    
    volume = metrics_df.iloc[0]['volume']

    push_to_prometheus.guage('latest_sales_volume', volume)

    logger.info(f"latest_sales_volume: {volume}")
