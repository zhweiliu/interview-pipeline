from prefect import task, get_run_logger
import utilx.clickhouse_client as ch

@task
def dim_product():
    """從 Data Vault 載入所有 product 維度"""
    logger = get_run_logger()
    logger.info(f"Building dim_product...")

    client = ch.get_client()
    client.command("TRUNCATE TABLE marts.Dim_Product")
    client.command("""
    INSERT INTO marts.Dim_Product
    with
        hub_product as (select hub_product_hash_key, StockCode from vault.hub_product),
        sat_product as (select hub_product_hash_key, Description from vault.sat_product)
    SELECT
       p.hub_product_hash_key as product_key,
       p.StockCode as stock_code,
       s.Description as description
    FROM hub_product p
    JOIN sat_product s ON p.hub_product_hash_key = s.hub_product_hash_key
    """)

    logger.info(f"dim_product builded.")

@task
def dim_customer():
    """從 Data Vault 載入所有 customer 維度"""
    logger = get_run_logger()
    logger.info(f"Building dim_customer...")

    client = ch.get_client()
    client.command("TRUNCATE TABLE marts.Dim_Customer")
    client.command("""
    INSERT INTO marts.Dim_Customer
    with
        hub_customer as (select hub_customer_hash_key, CustomerID from vault.hub_customer where CustomerID <> 0),
        link_customer_country as (select hub_customer_hash_key, hub_country_hash_key from vault.link_customer_country)
    SELECT
         c.hub_customer_hash_key as customer_key,
         c.CustomerID as customer_id,
         lcc.hub_country_hash_key as country_key
    FROM hub_customer c
    LEFT JOIN link_customer_country lcc ON c.hub_customer_hash_key = lcc.hub_customer_hash_key
    """)

    logger.info(f"dim_customer builded.")

@task
def dim_time():
    """從 Data Vault 載入所有 time 維度"""
    logger = get_run_logger()
    logger.info(f"Building dim_time...")

    client = ch.get_client()
    client.command("TRUNCATE TABLE marts.Dim_Time")
    client.command("""
    INSERT INTO marts.Dim_Time
    with
        hub_time as (select hub_time_hash_key from vault.hub_time),
        sat_time as (select hub_time_hash_key, date, year, month, day_of_week from vault.sat_time)
    SELECT
        t.hub_time_hash_key as time_key,
        s.date as date,
        s.year as year,
        s.month as month,
        s.day_of_week as day_of_week
    FROM hub_time t
    JOIN sat_time s ON t.hub_time_hash_key = s.hub_time_hash_key
    """)

    logger.info(f"dim_time builded.")

@task
def dim_country():
    """從 Data Vault 載入所有 country 維度"""
    logger = get_run_logger()
    logger.info(f"Building dim_country...")

    client = ch.get_client()
    client.command("TRUNCATE TABLE marts.Dim_Country")
    client.command("""
    INSERT INTO marts.Dim_Country
    with
        hub_country as (select hub_country_hash_key, Country from vault.hub_country)
    SELECT
        c.hub_country_hash_key as country_key,
        c.Country as country_name
    FROM hub_country c
    """)

    logger.info(f"dim_country builded.")
