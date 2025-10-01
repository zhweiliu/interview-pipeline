from prefect import task, get_run_logger
import utilx.clickhouse_client as ch

@task
def anomaly_customer_invoioces():
    """從 Vault 找出異常的客戶"""
    logger = get_run_logger()
    logger.info(f"mark anomaly_customer_invoioces...")

    client = ch.get_client()
    client.command("TRUNCATE TABLE quality.anomaly_customer_invoioces")
    client.command("""
    INSERT INTO quality.anomaly_customer_invoioces
    WITH
        hub_customer AS (SELECT hub_customer_hash_key, CustomerID FROM vault.hub_customer WHERE CustomerID = 0),
        link_invoice_customer AS (SELECT hub_invoice_hash_key, hub_customer_hash_key FROM vault.link_invoice_customer),
        link_invoice_product AS (SELECT hub_invoice_hash_key, hub_product_hash_key FROM vault.link_invoice_product),
        link_invoice_time AS (SELECT hub_invoice_hash_key, hub_time_hash_key FROM vault.link_invoice_time),
        link_invoice_country AS (SELECT hub_invoice_hash_key, hub_country_hash_key FROM vault.link_invoice_country),
        hub_invoice AS (SELECT hub_invoice_hash_key, InvoiceNo FROM vault.hub_invoice),
        sat_invoice AS (SELECT hub_invoice_hash_key, Quantity, UnitPrice, TotalAmount FROM vault.sat_invoice)
    SELECT
        i.hub_invoice_hash_key AS sale_id,
        i.InvoiceNo as invoice_no,
        p.hub_product_hash_key as product_key,
        c.hub_customer_hash_key as customer_key,
        t.hub_time_hash_key as time_key,
        co.hub_country_hash_key as country_key,       
        s.Quantity as quantity,
        s.UnitPrice as unit_price,
        s.TotalAmount as total_amount
    FROM hub_invoice i
    JOIN sat_invoice s ON i.hub_invoice_hash_key = s.hub_invoice_hash_key
    JOIN link_invoice_customer lic ON i.hub_invoice_hash_key = lic.hub_invoice_hash_key
    JOIN hub_customer c ON lic.hub_customer_hash_key = c.hub_customer_hash_key
    JOIN link_invoice_product p ON i.hub_invoice_hash_key = p.hub_invoice_hash_key
    JOIN link_invoice_time t ON i.hub_invoice_hash_key = t.hub_invoice_hash_key
    JOIN link_invoice_country co ON i.hub_invoice_hash_key = co.hub_invoice_hash_key
    """)

    logger.info(f"anomaly_customer_invoioces marked.")

@task
def anomaly_invoice():
    """從 Vault 找出異常的發票"""
    logger = get_run_logger()
    logger.info(f"mark anomaly_invoice...")

    client = ch.get_client()
    client.command("TRUNCATE TABLE quality.anomaly_invoice")
    client.command("""
    INSERT INTO quality.anomaly_invoice
    WITH
        fact_sales as (select * from marts.Fact_Sales where unit_price <= 0 or quantity > 1000)
    SELECT
        sale_id,
        invoice_no,
        product_key,
        customer_key,
        time_key,
        country_key,
        quantity,
        unit_price,
        total_amount
    FROM fact_sales
    """)

    logger.info(f"anomaly_invoice marked.")

@task
def data_quality():
    """從 Marts 檢查資料品質"""
    logger = get_run_logger()
    logger.info(f"mark data_quality...")

    client = ch.get_client()
    client.command("""
    INSERT INTO quality.data_quality
    WITH
        anomaly_customer_invoioces as (select sale_id from quality.anomaly_customer_invoioces),
        fact_sales as (select sale_id from marts.Fact_Sales),
        fact_sale_returns as (select sale_id from marts.Fact_Sale_Returns),
        anomaly_invoice as (select sale_id, quantity, unit_price from quality.anomaly_invoice)
    SELECT
        today() as check_date,
        (SELECT COUNT(sale_id) from anomaly_customer_invoioces) / ((SELECT COUNT(sale_id) from fact_sales) + (SELECT COUNT(sale_id) from fact_sale_returns)) as missing_customer_id_ratio,
        (SELECT COUNT(sale_id) from anomaly_invoice where unit_price <= 0) as anomaly_unit_price_count,
        (SELECT COUNT(sale_id) from anomaly_invoice where quantity > 1000) as anomaly_quantity_count
    """)

    logger.info(f"data_quality marked.")

@task
def sales_summary():
    """從 Marts 計算每日銷售總額"""
    logger = get_run_logger()
    logger.info(f"Calculating sales_summary...")

    client = ch.get_client()
    client.command("""
    INSERT INTO quality.sales_summary
    WITH
        fact_sales as (select * from marts.Fact_Sales),
        link_invoice_time as (select hub_invoice_hash_key, hub_time_hash_key from vault.link_invoice_time),
        sat_time as (select hub_time_hash_key, date from vault.sat_time)
    SELECT
        t.date as sale_date,
        MIN(s.total_amount) as min_total_amount,
        MAX(s.total_amount) as max_total_amount,
        median(s.total_amount) as median_total_amount,
        AVG(s.total_amount) as avg_total_amount,
        SUM(s.total_amount) as volume
    FROM fact_sales s
    JOIN link_invoice_time lit ON s.sale_id = lit.hub_invoice_hash_key
    JOIN sat_time t ON lit.hub_time_hash_key = t.hub_time_hash_key
    GROUP BY t.date
    ORDER BY t.date
    """)

    logger.info(f"sales_summary calculated.")
