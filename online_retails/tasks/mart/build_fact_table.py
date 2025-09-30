from prefect import task, get_run_logger
import utilx.clickhouse_client as ch

@task
def fact_sales():
    """從 Data Vault 載入所有 Sales Fact"""
    logger = get_run_logger()
    logger.info(f"Building fact_sales...")

    client = ch.get_client()
    client.command("TRUNCATE TABLE marts.Fact_Sales")
    client.command("""
    INSERT INTO marts.Fact_Sales
    with
        hub_invoice as (select hub_invoice_hash_key, InvoiceNo from vault.hub_invoice),
        sat_invoice as (select hub_invoice_hash_key, Quantity, UnitPrice, TotalAmount from vault.sat_invoice where ReturnStatus='Normal'),
        link_invoice_product as (select hub_invoice_hash_key, hub_product_hash_key from vault.link_invoice_product),
        link_invoice_customer as (select hub_invoice_hash_key, hub_customer_hash_key from vault.link_invoice_customer),
        link_invoice_time as (select hub_invoice_hash_key, hub_time_hash_key from vault.link_invoice_time),
        link_invoice_country as (select hub_invoice_hash_key, hub_country_hash_key from vault.link_invoice_country)
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
    LEFT JOIN link_invoice_product p ON i.hub_invoice_hash_key = p.hub_invoice_hash_key
    LEFT JOIN link_invoice_customer c ON i.hub_invoice_hash_key = c.hub_invoice_hash_key
    LEFT JOIN link_invoice_time t ON i.hub_invoice_hash_key = t.hub_invoice_hash_key
    LEFT JOIN link_invoice_country co ON i.hub_invoice_hash_key = co.hub_invoice_hash_key
    """)

    logger.info(f"fact_sales builded.")

@task
def fact_sale_returns():
    """從 Data Vault 載入所有 Sale Returns Fact"""
    logger = get_run_logger()
    logger.info(f"Building fact_sale_returns...")

    client = ch.get_client()
    client.command("TRUNCATE TABLE marts.Fact_Sale_Returns")
    client.command("""
    INSERT INTO marts.Fact_Sale_Returns
    with
        hub_invoice as (select hub_invoice_hash_key, InvoiceNo from vault.hub_invoice),
        sat_invoice as (select hub_invoice_hash_key, Quantity, UnitPrice, TotalAmount from vault.sat_invoice where ReturnStatus='Return'),
        link_invoice_product as (select hub_invoice_hash_key, hub_product_hash_key from vault.link_invoice_product),
        link_invoice_customer as (select hub_invoice_hash_key, hub_customer_hash_key from vault.link_invoice_customer),
        link_invoice_time as (select hub_invoice_hash_key, hub_time_hash_key from vault.link_invoice_time),
        link_invoice_country as (select hub_invoice_hash_key, hub_country_hash_key from vault.link_invoice_country)
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
    LEFT JOIN link_invoice_product p ON i.hub_invoice_hash_key = p.hub_invoice_hash_key
    LEFT JOIN link_invoice_customer c ON i.hub_invoice_hash_key = c.hub_invoice_hash_key
    LEFT JOIN link_invoice_time t ON i.hub_invoice_hash_key = t.hub_invoice_hash_key
    LEFT JOIN link_invoice_country co ON i.hub_invoice_hash_key = co.hub_invoice_hash_key
    """)

    logger.info(f"fact_sale_returns builded.")