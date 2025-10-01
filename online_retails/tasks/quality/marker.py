from prefect import task, get_run_logger
import utilx.clickhouse_client as ch

@task
def anomaly_customer_invoioces():
    """從 Marts 找出異常的客戶"""
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
