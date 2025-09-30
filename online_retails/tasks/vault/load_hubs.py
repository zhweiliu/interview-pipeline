from prefect import task, get_run_logger
import utilx.clickhouse_client as ch

@task
def hub_invoice():
    """從 PSA 載入所有 Invoice"""
    logger = get_run_logger()
    logger.info(f"Loading hub_invoice...")

    client = ch.get_client()
    
    client.command("""
    INSERT INTO vault.hub_invoice (hub_invoice_hash_key, InvoiceNo, InvoiceDate, LOAD_DATETIME, RECORD_SOURCE)
    SELECT DISTINCT hub_invoice_hash_key, InvoiceNo, InvoiceDate, LOAD_DATETIME, RECORD_SOURCE
    FROM raw.psa_online_retails
    WHERE hub_invoice_hash_key NOT IN (SELECT hub_invoice_hash_key FROM vault.hub_invoice)
    """)

    logger.info(f"hub_invoice loaded.")

@task
def hub_product():
    """從 PSA 載入所有 Product"""
    logger = get_run_logger()
    logger.info(f"Loading hub_product...")

    client = ch.get_client()
    
    client.command("""
    INSERT INTO vault.hub_product (hub_product_hash_key, StockCode, LOAD_DATETIME, RECORD_SOURCE)
    SELECT DISTINCT hub_product_hash_key, StockCode, LOAD_DATETIME, RECORD_SOURCE
    FROM raw.psa_online_retails
    WHERE hub_product_hash_key NOT IN (SELECT hub_product_hash_key FROM vault.hub_product)
    """)

    logger.info(f"hub_product loaded.")

@task
def hub_customer():
    """從 PSA 載入所有 Customer"""
    logger = get_run_logger()
    logger.info(f"Loading hub_customer...")

    client = ch.get_client()
    client.command("""
    INSERT INTO vault.hub_customer (hub_customer_hash_key, CustomerID, LOAD_DATETIME, RECORD_SOURCE)
    SELECT DISTINCT hub_customer_hash_key, CustomerID, LOAD_DATETIME, RECORD_SOURCE
    FROM raw.psa_online_retails
    WHERE hub_customer_hash_key NOT IN (SELECT hub_customer_hash_key FROM vault.hub_customer)
    """)
    logger.info(f"hub_customer loaded.")

@task
def hub_time():
    """從 PSA 載入所有 Time"""
    logger = get_run_logger()
    logger.info(f"Loading hub_time...")

    client = ch.get_client()
    client.command("""
    INSERT INTO vault.hub_time (hub_time_hash_key, InvoiceDate, LOAD_DATETIME, RECORD_SOURCE)
    SELECT DISTINCT hub_time_hash_key, InvoiceDate, LOAD_DATETIME, RECORD_SOURCE
    FROM raw.psa_online_retails
    WHERE hub_time_hash_key NOT IN (SELECT hub_time_hash_key FROM vault.hub_time)
    """)
    logger.info(f"hub_time loaded.")

@task
def hub_country():
    """從 PSA 載入所有 Country"""
    logger = get_run_logger()
    logger.info(f"Loading hub_country...")

    client = ch.get_client()
    client.command("""
    INSERT INTO vault.hub_country (hub_country_hash_key, Country, LOAD_DATETIME, RECORD_SOURCE)
    SELECT DISTINCT hub_country_hash_key, Country, LOAD_DATETIME, RECORD_SOURCE
    FROM raw.psa_online_retails
    WHERE hub_country_hash_key NOT IN (SELECT hub_country_hash_key FROM vault.hub_country)
    """)
    logger.info(f"hub_country loaded.")
