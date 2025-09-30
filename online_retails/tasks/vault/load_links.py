from prefect import task, get_run_logger
import utilx.clickhouse_client as ch

@task
def link_invoice_product():
	"""從 PSA 載入所有 Invoice-Product Link"""
	logger = get_run_logger()
	logger.info(f"Loading link_invoice_product...")
	client = ch.get_client()
	client.command("""
	INSERT INTO vault.link_invoice_product (link_invoice_product_hash_key, hub_invoice_hash_key, hub_product_hash_key, LOAD_DATETIME, RECORD_SOURCE)
	SELECT DISTINCT link_invoice_product_hash_key, hub_invoice_hash_key, hub_product_hash_key, LOAD_DATETIME, RECORD_SOURCE
	FROM raw.psa_online_retails
	WHERE link_invoice_product_hash_key NOT IN (SELECT link_invoice_product_hash_key FROM vault.link_invoice_product)
	""")
	logger.info(f"link_invoice_product loaded.")

@task
def link_invoice_customer():
	"""從 PSA 載入所有 Invoice-Customer Link"""
	logger = get_run_logger()
	logger.info(f"Loading link_invoice_customer...")
	client = ch.get_client()
	client.command("""
	INSERT INTO vault.link_invoice_customer (link_invoice_customer_hash_key, hub_invoice_hash_key, hub_customer_hash_key, LOAD_DATETIME, RECORD_SOURCE)
	SELECT DISTINCT link_invoice_customer_hash_key, hub_invoice_hash_key, hub_customer_hash_key, LOAD_DATETIME, RECORD_SOURCE
	FROM raw.psa_online_retails
	WHERE link_invoice_customer_hash_key NOT IN (SELECT link_invoice_customer_hash_key FROM vault.link_invoice_customer)
	""")
	logger.info(f"link_invoice_customer loaded.")

@task
def link_invoice_time():
	"""從 PSA 載入所有 Invoice-Time Link"""
	logger = get_run_logger()
	logger.info(f"Loading link_invoice_time...")
	client = ch.get_client()
	client.command("""
	INSERT INTO vault.link_invoice_time (link_invoice_time_hash_key, hub_invoice_hash_key, hub_time_hash_key, LOAD_DATETIME, RECORD_SOURCE)
	SELECT DISTINCT link_invoice_time_hash_key, hub_invoice_hash_key, hub_time_hash_key, LOAD_DATETIME, RECORD_SOURCE
	FROM raw.psa_online_retails
	WHERE link_invoice_time_hash_key NOT IN (SELECT link_invoice_time_hash_key FROM vault.link_invoice_time)
	""")
	logger.info(f"link_invoice_time loaded.")

@task
def link_invoice_country():
	"""從 PSA 載入所有 Invoice-Country Link"""
	logger = get_run_logger()
	logger.info(f"Loading link_invoice_country...")
	client = ch.get_client()
	client.command("""
	INSERT INTO vault.link_invoice_country (link_invoice_country_hash_key, hub_invoice_hash_key, hub_country_hash_key, LOAD_DATETIME, RECORD_SOURCE)
	SELECT DISTINCT link_invoice_country_hash_key, hub_invoice_hash_key, hub_country_hash_key, LOAD_DATETIME, RECORD_SOURCE
	FROM raw.psa_online_retails
	WHERE link_invoice_country_hash_key NOT IN (SELECT link_invoice_country_hash_key FROM vault.link_invoice_country)
	""")
	logger.info(f"link_invoice_country loaded.")

@task
def link_customer_country():
	"""從 PSA 載入所有 Invoice-Country Link"""
	logger = get_run_logger()
	logger.info(f"Loading link_invoice_country...")
	client = ch.get_client()
	client.command("""
	INSERT INTO vault.link_customer_country (link_customer_country_hash_key, hub_customer_hash_key, hub_country_hash_key, LOAD_DATETIME, RECORD_SOURCE)
	SELECT DISTINCT link_customer_country_hash_key, hub_customer_hash_key, hub_country_hash_key, LOAD_DATETIME, RECORD_SOURCE
	FROM raw.psa_online_retails
	WHERE link_customer_country_hash_key NOT IN (SELECT link_customer_country_hash_key FROM vault.link_customer_country)
	""")
	logger.info(f"link_invoice_country loaded.")
