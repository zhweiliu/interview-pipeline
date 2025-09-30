from prefect import task, get_run_logger
import utilx.clickhouse_client as ch

@task
def sat_invoice():
	"""從 PSA 載入所有 Invoice Satellite"""
	logger = get_run_logger()
	logger.info(f"Loading sat_invoice...")
	client = ch.get_client()
	client.command("""
	INSERT INTO vault.sat_invoice (hub_invoice_hash_key, Quantity, UnitPrice, TotalAmount, ReturnStatus, EFFECTIVE_FROM, LOAD_DATETIME, RECORD_SOURCE)
	SELECT DISTINCT hub_invoice_hash_key, Quantity, UnitPrice, TotalAmount, ReturnStatus, LOAD_DATETIME AS EFFECTIVE_FROM, LOAD_DATETIME, RECORD_SOURCE
	FROM raw.psa_online_retails
	WHERE hub_invoice_hash_key NOT IN (SELECT hub_invoice_hash_key FROM vault.sat_invoice)
	""")
	logger.info(f"sat_invoice loaded.")

@task
def sat_product():
	"""從 PSA 載入所有 Product Satellite"""
	logger = get_run_logger()
	logger.info(f"Loading sat_product...")
	client = ch.get_client()
	client.command("""
	INSERT INTO vault.sat_product (hub_product_hash_key, Description, EFFECTIVE_FROM, LOAD_DATETIME, RECORD_SOURCE)
	SELECT DISTINCT hub_product_hash_key, Description, LOAD_DATETIME AS EFFECTIVE_FROM, LOAD_DATETIME, RECORD_SOURCE
	FROM raw.psa_online_retails
	WHERE hub_product_hash_key NOT IN (SELECT hub_product_hash_key FROM vault.sat_product)
	""")
	logger.info(f"sat_product loaded.")

@task
def sat_time():
	"""從 PSA 載入所有 Time Satellite"""
	logger = get_run_logger()
	logger.info(f"Loading sat_time...")
	client = ch.get_client()
	client.command("""
	INSERT INTO vault.sat_time (hub_time_hash_key, date, year, month, day_of_week, EFFECTIVE_FROM, LOAD_DATETIME, RECORD_SOURCE)
	SELECT DISTINCT 
		hub_time_hash_key,
		toDate(InvoiceDate) AS date,
		toYear(InvoiceDate) AS year,
		toMonth(InvoiceDate) AS month,
		toDayOfWeek(InvoiceDate) AS day_of_week,
		LOAD_DATETIME AS EFFECTIVE_FROM,
		LOAD_DATETIME,
		RECORD_SOURCE
	FROM raw.psa_online_retails
	WHERE hub_time_hash_key NOT IN (SELECT hub_time_hash_key FROM vault.sat_time)
	""")
	logger.info(f"sat_time loaded.")
