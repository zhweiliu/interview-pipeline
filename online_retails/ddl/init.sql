-- DDL for interview_elt_pipeline

-- ====================================================================
-- 1. RAW DATABASE: 用於存放原始及持久化暫存資料
-- ====================================================================
CREATE DATABASE IF NOT EXISTS raw;

-- 1.1 原始資料落地表 (每次執行前 TRUNCATE)
CREATE TABLE IF NOT EXISTS raw.online_retails (
    InvoiceNo Nullable(String),
    StockCode Nullable(String),
    Description Nullable(String),
    Quantity Nullable(Int32),
    InvoiceDate Nullable(String),
    UnitPrice Nullable(Float64),
    CustomerID Nullable(UInt64),
    Country Nullable(String),
    TotalAmount Nullable(Float64)
) ENGINE = MergeTree()
ORDER BY tuple();

-- 1.2 持久化暫存區 (Persistent Staging Area)
CREATE TABLE IF NOT EXISTS raw.psa_online_retails (
    -- Hash Keys
    hub_invoice_hash_key UUID,
    hub_product_hash_key UUID,
    hub_customer_hash_key UUID,
    hub_time_hash_key UUID,
    hub_country_hash_key UUID,
    link_invoice_product_hash_key UUID,
    link_invoice_customer_hash_key UUID,
    link_invoice_time_hash_key UUID,
    link_invoice_country_hash_key UUID,
    link_customer_country_hash_key UUID,
    -- Business Keys & Attributes
    InvoiceNo String,
    StockCode String,
    Description Nullable(String),
    Quantity Int32,
    InvoiceDate DateTime64(0,'UTC'),
    UnitPrice Decimal(10, 4),
    TotalAmount Decimal(10, 4),
    CustomerID UInt64,
    Country String,
    ReturnStatus LowCardinality(String),
    -- DV2.0 Standard Columns
    LOAD_DATETIME DateTime64(0,'UTC'),
    RECORD_SOURCE String
) ENGINE = MergeTree()
ORDER BY (InvoiceDate, hub_invoice_hash_key);


-- ====================================================================
-- 2. VAULT DATABASE: 儲存核心業務模型
-- ====================================================================
CREATE DATABASE IF NOT EXISTS vault;

-- 2.1 HUBS --

CREATE TABLE IF NOT EXISTS vault.hub_invoice (
    hub_invoice_hash_key UUID,
    InvoiceNo String,
    InvoiceDate DateTime64(0, 'UTC'),
    LOAD_DATETIME DateTime64(0, 'UTC'),
    RECORD_SOURCE String
) ENGINE = MergeTree()
PRIMARY KEY hub_invoice_hash_key;

CREATE TABLE IF NOT EXISTS vault.hub_product (
    hub_product_hash_key UUID,
    StockCode String,
    LOAD_DATETIME DateTime64(0, 'UTC'),
    RECORD_SOURCE String
) ENGINE = MergeTree()
PRIMARY KEY hub_product_hash_key;

CREATE TABLE IF NOT EXISTS vault.hub_customer (
    hub_customer_hash_key UUID,
    CustomerID UInt64,
    LOAD_DATETIME DateTime64(0, 'UTC'),
    RECORD_SOURCE String
) ENGINE = MergeTree()
PRIMARY KEY hub_customer_hash_key;

drop table vault.hub_time;
CREATE TABLE IF NOT EXISTS vault.hub_time (
    hub_time_hash_key UUID,
    InvoiceDate DateTime64(0, 'UTC'),
    LOAD_DATETIME DateTime64(0, 'UTC'),
    RECORD_SOURCE String
) ENGINE = MergeTree()
PRIMARY KEY hub_time_hash_key;

CREATE TABLE IF NOT EXISTS vault.hub_country (
    hub_country_hash_key UUID,
    Country String,
    LOAD_DATETIME DateTime64(0, 'UTC'),
    RECORD_SOURCE String
) ENGINE = MergeTree()
PRIMARY KEY hub_country_hash_key;


-- 2.2 LINKS --
CREATE TABLE IF NOT EXISTS vault.link_invoice_product (
    link_invoice_product_hash_key UUID,
    hub_invoice_hash_key UUID,
    hub_product_hash_key UUID,
    LOAD_DATETIME DateTime64(0, 'UTC'),
    RECORD_SOURCE String
) ENGINE = MergeTree()
PRIMARY KEY link_invoice_product_hash_key;

CREATE TABLE IF NOT EXISTS vault.link_invoice_customer (
    link_invoice_customer_hash_key UUID,
    hub_invoice_hash_key UUID,
    hub_customer_hash_key UUID,
    LOAD_DATETIME DateTime64(0, 'UTC'),
    RECORD_SOURCE String
) ENGINE = MergeTree()
PRIMARY KEY link_invoice_customer_hash_key;

CREATE TABLE IF NOT EXISTS vault.link_invoice_time (
    link_invoice_time_hash_key UUID,
    hub_invoice_hash_key UUID,
    hub_time_hash_key UUID,
    LOAD_DATETIME DateTime64(0, 'UTC'),
    RECORD_SOURCE String
) ENGINE = MergeTree()
PRIMARY KEY link_invoice_time_hash_key;

CREATE TABLE IF NOT EXISTS vault.link_invoice_country (
    link_invoice_country_hash_key UUID,
    hub_invoice_hash_key UUID,
    hub_country_hash_key UUID,
    LOAD_DATETIME DateTime64(0, 'UTC'),
    RECORD_SOURCE String
) ENGINE = MergeTree()
PRIMARY KEY link_invoice_country_hash_key;

CREATE TABLE IF NOT EXISTS vault.link_customer_country (
    link_customer_country_hash_key UUID,
    hub_customer_hash_key UUID,
    hub_country_hash_key UUID,
    LOAD_DATETIME DateTime64(0, 'UTC'),
    RECORD_SOURCE String
) ENGINE = MergeTree()
PRIMARY KEY link_customer_country_hash_key;

-- 2.3 SATELLITES (Using ReplacingMergeTree for SCD2) --

CREATE TABLE IF NOT EXISTS vault.sat_invoice (
	hub_invoice_hash_key UUID,
	Quantity Int32,
    UnitPrice Decimal(10, 4),
    TotalAmount Decimal(10, 4),
    ReturnStatus Enum8('Normal' = 0, 'Return' = 1),
    EFFECTIVE_FROM DateTime64(0, 'UTC'),
    LOAD_DATETIME DateTime64(0, 'UTC'),
    RECORD_SOURCE String
) ENGINE = ReplacingMergeTree(LOAD_DATETIME)
PRIMARY KEY hub_invoice_hash_key
ORDER BY (hub_invoice_hash_key, EFFECTIVE_FROM);

CREATE TABLE IF NOT EXISTS vault.sat_product (
    hub_product_hash_key UUID,
    Description Nullable(String),
    EFFECTIVE_FROM DateTime64(0, 'UTC'),
    LOAD_DATETIME DateTime64(0, 'UTC'),
    RECORD_SOURCE String
) ENGINE = ReplacingMergeTree(LOAD_DATETIME)
PRIMARY KEY hub_product_hash_key
ORDER BY (hub_product_hash_key, EFFECTIVE_FROM);


CREATE TABLE IF NOT EXISTS vault.sat_time (
    hub_time_hash_key UUID,
    date Date,
    year int,
    month int,
    day_of_week int,
    EFFECTIVE_FROM DateTime64(0, 'UTC'),
    LOAD_DATETIME DateTime64(0, 'UTC'),
    RECORD_SOURCE String
) ENGINE = ReplacingMergeTree(LOAD_DATETIME)
PRIMARY KEY hub_time_hash_key
ORDER BY (hub_time_hash_key, EFFECTIVE_FROM);


-- ====================================================================
-- 3. MARTS DATABASE: 存放供分析的 Star Schema 及監控指標
-- ====================================================================
CREATE DATABASE IF NOT EXISTS marts;

-- 3.1 Dimensions --
CREATE TABLE IF NOT EXISTS marts.Dim_Product (
    product_key UUID,
    stock_code String,
    description Nullable(String)
) ENGINE = MergeTree()
PRIMARY KEY product_key;

CREATE TABLE IF NOT EXISTS marts.Dim_Customer (
    customer_key UUID,
    customer_id UInt64,
    country_key UUID
) ENGINE = MergeTree()
ORDER BY(customer_key, country_key);

CREATE TABLE IF NOT EXISTS marts.Dim_Time (
    time_key UUID,
    date Date,
    year UInt16,
    month UInt8,
    day_of_week UInt8
) ENGINE = MergeTree()
PRIMARY KEY time_key;

CREATE TABLE IF NOT EXISTS marts.Dim_Country (
    country_key UUID,
    country_name String
) ENGINE = MergeTree()
PRIMARY KEY country_key;



-- 3.2 Facts --
CREATE TABLE IF NOT EXISTS marts.Fact_Sales (
    sale_id UUID,
    invoice_no String,
    product_key UUID,
    customer_key UUID,
    time_key UUID,
    country_key UUID,
    quantity Int32,
    unit_price Decimal(10, 4),
    total_amount Decimal(10, 4)
) ENGINE = MergeTree()
ORDER BY (sale_id);


CREATE TABLE IF NOT EXISTS marts.Fact_Sale_Returns (
    sale_id UUID,
    invoice_no String,
    product_key UUID,
    customer_key UUID,
    time_key UUID,
    country_key UUID,
    quantity Int32,
    unit_price Decimal(10, 4),
    total_amount Decimal(10, 4)
) ENGINE = MergeTree()
ORDER BY (sale_id);


-- ====================================================================
-- 4. METRICS DATABASE: 存放異常數據與品質檢查的監控指標
-- ====================================================================

CREATE DATABASE IF NOT EXISTS quality;

-- 4.1 Metrics for anomaly detection --
CREATE TABLE IF NOT EXISTS quality.anomaly_customer_invoioces (
    sale_id UUID,
    invoice_no String,
    product_key UUID,
    customer_key UUID,
    time_key UUID,
    country_key UUID,
    quantity Int32,
    unit_price Decimal(10, 4),
    total_amount Decimal(10, 4)
) ENGINE = MergeTree()
ORDER BY sale_id;


CREATE TABLE IF NOT EXISTS quality.anomaly_invoice (
    sale_id UUID,
    invoice_no String,
    product_key UUID,
    customer_key UUID,
    time_key UUID,
    country_key UUID,
    quantity Int32,
    unit_price Decimal(10, 4),
    total_amount Decimal(10, 4)
) ENGINE = MergeTree()
ORDER BY sale_id;


CREATE TABLE IF NOT EXISTS quality.data_quality (
    check_date Date,
    missing_customer_id_ratio Float64,
    anomaly_unit_price_count UInt64,
    anomaly_quantity_count UInt64
) ENGINE = ReplacingMergeTree(check_date)
ORDER BY check_date;


CREATE TABLE IF NOT EXISTS quality.sales_summary (
    sales_date Date,
    min_total_amount Float64,
    max_total_amount Float64,
    median_total_amount Float64,
    avg_total_amount Float64,
    volume Float64
) ENGINE = ReplacingMergeTree(sales_date)
ORDER BY sales_date;
