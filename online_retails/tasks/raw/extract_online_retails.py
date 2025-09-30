from datetime import datetime
from pandera import Column, DataFrameSchema, Check
from prefect import task, get_run_logger
from utilx.hashx import generate_hash_key
import io
import os
import pandas as pd
import pandera.pandas as pa
import requests
import utilx.clickhouse_client as ch
import zipfile

@task(retries=3, retry_delay_seconds=10)
def loading_online_retails(url) -> pd.DataFrame:
    """下載、解壓縮、轉換資料並載入到 raw.online_retails"""
    
    logger = get_run_logger()
    logger.info(f"Downloading data from {url}...")
    response = requests.get(url)
    response.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        # 假設 zip 中只有一個 csv 檔
        csv_filename = z.namelist()[0]
        with z.open(csv_filename) as f:
            # 讀取 Excel 格式的 CSV
            df = pd.read_excel(f)

    logger.info("Data downloaded and read into DataFrame.")


    schema = DataFrameSchema({
        "InvoiceNo": Column(str, nullable=True),
        "StockCode": Column(str, nullable=True),
        "Description": Column(str, nullable=True),
        "Quantity": Column(int, nullable=True),
        "InvoiceDate": Column(str, nullable=True),
        "UnitPrice": Column(float, nullable=True),
        "CustomerID": Column("Int64", nullable=True),  # 或使用 Int64 (pandas nullable integer)
        "Country": Column(str, nullable=True),
        "TotalAmount": Column(float, nullable=True)
    }, strict=True, coerce=True)  # coerce=True 自動轉型
    
    # 基本轉換
    df.rename(
        columns={
            'Invoice': 'InvoiceNo', 
            'StockCode': 'StockCode',
            'Description': 'Description', 
            'Quantity': 'Quantity',
            'InvoiceDate': 'InvoiceDate', 
            'Price': 'UnitPrice',
            'Customer ID': 'CustomerID',
            'Country': 'Country'
        }, 
        inplace=True
)
    df['TotalAmount'] = df['Quantity'] * df['UnitPrice']

    # 確保欄位存在
    required_cols = ['InvoiceNo', 'StockCode', 'Description', 'Quantity', 'InvoiceDate', 'UnitPrice', 'CustomerID', 'Country', 'TotalAmount']
    df = df[required_cols]
    df_validated  = schema.validate(df)

    # 寫入 ClickHouse raw table
    logger.info("Writing to raw.online_retails...")
    client = ch.get_client()
    client.command('TRUNCATE TABLE raw.online_retails')
    client.insert_df('raw.online_retails', df_validated)
    logger.info(f"{len(df_validated)} rows inserted into raw.online_retails.")

    
    return df


@task
def prepare_psa_online_retails() -> None:
    """從 raw table 清理資料、產生 Hash Keys 並載入 PSA"""
    logger = get_run_logger()
    logger.info("Preparing data for PSA...")
    client = ch.get_client()
    df = client.query_df('SELECT * FROM raw.online_retails')

    # --- 資料清理與品質保證 ---
    # Pandera schema for PSA cleaning, matching ClickHouse table
    psa_schema = DataFrameSchema({
        # Hash Keys
        "hub_invoice_hash_key": Column(str),
        "hub_product_hash_key": Column(str),
        "hub_customer_hash_key": Column(str),
        "hub_time_hash_key": Column(str),
        "hub_country_hash_key": Column(str),
        "link_invoice_product_hash_key": Column(str),
        "link_invoice_customer_hash_key": Column(str),
        "link_invoice_time_hash_key": Column(str),
        "link_invoice_country_hash_key": Column(str),
        "link_customer_country_hash_key": Column(str),
        # Business Keys & Attributes
        "InvoiceNo": Column(str),
        "StockCode": Column(str),
        "Description": Column(str, nullable=True),
        "Quantity": Column(int),
        "InvoiceDate": Column(pd.Timestamp),
        "UnitPrice": Column(float),
        "TotalAmount": Column(float),
        "CustomerID": Column("UInt64"),
        "Country": Column(str),
        "ReturnStatus": Column(str),
        # DV2.0 Standard Columns
        "LOAD_DATETIME": Column(pd.Timestamp),
        "RECORD_SOURCE": Column(str)
    }, strict=True, coerce=True)

    # Drop NA and type conversions
    df.dropna(subset=['StockCode', 'InvoiceDate'], inplace=True)
    # 將 CustomerID 的空值填充為 0，代表未知客戶
    df['CustomerID'] = df['CustomerID'].fillna(0)

    # 負值與退貨處理
    df['ReturnStatus'] = 'Normal'
    df.loc[(df['InvoiceNo'].str.startswith('C', na=False)) | (df['Quantity'] < 0), 'ReturnStatus'] = 'Return'
    df['InvoiceNo'] = df['InvoiceNo'].str.replace('C', '', regex=False)

    # 產生 Hash Keys
    now = datetime.utcnow()
    df['LOAD_DATETIME'] = now
    df['RECORD_SOURCE'] = 'UCI Online Retail II'
    df['hub_invoice_hash_key'] = generate_hash_key(df, ['InvoiceNo', 'StockCode', 'InvoiceDate'])
    df['hub_product_hash_key'] = generate_hash_key(df, ['StockCode'])
    df['hub_customer_hash_key'] = generate_hash_key(df, ['CustomerID'])
    df['hub_time_hash_key'] = generate_hash_key(df, ['InvoiceDate'])
    df['hub_country_hash_key'] = generate_hash_key(df, ['Country'])
    df['link_invoice_product_hash_key'] = generate_hash_key(df, ['hub_invoice_hash_key', 'hub_product_hash_key'])
    df['link_invoice_customer_hash_key'] = generate_hash_key(df, ['hub_invoice_hash_key', 'hub_customer_hash_key'])
    df['link_invoice_time_hash_key'] = generate_hash_key(df, ['hub_invoice_hash_key', 'hub_time_hash_key'])
    df['link_invoice_country_hash_key'] = generate_hash_key(df, ['hub_invoice_hash_key', 'hub_country_hash_key'])
    df['link_customer_country_hash_key'] = generate_hash_key(df, ['hub_customer_hash_key', 'hub_country_hash_key'])

    psa_cols = [
        'hub_invoice_hash_key', 'hub_product_hash_key', 'hub_customer_hash_key', 'hub_time_hash_key',
        'hub_country_hash_key', 'link_invoice_product_hash_key', 'link_invoice_customer_hash_key',
        'link_invoice_time_hash_key', 'link_invoice_country_hash_key', 'link_customer_country_hash_key',
        'InvoiceNo', 'StockCode', 'Description', 'Quantity', 'InvoiceDate', 'UnitPrice', 
        'TotalAmount', 'CustomerID', 'Country', 'ReturnStatus', 'LOAD_DATETIME', 'RECORD_SOURCE'
    ]
    df_psa = df[psa_cols]
    # Validate with pandera
    df = psa_schema.validate(df_psa)

    # 寫入 PSA
    logger.info("Writing to raw.psa_online_retails...")
    client.command('TRUNCATE TABLE raw.psa_online_retails')
    client.insert_df('raw.psa_online_retails', df_psa)
    logger.info(f"{len(df_psa)} rows inserted into raw.psa_online_retails.")
