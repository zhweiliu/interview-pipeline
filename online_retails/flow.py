from prefect import flow
from tasks.raw import extract_online_retails
from tasks.vault import load_hubs, load_links, load_sats
from tasks.mart import build_fact_table, build_dim_table
from tasks.quality import marker
from tasks.metrics import notification
import os
from dotenv import load_dotenv
load_dotenv()

@flow(name="Online Retail ELT Pipeline")
def online_retail_elt_flow():
    """
    Main ELT flow to process online retail data from UCI dataset.
    - Extracts and loads data into Raw layer.
    - Cleans and stages data in PSA.
    - Loads data into Data Vault (Hubs, Links, Sats).
    - Builds analytical Star Schema in Marts layer.
    - Calculates monitoring metrics.
    """
    url=os.getenv('ONLINE_RETAIL_DATA_URL', '') 
    
    # --- RAW Layer ---
    raw_df_task = extract_online_retails.loading_online_retails(url=url)
    psa_task = extract_online_retails.prepare_psa_online_retails(wait_for=[raw_df_task])
    
    # --- VAULT Layer ---
    hub_invoice_task = load_hubs.hub_invoice(wait_for=[psa_task])
    hub_product_task = load_hubs.hub_product(wait_for=[psa_task])
    hub_customer_task = load_hubs.hub_customer(wait_for=[psa_task])
    hub_time_task = load_hubs.hub_time(wait_for=[psa_task])
    hub_country_task = load_hubs.hub_country(wait_for=[psa_task])
    link_invoice_product_task = load_links.link_invoice_product(wait_for=[hub_invoice_task,hub_product_task])
    link_invoice_customer_task = load_links.link_invoice_customer(wait_for=[hub_invoice_task,hub_customer_task])
    link_invoice_time_task = load_links.link_invoice_time(wait_for=[hub_invoice_task,hub_time_task])
    link_invoice_country_task = load_links.link_invoice_country(wait_for=[hub_invoice_task,hub_country_task])
    link_customer_country_task = load_links.link_customer_country(wait_for=[hub_customer_task,hub_country_task])
    sat_invoice_task = load_sats.sat_invoice(wait_for=[hub_invoice_task])
    sat_product_task = load_sats.sat_product(wait_for=[hub_product_task])
    sat_time_task = load_sats.sat_time(wait_for=[hub_time_task])
    
    # --- MARTS Layer ---
    # Facts depend on Links and Sats
    fact_sales_task = build_fact_table.fact_sales(wait_for=[hub_invoice_task, sat_invoice_task, link_invoice_product_task, link_invoice_customer_task, link_invoice_time_task, link_invoice_country_task])
    fact_sale_returns_task = build_fact_table.fact_sale_returns(wait_for=[hub_invoice_task, sat_invoice_task, link_invoice_product_task, link_invoice_customer_task, link_invoice_time_task, link_invoice_country_task])

    # Dimensions depend on Hubs and Sats
    dim_product_task = build_dim_table.dim_product(wait_for=[sat_product_task, fact_sales_task])
    dim_customer_task = build_dim_table.dim_customer(wait_for=[link_customer_country_task, fact_sales_task])
    dim_time_task = build_dim_table.dim_time(wait_for=[sat_time_task, fact_sales_task])
    dim_country_task = build_dim_table.dim_country(wait_for=[fact_sales_task])

    # Quality depend on Marts and Vault data
    anomaly_customer_invoioces_task = marker.anomaly_customer_invoioces(wait_for=[hub_customer_task, hub_invoice_task, sat_invoice_task, link_invoice_customer_task, link_invoice_product_task, link_invoice_time_task, link_invoice_country_task])
    anomaly_invoice_task = marker.anomaly_invoice(wait_for=[fact_sales_task])
    data_quality_task = marker.data_quality(wait_for=[anomaly_customer_invoioces_task, anomaly_invoice_task, fact_sales_task, fact_sale_returns_task])
    sales_summary_task = marker.sales_summary(wait_for=[fact_sales_task, link_invoice_time_task, sat_time_task])

    # Notify quality to Prometheus
    notification.anomaly_unit_price_count(wait_for=[data_quality_task])
    notification.anomaly_quantity_count(wait_for=[data_quality_task])
    notification.missing_customer_id_ratio(wait_for=[data_quality_task])
    notification.latest_min_total_amount(wait_for=[sales_summary_task])
    notification.latest_max_total_amount(wait_for=[sales_summary_task])
    notification.latest_median_total_amount(wait_for=[sales_summary_task])
    notification.latest_avg_total_amount(wait_for=[sales_summary_task])
    notification.latest_sales_volume(wait_for=[sales_summary_task])

if __name__ == "__main__":
    online_retail_elt_flow()
