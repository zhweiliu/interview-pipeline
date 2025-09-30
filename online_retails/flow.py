from prefect import flow
from tasks.raw import extract_online_retails
from tasks.vault import load_hubs, load_links, load_sats
from tasks.mart import build_fact_table, build_dim_table
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

    # Metrics depend on PSA and raw data
    # metrics_task = calculate_metrics(client=client, wait_for=[fact_sales_task, fact_sale_returns_task, dim_product_task, dim_customer_task, dim_time_task, dim_country_task, dim_country_task])

    print("ELT pipeline finished successfully.")

if __name__ == "__main__":
    online_retail_elt_flow()
