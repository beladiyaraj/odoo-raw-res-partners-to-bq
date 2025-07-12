import logging
import json
from odoo_api import OdooAPI
from bigquery_handler import BigQueryHandler
from utils import load_config

logging.basicConfig(level=logging.INFO)

def main(cloud_event, abc):
    # Load configuration
    config = load_config()

    # Initialize OdooAPI and BigQueryHandler
    odoo_api = OdooAPI(config['odoo'])
    bigquery_handler = BigQueryHandler(config['bigquery'])


    ##### Workflow for Fetching, Filtering, and Loading res_partner #####
    try:
        # Step 1: Fetch existing IDs directly from BigQuery
        existing_ids = bigquery_handler.fetch_existing_ids_from_bigquery("res_partner")
        
        # Step 2: Fetch and filter new records from OdooAPI based on existing BigQuery IDs
        new_records = odoo_api.fetch_res_partner(existing_ids)
        
        if new_records:
            # Step 3: Insert only new records into BigQuery
            bigquery_handler.insert_into_bigquery("res_partner", new_records, 1000)
            logging.info(f"Successfully inserted {len(new_records)} new records into BigQuery table 'res_partner'.")
        else:
            logging.info("No new res_partner to insert.")
    
    except Exception as e:
        logging.error(f"Error in fetching or loading res_partner to BigQuery: {e}")
