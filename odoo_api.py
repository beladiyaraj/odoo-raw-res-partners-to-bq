import requests
import logging
import json
from utils import safe_get, format_timestamp


class OdooAPI:
    def __init__(self, config):
        self.base_url = config['base_url']
        self.api_key = config['api_key']
        self.login = config['login']
        self.password = config['password']
        self.db_name = config['db_name']

    def _make_request(self, model, fields):
        """Private method to make the API request to Odoo."""
        url = (f"{self.base_url}/send_request?model={model}"
               f"&login={self.login}&password={self.password}&api-key={self.api_key}&db={self.db_name}"
               f"&Content-Type=application/json")

        logging.info(f"Constructed URL: {url}")

        headers = {
            "login": self.login,
            "password": self.password,
            "api-key": self.api_key,
            "db": self.db_name,
            "Content-Type": "application/json"
        }

        payload = {"fields": fields}
        # logging.info(f"Payload: {json.dumps(payload, indent=2)}")

        try:
            response = requests.get(
                url, headers=headers, data=json.dumps(payload))
            response.raise_for_status()
            logging.info(f"response has been received.")
            logging.info(f"the response is -> {response.text[0:100]}")
            return response.json().get('records', [])

        except requests.exceptions.RequestException as e:
            logging.error(f"Error making request: {str(e)}")
            return []

    def process_res_partner(self, record):
        """Process a single record into the desired format."""
        return {
            'id': str(record['id']),
            'name': str(record.get('name', '')),
            'state_id': str(record.get('state_id', '')),
            'street': str(record.get('street', '')),
            'street2': str(record.get('street2', '')),
            'contact_address_complete': str(record.get('contact_address_complete', '')),
            'contact_type': str(record.get('contact_type', ''))
        }

    def fetch_res_partner(self, existing_ids):
        """Fetch res users records from Odoo and process them."""

        # , "invoice_user_id"]
        fields = ["id", "category_id", "name", "state_id", "street",
                  "street2", "contact_address_complete", "contact_type"]

        # Fetch data from Odoo
        records = self._make_request('res.partner', fields)
        logging.info(
            f"Data retrieved from API. Total records: {len(records)}.")
        if not records:
            logging.info("No res partner found.")
            return []

        # Filter out records with existing IDs
        new_records = [record for record in records if str(
            record['id']) not in existing_ids]
        logging.info(f"{len(new_records)} new records found.")
        # Process records using map to apply process_record function
        processed_records = list(map(self.process_res_partner, new_records))

        return processed_records  # Only new records to be inserted into BigQuery
