import logging
from google.cloud import bigquery
from google.cloud import storage
import json
import io
from google.oauth2 import service_account
# Define the path to the service account file


# Load credentials from the file


class BigQueryHandler:
    def __init__(self, config):
        self.project_id = config['project_id']
        self.dataset_id = config['dataset_id']
        # GCS bucket for temporary file storage
        self.bucket_name = config['bucket_name']
        self.client = bigquery.Client(project=self.project_id)
        self.storage_client = storage.Client()

    def fetch_existing_ids_from_bigquery(self, table_name):
        """Fetches existing IDs from the specified BigQuery table."""
        try:
            dataset_ref = self.client.dataset(self.dataset_id)
            table_ref = dataset_ref.table(table_name)
            table = self.client.get_table(table_ref)
            logging.info(f"Table {table.table_id} exists.")
        except Exception as e:
            logging.info(
                f"Table {table_name} does not exist in {self.dataset_id}. {e}")
            return []

        query = f"SELECT id FROM `{self.project_id}.{self.dataset_id}.{table_name}`"
        query_job = self.client.query(query)

        # Convert the query result to a set of IDs for fast lookup
        existing_ids = {str(row["id"]) for row in query_job}
        logging.info(
            f"Fetched {len(existing_ids)} existing IDs from BigQuery table {table_name}.")
        return existing_ids

    def upload_to_gcs(self, data, gcs_path):
        """Upload newline-delimited JSON data to GCS with error handling and logging."""
        try:
            logging.info(
                f"Uploading data to GCS path: {gcs_path} in bucket {self.bucket_name}.")
            bucket = self.storage_client.bucket(self.bucket_name)
            blob = bucket.blob(gcs_path)

            # Open a file-like object to stream data to GCS
            with blob.open("w") as f:
                for record in data:
                    # Write newline-delimited JSON
                    f.write(json.dumps(record) + '\n')

            logging.info(
                f"Successfully uploaded rows {len(data)} to {gcs_path} in GCS bucket {self.bucket_name}.")
        except Exception as e:
            logging.error(f"Failed to upload data to GCS: {e}")
            raise

    def load_from_gcs_to_bigquery(self, table_name, gcs_path, schema):
        """Load data from GCS into BigQuery, with logging and error handling."""
        try:
            dataset_ref = self.client.dataset(self.dataset_id)
            table_ref = dataset_ref.table(table_name)

            # Set up load job configuration
            job_config = bigquery.LoadJobConfig(
                schema=schema,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  # Overwrite table
            )

            # Load data from GCS to BigQuery
            uri = f"gs://{self.bucket_name}/{gcs_path}"
            logging.info(
                f"Starting BigQuery load job from {uri} to table {table_name}.")
            load_job = self.client.load_table_from_uri(
                uri, table_ref, job_config=job_config)

            # Wait for the job to complete
            load_job.result()
            logging.info(
                f"Successfully loaded rows {load_job.output_rows} from {uri} into BigQuery table {table_name}.")
            if load_job.errors:
                logging.error(
                    f"Errors occurred during BigQuery load job: {load_job.errors}")
                raise RuntimeError(
                    f"BigQuery load job encountered errors: {load_job.errors}")

        except Exception as e:
            logging.error(f"Failed to load data into BigQuery from GCS: {e}")
            raise

    def insert_into_bigquery(self, table_name, data, chunk_size=500):
        """Insert new data into BigQuery in chunks with logging and error handling."""
        try:
            # Define schema based on the first record
            schema = [bigquery.SchemaField(field, "STRING")
                      for field in data[0].keys()]

            # Get table reference and ensure table exists with correct schema
            dataset_ref = self.client.dataset(self.dataset_id)
            table_ref = dataset_ref.table(table_name)

            # Check if table exists; create if not
            try:
                table = self.client.get_table(table_ref)
                logging.info(f"Table {table_name} exists in BigQuery.")
            except Exception:
                table = bigquery.Table(table_ref, schema=schema)
                table = self.client.create_table(table)
                logging.info(
                    f"Created table {table_name} in BigQuery with specified schema.")

            # Insert data in chunks
            total_records = len(data)
            logging.info(
                f"Inserting {total_records} records into BigQuery table {table_name} in chunks.")
            total_batches = total_records // chunk_size + 1
            for i in range(0, total_records, chunk_size):
                batch = data[i:i+chunk_size]
                errors = self.client.insert_rows_json(table_ref, batch)

                if errors:
                    logging.error(
                        f"Errors occurred while inserting rows into BigQuery: {errors}")
                else:
                    logging.info(
                        f"Successfully inserted batch {i // chunk_size + 1} of total {total_batches} batches.")

        except Exception as e:
            logging.error(
                f"Failed to insert data into BigQuery for table {table_name}: {e}")
            raise
