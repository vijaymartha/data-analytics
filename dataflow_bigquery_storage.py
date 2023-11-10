import apache_beam as beam
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json
from apache_beam.io.gcp.bigquery_tools import parse_table_reference_from_string

# Replace 'your_project', 'your_dataset', and 'your_table' with your actual project, dataset, and table names.
project_id = 'your_project'
dataset_id = 'your_dataset'
table_id = 'your_table'

# Replace 'path/to/your/keyfile.json' with the path to your Google Cloud service account key file.
keyfile_path = 'path/to/your/keyfile.json'

# Create a pipeline
options = beam.options.pipeline_options.PipelineOptions()
pipeline = beam.Pipeline(options=options)

# Read BigQuery table schema
table_reference = f"{project_id}:{dataset_id}.{table_id}"
table_schema = parse_table_schema_from_json('{"fields": [{"name": "field1", "type": "STRING"}, {"name": "field2", "type": "INTEGER"}]}')

# Read data from BigQuery using Direct Table Reads from the BigQuery Storage API
read_result = (
    pipeline
    | 'Read from BigQuery Storage API' >> beam.io.ReadFromBigQueryStorage(
        table=table_reference,
        project=project_id,
        keyfile=keyfile_path,
        selected_fields=['field1', 'field2'],  # Specify the columns you want to read
        row_filter='',  # Add a filter if needed
        schema=table_schema
    )
    | 'Print Results' >> beam.Map(print)
)

# Execute the pipeline
pipeline.run()

import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.io.gcp.bigquery import BigQuerySource

# Replace 'your_project', 'your_dataset', and 'your_table' with your actual project, dataset, and table names.
project_id = 'your_project'
dataset_id = 'your_dataset'
table_id = 'your_table'

# Replace 'your_bucket' with your GCS bucket name.
output_bucket = 'your_bucket'
output_file_prefix = 'output_data'

# BigQuery query to fetch data
query = f'SELECT * FROM `{project_id}.{dataset_id}.{table_id}`'

# Define a function to format output file names
def format_output(element):
    return element

# Create a Dataflow pipeline
options = beam.options.pipeline_options.PipelineOptions()
pipeline = beam.Pipeline(options=options)

# Read data from BigQuery using a query
data_from_bigquery = (
    pipeline
    | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query=query, use_standard_sql=True))
)

# Write data to GCS
data_from_bigquery | 'Write to GCS' >> WriteToText(f'gs://{output_bucket}/{output_file_prefix}')

# Execute the pipeline
pipeline.run()
