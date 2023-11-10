import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# Replace 'your_project', 'your_dataset', and 'table1'/'table2' with your actual project, dataset, and table names.
project_id = 'your_project'
dataset_id = 'your_dataset'
table1_id = 'table1'
table2_id = 'table2'

# Define your pipeline options
options = PipelineOptions()

def compare_rows(row1, row2):
    # Compare rows column-wise and value-wise
    # Add your custom comparison logic here
    return row1 == row2

# Define your pipeline
with beam.Pipeline(options=options) as p:
    # Read data from the first table
    data1 = (
        p
        | 'Read from Table1' >> beam.io.Read(beam.io.BigQuerySource(query=f'SELECT * FROM `{project_id}.{dataset_id}.{table1_id}`', use_standard_sql=True))
    )

    # Read data from the second table
    data2 = (
        p
        | 'Read from Table2' >> beam.io.Read(beam.io.BigQuerySource(query=f'SELECT * FROM `{project_id}.{dataset_id}.{table2_id}`', use_standard_sql=True))
    )

    # Compare the rows using a custom function
    compared_data = (
        {'data1': data1, 'data2': data2}
        | 'Compare Rows' >> beam.Map(lambda element: compare_rows(element['data1'], element['data2']))
        | 'Filter Mismatched Rows' >> beam.Filter(lambda comparison_result: not comparison_result)
        | 'Print Mismatched Rows' >> beam.Map(print)
    )






#####
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
