import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import bigquery
from datetime import datetime

# BigQuery table names
project_id = "forward-entity-444814-b5"
table = "forward-entity-444814-b5:raw_layer.sales"


class TransformData(beam.DoFn):
    def process(self, element):
        from datetime import datetime
        # Parse CSV row
        fields = element.split(',')
        yield {
             'invoice_id': fields[0] ,
             'branch': fields[1],
             'city': fields[2] ,
             'customer_type': fields[3] ,
             'gender': fields[4],
             'product_line': fields[5],
             'unit_price': float(fields[6]),
             'quantity': int(fields[7]),
             'tax': float(fields[8]),
             'total': float(fields[9]),
             't_date': datetime.strptime(fields[10], "%m/%d/%Y").strftime("%Y-%m-%d") ,
             't_time': fields[11] + ':00.000000',
             'payment': fields[12],
             'cogs': float(fields[13]),
             'gross_margin_percentage': float(fields[14]),
             'gross_income': float(fields[15]),
             'rating': float(fields[16])
        }
        

def run():
    options = PipelineOptions(runner='DataflowRunner', 
                              project=project_id, 
                              temp_location='gs://data_load_kaggle/temp', 
                              region='us-central1',)
    with beam.Pipeline(options=options) as p:
        bq_client = bigquery.Client(project_id)
        table_ref = bq_client.get_table(table.replace(":","."))
        schema = table_ref.to_api_repr()['schema']

        raw_data = p | "Read CSV" >> beam.io.ReadFromText("gs://data_load_kaggle/supermarket_sales.csv", skip_header_lines=1)
        
        transformed = raw_data | "Transform Data" >> beam.ParDo(TransformData())
        
        # Write to BigQuery
        transformed | "Write Raw layer" >> beam.io.WriteToBigQuery(
            table, schema=schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
        ) # Used write_truncate as its a whole load table

if __name__ == '__main__':
    run()