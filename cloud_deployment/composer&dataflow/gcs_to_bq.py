from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator 
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from google.cloud import bigquery

# Default arguments for the DAG
default_args = {
    'owner': 'Ramprakash Pavithrakannan',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Function to fetch data and format it as plain text
def fetch_data_and_format(**context):
    table_name = context['templates_dict']['table_name']
    header = context['templates_dict']['header']
    client = bigquery.Client()
    
    # SQL query to fetch the data
    query = f"SELECT * FROM analytics_layer.{table_name};"
    query_job = client.query(query)
    results = query_job.result() 

    # Format the records as plain text
    #plain_text_data = "Product Line | Total Sales\n"
    plain_text_data = header
    plain_text_data += "-" * 50 + "\n"
    
    for row in results:
        if table_name != 'max_last_sold':
            plain_text_data += f"{row[0]} | {row[1]} | ${row[2]}\n"
        else:
            plain_text_data += f"{row[0]} | {row[1]} | {row[2]}\n"
    
    print(plain_text_data)
    # Return the formatted plain text data
    return plain_text_data


# Define the DAG
with DAG(
    dag_id='gcs_to_bq_ingestion',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:


    trigger_dataflow = BashOperator(task_id='load_gcs_to_raw',
    bash_command = "python3 /home/airflow/gcs/dags/dataflow/dataflow.py --job_id='gcs_to_bq' --project='forward-entity-444814-b5'")

    confirmed_layer = BigQueryInsertJobOperator(task_id='load_confirmed_layer',
                      configuration={
                        "query": {
                            "query":"CALL confirmed_layer.load_dimensions_and_fact()",
                            "useLegacySql": False

                      }})

    analytical_layer_1 = BigQueryInsertJobOperator(task_id='load_top_3_products_by_store',
                      configuration={
                        "query": {
                            "query":"CALL analytics_layer.top_3_products_by_store()",
                            "useLegacySql": False

                      }})


    analytical_layer_2 = BigQueryInsertJobOperator(task_id='load_top_store_by_product',
                      configuration={
                        "query": {
                            "query":"CALL analytics_layer.top_store_by_product()",
                            "useLegacySql": False

                      }})


    analytical_layer_3 = BigQueryInsertJobOperator(task_id='load_max_last_sold',
                      configuration={
                        "query": {
                            "query":"CALL analytics_layer.max_last_sold()",
                            "useLegacySql": False

                      }})

    fetch_and_format_task_1 = PythonOperator(
        task_id='fetch_and_format_data_top_3_products_by_store',
        python_callable=fetch_data_and_format,
        templates_dict={
            "table_name": "top_3_products_by_store",
            "header": "product_line,branch,total_sale\n"
        }
    )

    send_email_task_1 = EmailOperator(
        task_id='send_email_top_3_products_by_store',
        to='rampk.can@gmail.com',
        subject='Top 3 Products by store',
        html_content="{{ ti.xcom_pull(task_ids='fetch_and_format_task_1') }}",  
        mime_charset='utf-8',
        #body="{{ ti.xcom_pull(task_ids='fetch_and_format_task_1') }}",  
    )

    fetch_and_format_task_2 = PythonOperator(
        task_id='fetch_and_format_data_top_store_by_product',
        python_callable=fetch_data_and_format,
        templates_dict={
            "table_name": "top_store_by_product",
            "header": "product_line,branch,total_sale\n"
        }
    )

    send_email_task_2 = EmailOperator(
        task_id='send_email_top_store_by_product',
        to='rampk.can@gmail.com',
        subject='Top store by product',
        html_content="{{ ti.xcom_pull(task_ids='fetch_and_format_task_2') }}",  
        mime_charset='utf-8',
        #body="{{ ti.xcom_pull(task_ids='fetch_and_format_task_2') }}",  
    )

    fetch_and_format_task_3 = PythonOperator(
        task_id='fetch_and_format_data_max_last_sold',
        python_callable=fetch_data_and_format,
        templates_dict={
            "table_name": "max_last_sold",
            "header": "product_line,branch,total_sale\n"
        }
    )

    send_email_task_3 = EmailOperator(
        task_id='send_email_max_last_sold',
        to='rampk.can@gmail.com',
        subject='Max last sold product',
        html_content="{{ ti.xcom_pull(task_ids='fetch_and_format_task_3') }}",  
        mime_charset='utf-8',
        #body="{{ ti.xcom_pull(task_ids='fetch_and_format_task_3') }}",  
    )

    trigger_dataflow >> confirmed_layer
    confirmed_layer >> analytical_layer_1 >> fetch_and_format_task_1 >> send_email_task_1
    confirmed_layer >> analytical_layer_2 >> fetch_and_format_task_2 >> send_email_task_2
    confirmed_layer >> analytical_layer_3 >> fetch_and_format_task_3 >> send_email_task_3
