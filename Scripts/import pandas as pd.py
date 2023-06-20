import pandas as pd
import io
from datetime import datetime
from google.cloud import storage, bigquery, logging
from config import project_id, dataset_id, table_id, bucket_id, file_name, sender_email, sender_password, receiver_email, subject, sm_server, port, json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
from google.oauth2 import service_account


log_name = f"{datetime.now().strftime('logfile_%H_%M_%d_%m_%Y.log')}"


storage_client = storage.Client.from_service_account_json(json)
bq_client = bigquery.Client.from_service_account_json(json)
client = logging.Client.from_service_account_json(json)
logger = client.logger(log_name)
logger.log_text("Log Entry")
credentials = service_account.Credentials.from_service_account_file(json)


def read_file_from_storage(bucket_name, file_name):

    logger.log_text(f"Reading file {file_name}")

    file_path = f'gs://{bucket_name}/{file_name}'
    storage_df = pd.read_excel(file_path)
    return storage_df

def read_data_from_bigquery(project_id, table_id, dataset):

    logger.log_text(f"Reading gbq table {table_id}")

    query = f"select * from {project_id}.{dataset}.{table_id}"

    gbq_df = pd.read_gbq(query, credentials=credentials)
    # print(gbq_df)

    return gbq_df




def load_data_gbq_table(storage_df, gbq_df, table_name, dataset_name):

    merged = storage_df._append(gbq_df, ignore_index=True)

    columns = merged.columns.tolist()
    schema = []

    for column in columns:

        col = {'name': f'{column}', 'type': 'STRING'}

        schema.append(col)

    merged.fillna('None')
    merged = merged.astype(str)

    merged.to_gbq(f'{dataset_name}.{table_name}', if_exists='replace',
                  table_schema=schema, credentials=credentials)


def send_email(sender_email, sender_password, receiver_email, subject, message):

    smtp_server = sm_server
    smtp_port = port

    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = receiver_email
    msg["Subject"] = subject

    msg.attach(MIMEText(message, "plain"))

    try:

        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(sender_email, sender_password)

        server.send_message(msg)

        server.quit()
        print("Email notification sent successfully!")
    except Exception as e:
        print("An error occurred while sending the email:", str(e))
        

def process_start(event, context):

    message = f"Hello, {file_name} has received into bucket Job is starting"
    send_email(sender_email, sender_password, receiver_email, subject, message)

    bucket_name = event['bucket']
    file_name = event['name']

    store_df = read_file_from_storage(bucket_name, file_name)

    bq_df = read_data_from_bigquery(project_id, table_id, dataset_id)

    load_data_gbq_table(store_df, bq_df, table_id, dataset_id)

    Output_folder = 'Output'
    cmd = f"gsutil mv 'gs://{bucket_id}/{file_name}' 'gs://{bucket_id}/{Output_folder}/'"
    os.system(cmd)
    logger.log_text(f'{cmd} Execution completed')















# def load_data_to_bigquery(event, context):


#     bigquery_client = bigquery.Client()
    
#     # Extracting bucket and file details from the event
#     bucket_name = event['bucket']
#     file_name = event['name']
    
#     # Creating the GCS file URI
#     file_uri = f"gs://{bucket_name}/{file_name}"
    
#     # Setting up the BigQuery dataset and table names
    
#     table_id = f"{Project_id}.{Dataset_id}.{Table_id}"

   


#     job_config = bigquery.LoadJobConfig(
#         schema=[
#         bigquery.SchemaField("Name", "STRING"),
#         bigquery.SchemaField("Sex", "STRING"),
#         bigquery.SchemaField("Age", "INTEGER"),
#         bigquery.SchemaField("Height__in_", "INTEGER"),
#         bigquery.SchemaField("Weight__lbs_", "INTEGER"),

#     ],
#         skip_leading_rows=1,
#         source_format=bigquery.SourceFormat.CSV
#     )
#     load_job = bigquery_client.load_table_from_uri(
#         file_uri,
#         table_id,
#         job_config=job_config
#     )
#     load_job.result()  # Wait for the job to complete
#     print(f"Data loaded into BigQuery table {dataset_id}.{table_id} successfully.")

#     Output_folder = 'Output'

#     cmd = f'gsutil mv gs://{bucket_name}/{file_name} gs://{bucket_name}/{Output_folder}/'
#     os.run(cmd)

merged = storage_df._append(gbq_df, ignore_index=True)

        columns = merged.columns.tolist()
        schema = []

        for column in columns:

            col = {'name': f'{column}', 'type': 'STRING'}

            schema.append(col)

        #merged.fillna('None')
        merged = merged.astype(str)

        merged.to_gbq(f'{dataset_name}.{table_name}', if_exists='replace',
                    table_schema=schema, credentials=credentials)
        logger.log_text(f"Table Load Completed")