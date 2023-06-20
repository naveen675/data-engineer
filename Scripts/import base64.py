import base64
import pandas as pd
import io
from datetime import datetime
from google.cloud import storage, bigquery, logging, dlp
from config import project_id, dataset_id, table_id, etl_tracker, bucket_id, output_bucket, sender_email, sender_password, receiver_email, subject, sm_server, port, json, progress, success, failed
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
from google.oauth2 import service_account


log_name = f"{datetime.now().strftime('logfile_%H_%M_%d_%m_%Y.log')}"
print(log_name)


storage_client = storage.Client.from_service_account_json(json)
dlp_client = dlp.DlpServiceClient.from_service_account_file(json)
bq_client = bigquery.Client.from_service_account_json(json)
client = logging.Client.from_service_account_json(json)
logger = client.logger(log_name)
logger.log_text("Log Entry")
credentials = service_account.Credentials.from_service_account_file(json)


def hello_pubsub():

    pubsub_message = "gs://pubsub_trigger/Planner_Search_Political Events.xlsx"

    try:

        bucket = pubsub_message.split("/")[2]
        bucket_name = storage_client.get_bucket(bucket)
        file_name = pubsub_message.split("/")[-1]
        # print(bucket_name)

        blob = bucket_name.blob(file_name)
        print(blob)
        if not blob.exists():
            print(f"{file_name} not available in {bucket}")

        # return process_start(bucket, file_name)

    except Exception as e:
        print(f"Error in storage path parsing {e}")
        # update_tracker_table(
        #     file_name, failed, "Error in storage path parsing")
        # move_file(file_name, "Failed")
        return "Job Exited with error"


hello_pubsub()


def read_file_from_storage(bucket_name, file_name):

    logger.log_text(f"Reading file {file_name}")

    file_path = f"gs://{bucket_name}/{file_name}"

    try:
        storage_df = pd.read_excel(file_path)
        storage_df = storage_df.applymap(str)
        storage_df = storage_df.applymap(mask_sensitive_data)
        logger.log_text(f"File Reading Completed")
        update_tracker_table(file_name, progress, "File Reading completed")

    except Exception as e:
        logger.log_text(f"Job has been failed with error {e}")
        update_tracker_table(file_name, failed, "Error in file reading")
        move_file(file_name, "Failed")
        return "Job Exited with error"
    return read_data_from_bigquery(project_id, table_id, dataset_id, storage_df, file_name)


def read_data_from_bigquery(project_id, table_id, dataset, storage_df, file_name):

    logger.log_text(f"Reading gbq table {table_id}")

    query = f"select * from {project_id}.{dataset}.{table_id}"

    try:

        gbq_df = pd.read_gbq(query, credentials=credentials)
        print(gbq_df)
        logger.log_text(f"Table Reading Completed")
        update_tracker_table(file_name, progress,
                             f"Table {table_id} to dataframe load completed")
    except Exception as e:
        logger.log_text(f"Job has been failed with error {e}")
        update_tracker_table(
            file_name, failed, f"Error in reading Table {table_id} ")
        move_file(file_name, "Failed")
        return e
    return load_data_gbq_table(storage_df, gbq_df, table_id, dataset, file_name)


def load_data_gbq_table(storage_df, gbq_df, table_name, dataset_name, file_name):

    logger.log_text(f"Table Load is Starting")

    try:
        merged = storage_df._append(gbq_df, ignore_index=True)

        columns = merged.columns.tolist()
        schema = []

        for column in columns:

            col = {'name': f'{column}', 'type': 'STRING'}

            schema.append(col)

        # merged.fillna('None')
        merged = merged.astype(str)

        merged.to_gbq(f'{dataset_name}.{table_name}', if_exists='replace',
                      table_schema=schema, credentials=credentials)
        logger.log_text(f"Table Load Completed")

        message = f"Hello, {file_name} has been loaded"
        send_email(sender_email, sender_password,
                   receiver_email, subject, message)

        update_tracker_table(file_name, success,
                             f"file {file_name} loaded to table {table_name}")
        move_file(file_name, "Success")

        logger.log_text(f'file has been moved')
    except Exception as e:
        logger.log_text(f"Job has been failed with error {e}")
        update_tracker_table(file_name, failed, f"Table load  is Incomplete")
        move_file(file_name, "Failed")
        return e
    return "Job execution has been completed"


def move_file(file_name, folder):

    bucket = storage_client.get_bucket(bucket_id)
    blob_old = bucket.blob(file_name)

    out = storage_client.get_bucket(output_bucket)

    blob_new = bucket.copy_blob(
        blob_old, out, new_name=f'{folder}/{file_name}')

    blob_old.delete()


def update_tracker_table(file_name, status, stage):

    query = f"SELECT * FROM {dataset_id}.{etl_tracker} WHERE file_name='{file_name}';"

    rows = pd.read_gbq(query, credentials=credentials)

    if rows.empty:

        query = f"INSERT INTO {dataset_id}.{etl_tracker} (file_name,status,stage) VALUES ('{file_name}','{status}','{stage}');"

        job = bq_client.query(query).result()

    query = f"UPDATE {dataset_id}.{etl_tracker} SET status='{status}',stage='{stage}' WHERE file_name='{file_name}' ;"

    bq_client.query(query).result()


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


def mask_sensitive_data(str):

    try:

        item = {"value": str}

        # Get your project ID from the config file

        inspect_config = {
            "info_types": [
                {"name": "PHONE_NUMBER"},
                {"name": "EMAIL_ADDRESS"},

            ]
        }

    # Set the parent value for your request
        parent = f"projects/{project_id}"

        # Create the inspect request
        request = {
            "inspect_config": inspect_config,
            "item": item,
            "parent": parent

        }

        response = dlp_client.inspect_content(request=request)

        if response.result.findings:

            masked_text = response.result.findings[0].info_type.name

            return masked_text
        else:

            return str

    except Exception as e:
        logger.log_text(f"Job has been failed with error {e}")
        return e


def process_start(bucket_name, file_name):

    try:

        print(file_name)

        query = f"SELECT * FROM {dataset_id}.{etl_tracker} WHERE file_name='{file_name}' AND status ='{success}';"

        rows = pd.read_gbq(query, credentials=credentials)

        if not rows.empty:

            message = f"Hello, {file_name} is a duplicate file moving this file to duplicate folder, Pipeline Execution Aborted "
            send_email(sender_email, sender_password,
                       receiver_email, subject, message)
            move_file(file_name, 'Duplicate')

            logger.log_text(
                f"{file_name} is a duplicate file exiting the process")
            return "Received duplicate file so Job Exited"

        message = f"Hello, {file_name} has received into bucket Job is starting"
        send_email(sender_email, sender_password,
                   receiver_email, subject, message)

        read_file_from_storage(bucket_name, file_name)

    except Exception as e:
        logger.log_text(f"Job has been failed with error {e}")
        move_file(file_name, "Failed")

        return e
