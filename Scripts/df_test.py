import base64
from config import project_id, file_name, etl_tracker, dataset_id, table_id, bucket_id, output_bucket, sender_email, sender_password, receiver_email, subject, sm_server, port, json
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
from google.cloud import storage, bigquery, logging, dlp
from datetime import datetime
import io
import pandas as pd
from google.cloud import storage, bigquery
from config import json
from google.oauth2 import service_account
import time
credentials = service_account.Credentials.from_service_account_file(json)

storage_client = storage.Client.from_service_account_json(json)
# dlp_client = dlp.DlpServiceClient.from_service_account_file(json)
# bq_client = bigquery.Client.from_service_account_json(json)
# client = logging.Client.from_service_account_json(json)
# logger = client.logger(log_name)
# logger.log_text("Log Entry")
# credentials = service_account.Credentials.from_service_account_file(json)

try:
    bq_client = bigquery.Client.from_service_account_json(json)

    query = f"select * from {project_id}.{dataset_id}.bio;"

    gbq_df = pd.read_gbq(query, credentials=credentials)
    gbq_df = gbq_df.reset_index(drop=True)

    df = pd.read_excel('Sample5.xlsx')
    df = df.astype(str)
    matched = gbq_df['Id'].isin(df['Id'])
    new_df = gbq_df[matched]
    print(new_df['Id'])
    # columns = df.columns.to_list()
    # print("columns ")

    # df_excel_sorted = df.sort_values(by=['Id']).reset_index(drop=True)
    # df_bigquery_sorted = gbq_df.sort_values(by=['Id']).reset_index(drop=True)
    # columns_to_compare = columns

    # df_excel_selected = df_excel_sorted[columns_to_compare]
    # df_bigquery_selected = df_bigquery_sorted[columns_to_compare]
    # df_excel_selected = df_excel_selected.astype(str)

    # print()
    # print(df_bigquery_selected.dtypes.to_list)

    # print(gbq_df[columns_to_compare])
    # print()
    # print(df[columns_to_compare])
    # print(df_bigquery_selected.compare(df_excel_selected).shape[0])

except Exception as e:

    print(e)

# df_excel_set = set(tuple(row) for _, row in df_excel_selected.iterrows())
# df_bigquery_set = set(tuple(row) for _, row in df_bigquery_selected.iterrows())

# if df_excel_set == df_bigquery_set:
#     print("The Excel file and BigQuery table have the same data.")
# else:
#     print("The Excel file and BigQuery table have different data.")

# gbq_json = gbq_df.to_json('test.json')
# name = str(datetime.now().strftime('%Y%m%d-%H%M%S'))
# file_name = f"{name}.xlsx"
# print(file_name)
# df = pd.read_json('test.json')
# with pd.ExcelWriter(file_name, engine='xlsxwriter') as writer:
#     df.to_excel(writer, index=False)

# bucket_name = 'pubsub_trigger'
# bucket = storage_client.get_bucket(bucket_name)
# blob = bucket.blob(file_name)
# # blob.up(file_name)
# print("file Upload Completed")


def hello_pubsub(event, context):

    try:
        pubsub_message = base64.b64decode(event['data']).decode('utf-8')

        if pubsub_message.startswith('gs://'):

            print(f"Bucket : {pubsub_message}")
        else:

            json_df = pd.read_json(pubsub_message)

            print(json_df)

    except Exception as e:

        print(e)


# def send_email():

#     try:
#         smtp_server = sm_server
#         smtp_port = port

#         msg = MIMEMultipart()
#         msg["From"] = sender_email
#         msg["To"] = receiver_email
#         msg["Subject"] = subject

#         body = f"""
#         <html>
#         <body>
#             <p>file has already processed. Moving to Duplicate folder</p>
#             <p>Please check "{dataset_id}.{etl_tracker}" table for more details</p>
#             <table>
#                 <tr>
#                     <th>file path</th>
#                 </tr>
#                 <tr>
#                     <td>{file_path}</td>
#                 </tr>
#             </table>
#         </body>
#         </html>
#         """

#         msg = MIMEMultipart('alternative')
#         msg.attach(MIMEText(body, 'html'))
#         with smtplib.SMTP('smtp.gmail.com', 587) as server:
#         server.starttls()
#         server.login(sender_email, 'your_password')  # Replace with your email password
#         server.sendmail(sender_email, receiver_email, message.as_string())

#     print('Email sent successfully!')


# send_email()
# print("mail sent")


def send_email(sender_email, receiver_email, sender_password):
    # Email content
    sender_email = sender_email
    receiver_email = receiver_email
    subject = 'Example Email with Embedded Link'
    # pubsub_trigger/Sample10.xlsx

    body = f"""
        <html>
        <body>
            Hi, 

            <p>file has already processed. Moving to Duplicate folder</p>
            <p>Please check "{dataset_id}.{etl_tracker}" table for more details</p>
             <p></p> 
             <table class="center">
                <tr>
                    <th>File Path : </th>
                    <td><a href="https://storage.cloud.google.com/{bucket_id}{file_name}">{file_path}</a></td>
                </tr>
               
            </table>

        </body>
        </html>
        """

    # Create a multipart message
    message = MIMEMultipart('alternative')
    message['From'] = sender_email
    message['To'] = receiver_email
    message['Subject'] = subject

    # Attach the HTML content
    message.attach(MIMEText(body, 'html'))

    # Connect to the SMTP server and send the email
    with smtplib.SMTP('smtp.gmail.com', 587) as server:
        server.starttls()
        # Replace with your email password
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, receiver_email, message.as_string())

    print('Email sent successfully!')


# Call the function to send the email
# send_email(sender_email, receiver_email, sender_password)


# file_storage = pd.read_excel(
#     "Planner_Search_Political Events.xlsx")
# csv_file = pd.read_csv('homes.csv')
# sample = csv_file._append(file_storage, ignore_index=True)
# columns = sample.columns.tolist()
# schema = []
# for column in columns:
#     col = {'name': f'{column}', 'type': 'STRING'}
#     schema.append(col)
# print(sample)
credentials = service_account.Credentials.from_service_account_file(json)


def read_data_from_bigquery():

    query = f"select * from {project_id}.{dataset_id}.{table_id};"

    table_ref = bq_client.dataset(dataset_id).table(table_id)
    table = bq_client.get_table(table_ref)

    new_schema = table.schema

    gbq_df = pd.read_gbq(query, credentials=credentials)
    file_name = 'file_example_XLS_50.xls'
    file_path = f"gs://{bucket_id}/{file_name}"
    storage_df = pd.read_excel(file_path)

    storage_columns = set(storage_df.columns)
    gbq_columns = set(gbq_df.columns)

    if 'Id' not in storage_columns:
        return "required Column is missing"

    new_columns = storage_columns - gbq_columns

    types = storage_df.dtypes.to_dict()
    # print(gbq_columns)
    # print(new_columns)
    for column, type in types.copy().items():

        if column not in new_columns:
            del types[column]
    # print(len(types))
    # print(types)
    for column, type in types.items():
        # print(column, type)

        if type == 'int64':
            type = 'INT64'

        elif type == 'float64':
            type = 'FLOAT64'

        elif type == 'bool':
            type = 'BOOLEAN'

        else:
            type = 'STRING'
        # print(column, type)
        new_schema.append(bigquery.SchemaField(column, type))

    table.schema = new_schema
    bq_client.update_table(table, ["schema"])

    tmp_schema = []
    for field in new_schema:

        col = {'name': f'{field.name}', 'type': f'{field.field_type}'}

        tmp_schema.append(col)

    temp_table = 'temp'
    tamep_table_ref = bq_client.dataset(dataset_id).table(temp_table)
    temp = bigquery.Table(tamep_table_ref, schema=new_schema)
    bq_client.delete_table(temp, not_found_ok=True)
    temp = bq_client.create_table(temp)
    # print(storage_df)
    storage_df.to_gbq(f'{temp}', table_schema=tmp_schema, if_exists='replace',
                      credentials=credentials)

    source_list = storage_df.columns.to_list()
    comma_seperated = ','.join(source_list)
    # print(comma_seperated)
    temp_data = []
    for column in source_list:
        st = f'target.{column} = source.{column}'
        temp_data.append(st)

    assign = ','.join(temp_data)
    # print("")
    # print(assign)

    # print(" ")

    merge_query = f""" 

            MERGE `{project_id}.{dataset_id}.{table_id}` AS target
        USING (
            SELECT *
            FROM `{project_id}.{dataset_id}.temp`
        ) AS source
        ON target.Id = source.Id
        WHEN MATCHED THEN
            UPDATE SET {assign}
        WHEN NOT MATCHED THEN
            INSERT ({comma_seperated}) Values({comma_seperated});

    
    
    """

    print(merge_query)

    job = bq_client.query(merge_query)
    print(job.result())

    bq_client.delete_table(tamep_table_ref, not_found_ok=True)
    print("temp table is removed")


# print(read_data_from_bigquery())

#  MERGE `flawless-will-388608.etl.bio` AS target
#  USING (
#      SELECT *
#      FROM `flawless-will-388608.etl.temp`
#  ) AS source
#  ON target.Id = source.Id
#  WHEN MATCHED THEN
#      UPDATE SET target.Gender = source.Gender, target.Country=source.Country, target.First_name = Source.First_name, target.Age = source.Age, target.Id = source.Id, target.status = source.status, target.Summary = source.Summary, target.Last_name = source.Last_name
#  WHEN NOT MATCHED THEN
#      INSERT (Gender,Country,Age,Date,Id,status,Summary,First_Name,Last_name) Values(Gender,Country,Age,Date,Id,status,Summary,First_Name,Last_name)


# def upsert():


#     table_ref = bq_client.dataset(dataset_id).table('temp')


#     columns = merged.columns.tolist()
#     schema = []

#     for column in columns:

#         col = {'name': f'{column}', 'type': 'STRING'}

#         schema.append(col)

#         #merged.fillna('None')
#         merged = merged.astype(str)

#         merged.to_gbq(f'{dataset_name}.{table_name}', if_exists='replace',
#                     table_schema=schema, credentials=credentials)
#         logger.log_text(f"Table Load Completed")

#     # Perform the upsert operation from the temporary table to the final table
#     merge_query = f"""
#         MERGE `{project_id}.{dataset_id}.{table_id}` AS target
#         USING (
#             SELECT *
#             FROM `{project_id}.{dataset_id}.TEMP_TABLE_NAME`
#         ) AS source
#         ON target.primary_key_column = source.primary_key_column
#         WHEN MATCHED THEN
#             UPDATE SET *
#         WHEN NOT MATCHED THEN
#             INSERT ROW
#     """

#     query_job = bigquery_client.query(merge_query)
#     query_job.result()  # Wait for the upsert query to complete

#     # Delete the temporary table
#     temp_table_ref = bigquery_client.dataset(dataset_id).table("TEMP_TABLE_NAME")
#     bigquery_client.delete_table(temp_table_ref, not_found_ok=True)

#     print("Upsert operation completed successfully.")


###############


# log_name = f"{datetime.now().strftime('logfile_%H_%M_%d_%m_%Y.log')}"
# print(log_name)


# storage_client = storage.Client.from_service_account_json(json)
# dlp_client = dlp.DlpServiceClient.from_service_account_file(json)
# bq_client = bigquery.Client.from_service_account_json(json)
# client = logging.Client.from_service_account_json(json)
# logger = client.logger(log_name)
# logger.log_text("Log Entry")
# credentials = service_account.Credentials.from_service_account_file(json)


# def read_file_from_storage(bucket_name, file_name):

#     logger.log_text(f"Reading file {file_name}")

#     file_path = f"gs://{bucket_name}/{file_name}"

#     try:
#         storage_df = pd.read_excel(file_path)
#         storage_df = storage_df.applymap(str)
#         storage_df = storage_df.applymap(mask_sensitive_data)
#         logger.log_text(f"File Reading Completed")
#     except e:
#         logger.log_text("Job has been failed with error ", str(e))
#         move_file(file_name, "Failed")
#         return e
#     return read_data_from_bigquery(project_id, table_id, dataset_id, storage_df, file_name)


# def read_data_from_bigquery(project_id, table_id, dataset, storage_df, file_name):

#     logger.log_text(f"Reading gbq table {table_id}")

#     query = f"select * from {project_id}.{dataset}.{table_id}"

#     try:

#         gbq_df = pd.read_gbq(query, credentials=credentials)
#         # print(gbq_df)
#         logger.log_text(f"Table Reading Completed")
#     except e:
#         logger.log_text("Job has been failed with error ", str(e))
#         move_file(file_name, "Failed")
#         return e
#     return load_data_gbq_table(storage_df, gbq_df, table_id, dataset, file_name)


# def load_data_gbq_table(storage_df, gbq_df, table_name, dataset_name, file_name):

#     logger.log_text(f"Table Load is Starting")

#     try:
#         merged = storage_df._append(gbq_df, ignore_index=True)

#         columns = merged.columns.tolist()
#         schema = []

#         for column in columns:

#             col = {'name': f'{column}', 'type': 'STRING'}

#             schema.append(col)

#         # merged.fillna('None')
#         merged = merged.astype(str)

#         merged.to_gbq(f'{dataset_name}.{table_name}', if_exists='replace',
#                       table_schema=schema, credentials=credentials)
#         logger.log_text(f"Table Load Completed")

#         message = f"Hello, {file_name} has been loaded"
#         send_email(sender_email, sender_password,
#                    receiver_email, subject, message)

#         move_file(file_name, "Success")

#         logger.log_text(f'file has been moved')
#     except e:
#         logger.log_text("Job has been failed with error ", str(e))
#         move_file(file_name, "Failed")
#         return e
#     return "Job execution has been completed"


# def move_file(file_name, folder):

#     bucket = storage_client.get_bucket(bucket_id)
#     blob_old = bucket.blob(file_name)

#     out = storage_client.get_bucket(output_bucket)

#     blob_new = bucket.copy_blob(
#         blob_old, out, new_name=f'{folder}/{file_name}')

#     blob_old.delete()


# def send_email(sender_email, sender_password, receiver_email, subject, message):

#     smtp_server = sm_server
#     smtp_port = port

#     msg = MIMEMultipart()
#     msg["From"] = sender_email
#     msg["To"] = receiver_email
#     msg["Subject"] = subject

#     msg.attach(MIMEText(message, "plain"))

#     try:

#         server = smtplib.SMTP(smtp_server, smtp_port)
#         server.starttls()
#         server.login(sender_email, sender_password)

#         server.send_message(msg)

#         server.quit()
#         print("Email notification sent successfully!")
#     except Exception as e:
#         print("An error occurred while sending the email:", str(e))


# def mask_sensitive_data(str):

#     try:

#         item = {"value": str}

#         # Get your project ID from the config file

#         inspect_config = {
#             "info_types": [
#                 {"name": "PHONE_NUMBER"},
#                 {"name": "EMAIL_ADDRESS"},

#             ]
#         }

#     # Set the parent value for your request
#         parent = f"projects/{project_id}"

#         # Create the inspect request
#         request = {
#             "inspect_config": inspect_config,
#             "item": item,
#             "parent": parent

#         }

#         response = dlp_client.inspect_content(request=request)

#         if response.result.findings:

#             masked_text = response.result.findings[0].info_type.name

#             return masked_text
#         else:

#             return str

#     except e:
#         logger.log_text("Job has been failed with error ", str(e))
#         return e


# def process_start(event, context):

#     try:

#         bucket_name = event['bucket']
#         file_name = event['name']
#         print(file_name)

#         message = f"Hello, {file_name} has received into bucket Job is starting"
#         send_email(sender_email, sender_password,
#                    receiver_email, subject, message)

#         read_file_from_storage(bucket_name, file_name)

#     except e:
#         logger.log_text("Job has been failed with error ", str(e))
#         move_file(file_name, "Failed")

#         return e


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


################# #*************************************** ################


# log_name = f"{datetime.now().strftime('logfile_%H_%M_%d_%m_%Y.log')}"
# print(log_name)


# storage_client = storage.Client.from_service_account_json(json)
# dlp_client = dlp.DlpServiceClient.from_service_account_file(json)
# bq_client = bigquery.Client.from_service_account_json(json)
# client = logging.Client.from_service_account_json(json)
# # logger = client.logger(log_name)
# logger.log_text("Log Entry")
# credentials = service_account.Credentials.from_service_account_file(json)


# def read_file_from_storage(bucket_name, file_name):

#     logger.log_text(f"Reading file {file_name}")

#     file_path = f"gs://{bucket_name}/{file_name}"

#     try:
#         storage_df = pd.read_excel(file_path)
#         storage_df = storage_df.applymap(str)
#         storage_df = storage_df.applymap(mask_sensitive_data)
#         logger.log_text(f"File Reading Completed")
#         update_tracker_table(file_name, progress, "File Reading completed")

#     except Exception as e:
#         logger.log_text(f"Job has been failed with error {e}")
#         update_tracker_table(file_name, failed, "Error in file reading")
#         move_file(file_name, "Failed")
#         file_path = f"gs://{output_bucket}/failed/{file_name}"
#         rows_processed = 0
#         send_email(sender_email, sender_password, receiver_email,
#                    subject, failed, file_path, rows_processed)
#         return "Job Exited with error"
#     return read_data_from_bigquery(project_id, table_id, dataset_id, storage_df, file_name)


# def read_data_from_bigquery(project_id, table_id, dataset, storage_df, file_name):

#     logger.log_text(f"Reading gbq table {table_id}")

#     query = f"select * from {project_id}.{dataset}.{table_id}"

#     try:

#         gbq_df = pd.read_gbq(query, credentials=credentials)
#         # print(gbq_df)
#         logger.log_text(f"Table Reading Completed")
#         update_tracker_table(file_name, progress,
#                              f"Table {table_id} to dataframe load completed")
#     except Exception as e:
#         logger.log_text(f"Job has been failed with error {e}")
#         update_tracker_table(
#             file_name, failed, f"Error in reading Table {table_id} ")
#         move_file(file_name, "Failed")
#         file_path = f"gs://{output_bucket}/failed/{file_name}"
#         rows_processed = 0
#         send_email(sender_email, sender_password, receiver_email,
#                    subject, failed, file_path, rows_processed)
#         return e
#     return load_data_gbq_table(storage_df, gbq_df, table_id, dataset, file_name)


# def load_data_gbq_table(storage_df, gbq_df, table_name, dataset_name, file_name):

#     logger.log_text(f"Table Load is Starting")

#     try:
#         merged = storage_df._append(gbq_df, ignore_index=True)

#         columns = merged.columns.tolist()
#         schema = []

#         for column in columns:

#             col = {'name': f'{column}', 'type': 'STRING'}

#             schema.append(col)

#         # merged.fillna('None')
#         merged = merged.astype(str)

#         merged.to_gbq(f'{dataset_name}.{table_name}', if_exists='replace',
#                       table_schema=schema, credentials=credentials)
#         logger.log_text(f"Table Load Completed")

#         update_tracker_table(file_name, success,
#                              f"file {file_name} loaded to table {table_name}")
#         move_file(file_name, "Success")
#         file_path = f"gs://{output_bucket}/Success/{file_name}"
#         rows_processed = gbq_df.shape[0]
#         send_email(sender_email, sender_password, receiver_email,
#                    subject, failed, file_path, rows_processed)

#         logger.log_text(f'file has been moved')
#         return "Job execution has been completed"
#     except Exception as e:
#         logger.log_text(f"Job has been failed with error {e}")
#         update_tracker_table(file_name, failed, f"Table load  is Incomplete")
#         move_file(file_name, "Failed")
#         file_path = f"gs://{output_bucket}/failed/{file_name}"
#         rows_processed = 0
#         send_email(sender_email, sender_password, receiver_email,
#                    subject, failed, file_path, rows_processed)
#         return e


# def move_file(file_name, folder):

#     bucket = storage_client.get_bucket(bucket_id)
#     blob_old = bucket.blob(file_name)

#     out = storage_client.get_bucket(output_bucket)

#     blob_new = bucket.copy_blob(
#         blob_old, out, new_name=f'{folder}/{file_name}')

#     blob_old.delete()


# def update_tracker_table(file_name, status, stage):

#     query = f"SELECT * FROM {dataset_id}.{etl_tracker} WHERE file_name='{file_name}';"

#     rows = pd.read_gbq(query, credentials=credentials)

#     if rows.empty:

#         query = f"INSERT INTO {dataset_id}.{etl_tracker} (file_name,status,stage) VALUES ('{file_name}','{status}','{stage}');"

#         job = bq_client.query(query).result()

#     query = f"UPDATE {dataset_id}.{etl_tracker} SET status='{status}',stage='{stage}' WHERE file_name='{file_name}' ;"

#     bq_client.query(query).result()


# def send_email(sender_email, sender_password, receiver_email, subject, status, file_path, number_of_rows_processed):

#     smtp_server = sm_server
#     smtp_port = port

#     msg = MIMEMultipart()
#     msg["From"] = sender_email
#     msg["To"] = receiver_email
#     msg["Subject"] = subject

#     if status == 'Duplicate':

#         message = f"""Hi,

#         file has already processed. Moving to Duplicate folder
#         Please check "{dataset_id}.{etl_tracker}" table for more details

#         file path : {file_path}

#         Thanks """
#     if status == 'file received':

#         message = f"""Hi,

#         file received into the bucket, Job is starting. Please find the received file path

#         file path : {file_path}

#         Thanks """

#     else:
#         message = f"""Hi,

#             Job {status} Please find the status and file path below.
#             Please check "{dataset_id}.{etl_tracker}" table for more details

#             File Path : {file_path},
#             Rows Processed : {number_of_rows_processed}


#             Thanks
#             """

#     msg.attach(MIMEText(message, "plain"))

#     try:

#         server = smtplib.SMTP(smtp_server, smtp_port)
#         server.starttls()
#         server.login(sender_email, sender_password)

#         server.send_message(msg)

#         server.quit()
#         print("Email notification sent successfully!")
#     except Exception as e:
#         print("An error occurred while sending the email:", str(e))


# def mask_sensitive_data(str):

#     try:

#         item = {"value": str}

#         # Get your project ID from the config file

#         inspect_config = {
#             "info_types": [
#                 {"name": "PHONE_NUMBER"},
#                 {"name": "EMAIL_ADDRESS"},

#             ]
#         }

#     # Set the parent value for your request
#         parent = f"projects/{project_id}"

#         # Create the inspect request
#         request = {
#             "inspect_config": inspect_config,
#             "item": item,
#             "parent": parent

#         }

#         response = dlp_client.inspect_content(request=request)

#         if response.result.findings:

#             masked_text = response.result.findings[0].info_type.name

#             return masked_text
#         else:

#             return str

#     except Exception as e:
#         logger.log_text(f"Job has been failed with error {e}")
#         return e


# def process_start(event, context):

#     try:

#         bucket_name = event['bucket']
#         file_name = event['name']
#         print(file_name)

#         query = f"SELECT * FROM {dataset_id}.{etl_tracker} WHERE file_name='{file_name}' AND status ='{success}';"

#         rows = pd.read_gbq(query, credentials=credentials)

#         if not rows.empty:

#             move_file(file_name, 'Duplicate')
#             file_path = f"gs://{output_bucket}/Duplicate/{file_name}"
#             send_email(sender_email, sender_password, receiver_email,
#                        subject, 'Duplicate', file_path, 0)

#             logger.log_text(
#                 f"{file_name} is a duplicate file exiting the process")
#             return "Received duplicate file so Job Exited"

#         message = f"Hello, {file_name} has received into bucket Job is starting"
#         file_path = f"gs://{bucket_name}/{file_name}"
#         send_email(sender_email, sender_password, receiver_email,
#                    subject, 'file received', file_path, 0)

#         read_file_from_storage(bucket_name, file_name)

#     except Exception as e:
#         logger.log_text(f"Job has been failed with error {e}")
#         move_file(file_name, "Failed")
#         file_path = f"gs://{output_bucket}/Failed/{file_name}"
#         send_email(sender_email, sender_password, receiver_email,
#                    subject, 'Failed', file_path, 0)

#         return e


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


################# *************** ############## ************ ##########


# log_name = f"{datetime.now().strftime('logfile_%H_%M_%d_%m_%Y.log')}"
# print(log_name)


# storage_client = storage.Client.from_service_account_json(json)
# dlp_client = dlp.DlpServiceClient.from_service_account_file(json)
# bq_client = bigquery.Client.from_service_account_json(json)
# client = logging.Client.from_service_account_json(json)
# logger = client.logger(log_name)
# logger.log_text("Log Entry")
# credentials = service_account.Credentials.from_service_account_file(json)


# def hello_pubsub(event, context):

#     try:

#         pubsub_message = str(base64.b64decode(event['data']).decode('utf-8'))
#         logger.log_text(f'message : {pubsub_message}')

#         bucket = pubsub_message.split("/")[2]
#         logger.log_text('bucket : {bucket}')

#         file_name = pubsub_message.split("/")[-1]
#         logger.log_text(f'file name : {file_name}')
#         bucket_name = storage_client.get_bucket(bucket)
#         print(bucket_name)
#         blob = bucket_name.blob(file_name)
#         if not blob.exists():
#             logger.log_text(f"{file_name} not available in {bucket}")
#             file_path = pubsub_message
#             send_email(sender_email, sender_password, receiver_email,
#                        subject, 'File Unavailable', file_path, 0)
#             return "process exited due to file unavailability"

#         return process_start(bucket, file_name)

#     except Exception as e:
#         logger.log_text(f"Error in storage path parsing {e}")
#         update_tracker_table(
#             file_name, failed, "Error in storage path parsing")
#         move_file(file_name, "Failed")
#         file_path = f"gs://{output_bucket}/Failed/{file_name}"
#         send_email(sender_email, sender_password, receiver_email,
#                    subject, 'Failed', file_path, 0)
#         return "Job Exited with error"


# def read_file_from_storage(bucket_name, file_name):

#     logger.log_text(f"Reading file {file_name}")

#     file_path = f"gs://{bucket_name}/{file_name}"

#     try:
#         storage_df = pd.read_excel(file_path)
#         storage_df = storage_df.applymap(str)
#         storage_df = storage_df.applymap(mask_sensitive_data)
#         logger.log_text(f"File Reading Completed")
#         update_tracker_table(file_name, progress, "File Reading completed")
#         return read_data_from_bigquery(project_id, table_id, dataset_id, storage_df, file_name)

#     except Exception as e:
#         logger.log_text(f"Job has been failed with error {e}")
#         update_tracker_table(file_name, failed, "Error in file reading")
#         move_file(file_name, "Failed")
#         file_path = f"gs://{output_bucket}/failed/{file_name}"
#         rows_processed = 0
#         send_email(sender_email, sender_password, receiver_email,
#                    subject, failed, file_path, rows_processed)
#         return "Job Exited with error"


# def read_data_from_bigquery(project_id, table_id, dataset, storage_df, file_name):

#     logger.log_text(f"Reading gbq table {table_id}")

#     query = f"select * from {project_id}.{dataset}.{table_id}"

#     try:

#         gbq_df = pd.read_gbq(query, credentials=credentials)
#         # print(gbq_df)
#         logger.log_text(f"Table Reading Completed")
#         update_tracker_table(file_name, progress,
#                              f"Table {table_id} to dataframe load completed")
#         return load_data_gbq_table(storage_df, gbq_df, table_id, dataset, file_name)
#     except Exception as e:
#         logger.log_text(f"Job has been failed with error {e}")
#         update_tracker_table(
#             file_name, failed, f"Error in reading Table {table_id} ")
#         move_file(file_name, "Failed")
#         file_path = f"gs://{output_bucket}/failed/{file_name}"
#         rows_processed = 0
#         send_email(sender_email, sender_password, receiver_email,
#                    subject, failed, file_path, rows_processed)
#         return e


# def load_data_gbq_table(storage_df, gbq_df, table_name, dataset_name, file_name):

#     logger.log_text(f"Table Load is Starting")

#     try:
#         merged = storage_df._append(gbq_df, ignore_index=True)

#         columns = merged.columns.tolist()
#         schema = []

#         for column in columns:

#             col = {'name': f'{column}', 'type': 'STRING'}

#             schema.append(col)

#         # merged.fillna('None')
#         merged = merged.astype(str)

#         merged.to_gbq(f'{dataset_name}.{table_name}', if_exists='replace',
#                       table_schema=schema, credentials=credentials)
#         logger.log_text(f"Table Load Completed")

#         update_tracker_table(file_name, success,
#                              f"file {file_name} loaded to table {table_name}")
#         move_file(file_name, "Success")
#         file_path = f"gs://{output_bucket}/Success/{file_name}"
#         rows_processed = storage_df.shape[0]
#         send_email(sender_email, sender_password, receiver_email,
#                    subject, success, file_path, rows_processed)

#         logger.log_text(f'file has been moved')
#         return "Job execution has been completed"
#     except Exception as e:
#         logger.log_text(f"Job has been failed with error {e}")
#         update_tracker_table(file_name, failed, f"Table load  is Incomplete")
#         move_file(file_name, "Failed")
#         file_path = f"gs://{output_bucket}/failed/{file_name}"
#         rows_processed = 0
#         send_email(sender_email, sender_password, receiver_email,
#                    subject, failed, file_path, rows_processed)
#         return e


# def move_file(file_name, folder):

#     bucket = storage_client.get_bucket(bucket_id)
#     blob_old = bucket.blob(file_name)

#     out = storage_client.get_bucket(output_bucket)

#     blob_new = bucket.copy_blob(
#         blob_old, out, new_name=f'{folder}/{file_name}')

#     blob_old.delete()


# def update_tracker_table(file_name, status, stage):

#     query = f"SELECT * FROM {dataset_id}.{etl_tracker} WHERE file_name='{file_name}';"

#     rows = pd.read_gbq(query, credentials=credentials)

#     if rows.empty:

#         query = f"INSERT INTO {dataset_id}.{etl_tracker} (file_name,status,stage) VALUES ('{file_name}','{status}','{stage}');"

#         job = bq_client.query(query).result()

#     query = f"UPDATE {dataset_id}.{etl_tracker} SET status='{status}',stage='{stage}' WHERE file_name='{file_name}' ;"

#     bq_client.query(query).result()


# def send_email(sender_email, sender_password, receiver_email, subject, status, file_path, number_of_rows_processed):

#     smtp_server = sm_server
#     smtp_port = port

#     msg = MIMEMultipart()
#     msg["From"] = sender_email
#     msg["To"] = receiver_email
#     msg["Subject"] = subject

#     if status == 'Duplicate':

#         message = f"""Hi,

#         file has already processed. Moving to Duplicate folder
#         Please check "{dataset_id}.{etl_tracker}" table for more details

#         file path : {file_path}

#         Thanks """
#     if status == 'file received':

#         message = f"""Hi,

#         file received into the bucket, Job is starting. Please find the received file path

#         file path : {file_path}

#         Thanks """

#     if status == 'File Unavailable':
#         message = f""" Hi,

#             The file "{file_path}" is not available in the bucket. Job exited

#             Thanks """
#     else:
#         message = f"""Hi,

#             Job {status} Please find the status and file path below.
#             Please check "{dataset_id}.{etl_tracker}" table for more details

#             File Path : {file_path},
#             Rows Processed : {number_of_rows_processed}


#             Thanks
#             """

#     msg.attach(MIMEText(message, "plain"))

#     try:

#         server = smtplib.SMTP(smtp_server, smtp_port)
#         server.starttls()
#         server.login(sender_email, sender_password)

#         server.send_message(msg)

#         server.quit()
#         print("Email notification sent successfully!")
#     except Exception as e:
#         print("An error occurred while sending the email:", str(e))


# def mask_sensitive_data(str):

#     try:

#         item = {"value": str}

#         # Get your project ID from the config file

#         inspect_config = {
#             "info_types": [
#                 {"name": "PHONE_NUMBER"},
#                 {"name": "EMAIL_ADDRESS"},

#             ]
#         }

#     # Set the parent value for your request
#         parent = f"projects/{project_id}"

#         # Create the inspect request
#         request = {
#             "inspect_config": inspect_config,
#             "item": item,
#             "parent": parent

#         }

#         response = dlp_client.inspect_content(request=request)

#         if response.result.findings:

#             masked_text = response.result.findings[0].info_type.name

#             return masked_text
#         else:

#             return str

#     except Exception as e:
#         logger.log_text(f"Job has been failed with error {e}")
#         return e


# def process_start(bucket_name, file_name):

#     try:

#         print(file_name)

#         query = f"SELECT * FROM {dataset_id}.{etl_tracker} WHERE file_name='{file_name}' AND status ='{success}';"

#         rows = pd.read_gbq(query, credentials=credentials)

#         if not rows.empty:

#             move_file(file_name, 'Duplicate')

#             file_path = f"gs://{output_bucket}/Duplicate/{file_name}"
#             send_email(sender_email, sender_password, receiver_email,
#                        subject, 'Duplicate', file_path, 0)

#             logger.log_text(
#                 f"{file_name} is a duplicate file exiting the process")
#             return "Received duplicate file so Job Exited"

#         file_path = f"gs://{bucket_name}/{file_name}"
#         send_email(sender_email, sender_password, receiver_email,
#                    subject, 'file received', file_path, 0)

#         read_file_from_storage(bucket_name, file_name)

#     except Exception as e:
#         logger.log_text(f"Job has been failed with error {e}")
#         move_file(file_name, "Failed")
#         file_path = f"gs://{output_bucket}/Failed/{file_name}"
#         send_email(sender_email, sender_password, receiver_email,
#                    subject, 'Failed', file_path, 0)

#         return e


####### ************* ############ ************ #############


log_name = f"{datetime.now().strftime('logfile_%H_%M_%d_%m_%Y.log')}"
print(log_name)

start_time = ''
end_time = ''
duration = ''
file_upload = ''


storage_client = storage.Client.from_service_account_json(json)
dlp_client = dlp.DlpServiceClient.from_service_account_file(json)
bq_client = bigquery.Client.from_service_account_json(json)
client = logging.Client.from_service_account_json(json)
logger = client.logger(log_name)
logger.log_text("Log Entry")
credentials = service_account.Credentials.from_service_account_file(json)


def read_file_from_storage(bucket_name, file_name):

    logger.log_text(f"Reading file {file_name}")

    file_path = f"gs://{bucket_name}/{file_name}"

    try:
        storage_df = pd.read_excel(file_path)
        storage_columns = set(storage_df.columns)
        if mandatory_column not in storage_columns:

            file_path = f"gs://{output_bucket}/failed/{file_name}"
            rows_processed = 0
            send_email(sender_email, sender_password, receiver_email,
                       subject, failed, file_path, rows_processed)
            move_file(file_name, "Insufficient_Data")
            end_time = datetime.now()
            update_tracker_table(file_name, failed, "Mandatory Column is missing in file",
                                 start_time, end_time, file_upload, duration, process)
            return "Mandatory column is missing. Pipeline execution stopped"

        storage_df = storage_df.applymap(str)
        storage_df = storage_df.applymap(mask_sensitive_data)
        logger.log_text(f"File Reading Completed")
        end_time = datetime.now()
        update_tracker_table(file_name, failed, "File Reading completed",
                             start_time, end_time, file_upload, duration, process)
        # update_tracker_table(file_name,progress,"File Reading completed")
        return read_data_from_bigquery(project_id, table_id, dataset_id, storage_df, file_name)

    except Exception as e:
        logger.log_text(f"Job has been failed with error {e}")
        end_time = datetime.now()
        update_tracker_table(file_name, failed, "Error in file reading",
                             start_time, end_time, file_upload, duration, process)
        # update_tracker_table(file_name,failed, "Error in file reading")
        move_file(file_name, "Failed")
        file_path = f"gs://{output_bucket}/failed/{file_name}"
        rows_processed = 0
        send_email(sender_email, sender_password, receiver_email,
                   subject, failed, file_path, rows_processed)
        return "Job Exited with error"


def read_data_from_bigquery(project_id, table_id, dataset, storage_df, file_name):

    logger.log_text(f"Reading gbq table {table_id}")

    query = f"select * from {project_id}.{dataset}.{table_id}"

    try:

        gbq_df = pd.read_gbq(query, credentials=credentials)
        # print(gbq_df)
        logger.log_text(f"Table Reading Completed")
        end_time = datetime.now()
        update_tracker_table(
            file_name, failed, "Table {table_id} to dataframe load completed", start_time, end_time, file_upload, duration, process)
        return load_data_gbq_table(storage_df, gbq_df, table_id, dataset, file_name)
    except Exception as e:
        logger.log_text(f"Job has been failed with error {e}")
        end_time = datetime.now()
        update_tracker_table(
            file_name, failed, f"Error in reading Table {table_id} ", start_time, end_time, file_upload, duration, process)
        # update_tracker_table(file_name,failed,f"Error in reading Table {table_id} ")
        move_file(file_name, "Failed")
        file_path = f"gs://{output_bucket}/failed/{file_name}"
        rows_processed = 0
        send_email(sender_email, sender_password, receiver_email,
                   subject, failed, file_path, rows_processed)
        return e


def load_data_gbq_table(storage_df, gbq_df, table_name, dataset_name, file_name):

    logger.log_text(f"Table Load is Starting")

    try:
        table_ref = bq_client.dataset(dataset_id).table(table_id)
        table = bq_client.get_table(table_ref)

        new_schema = table.schema

        storage_columns = set(storage_df.columns)
        gbq_columns = set(gbq_df.columns)
        new_columns = storage_columns - gbq_columns

        types = storage_df.dtypes.to_dict()

        for column, type in types.copy().items():

            if column not in new_columns:
                del types[column]
        # print(len(types))
        # print(types)
        for column, type in types.items():
            # print(column, type)

            if type == 'int64':
                type = 'INT64'

            elif type == 'float64':
                type = 'FLOAT64'

            elif type == 'bool':
                type = 'BOOLEAN'

            else:
                type = 'STRING'
            # print(column, type)
            new_schema.append(bigquery.SchemaField(column, type))

        table.schema = new_schema
        bq_client.update_table(table, ["schema"])

        tmp_schema = []
        for field in new_schema:

            col = {'name': f'{field.name}', 'type': f'{field.field_type}'}

            tmp_schema.append(col)

        temp_table = 'temp'
        tamep_table_ref = bq_client.dataset(dataset_id).table(temp_table)
        temp = bigquery.Table(tamep_table_ref, schema=new_schema)
        bq_client.delete_table(temp, not_found_ok=True)
        temp = bq_client.create_table(temp)
        # print(storage_df)
        storage_df.to_gbq(f'{temp}', table_schema=tmp_schema, if_exists='replace',
                          credentials=credentials)

        source_list = storage_df.columns.to_list()
        comma_seperated = ','.join(source_list)
        # print(comma_seperated)
        temp_data = []
        for column in source_list:
            st = f'target.{column} = source.{column}'
            temp_data.append(st)

        assign = ','.join(temp_data)
        # print("")
        # print(assign)

        # print(" ")

        merge_query = f""" 

                MERGE `{project_id}.{dataset_id}.{table_id}` AS target
            USING (
                SELECT *
                FROM `{project_id}.{dataset_id}.temp`
            ) AS source
            ON target.Id = source.Id
            WHEN MATCHED THEN
                UPDATE SET {assign}
            WHEN NOT MATCHED THEN
                INSERT ({comma_seperated}) Values({comma_seperated});

        
        
        """

        print(merge_query)

        job = bq_client.query(merge_query)
        print(job.result())

        bq_client.delete_table(tamep_table_ref, not_found_ok=True)
        print("temp table is removed")

        end_time = datetime.now()
        update_tracker_table(
            file_name, failed, f"file {file_name} loaded to table {table_name}", start_time, end_time, file_upload, duration, process)
        # update_tracker_table(file_name,success,f"file {file_name} loaded to table {table_name}")
        move_file(file_name, "Success")
        file_path = f"gs://{output_bucket}/Success/{file_name}"
        rows_processed = storage_df.shape[0]
        send_email(sender_email, sender_password, receiver_email,
                   subject, success, file_path, rows_processed)

        logger.log_text(f'file has been moved')
        return "Job execution has been completed"
    except Exception as e:
        logger.log_text(f"Job has been failed with error {e}")
        end_time = datetime.now()
        update_tracker_table(file_name, failed, f"Table load  is Incomplete",
                             start_time, end_time, file_upload, duration, process)
        # update_tracker_table(file_name,failed,f"Table load  is Incomplete", )
        move_file(file_name, "Failed")
        file_path = f"gs://{output_bucket}/failed/{file_name}"
        rows_processed = 0
        send_email(sender_email, sender_password, receiver_email,
                   subject, failed, file_path, rows_processed)
        return e


def move_file(file_name, folder):

    bucket = storage_client.get_bucket(bucket_id)
    blob_old = bucket.blob(file_name)

    out = storage_client.get_bucket(output_bucket)

    blob_new = bucket.copy_blob(
        blob_old, out, new_name=f'{folder}/{file_name}')

    blob_old.delete()


def update_tracker_table(file_name, status, stage, start_time, end_time, file_upload, duration, process):

    query = f"SELECT * FROM {dataset_id}.{etl_tracker} WHERE file_name='{file_name}';"

    rows = pd.read_gbq(query, credentials=credentials)
    file_upload = start_time
    duration = (end_time - start_time).microseconds

    if rows.empty:

        query = f"INSERT INTO {dataset_id}.{etl_tracker} (file_name,status,stage,start_time,end_time,'duration in ms',upload_time,process) VALUES ('{file_name}','{status}','{stage}','{start_time}','{end_time}','{duration}','{file_upload}','{process}');"

        job = bq_client.query(query).result()

    query = f"UPDATE {dataset_id}.{etl_tracker} SET status='{status}',stage='{stage}',start_time='{start_time}',end_time='{end_time}','duration in ms'='{duration}',upload_time='{file_upload}',process='{process}'  WHERE file_name='{file_name}' ;"

    bq_client.query(query).result()


def send_email(sender_email, sender_password, receiver_email, subject, status, file_path, number_of_rows_processed):

    smtp_server = sm_server
    smtp_port = port

    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = receiver_email
    msg["Subject"] = subject

    if status == 'Duplicate':

        message = f"""Hi,

        file has already processed. Moving to Duplicate folder
        Please check "{dataset_id}.{etl_tracker}" table for more details

        file path : {file_path}

        Thanks """
    elif status == 'file received':

        message = f"""Hi,

        file received into the bucket, Job is starting. Please find the received file path

        file path : {file_path}

        Thanks """

    else:
        message = f"""Hi,

            Job {status} Please find the status and file path below. 
            Please check "{dataset_id}.{etl_tracker}" table for more details

            File Path : {file_path},
            Rows Processed : {number_of_rows_processed}
            

            Thanks
            """

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


def process_start(event, context):

    try:

        bucket_name = event['bucket']
        file_name = event['name']
        print(file_name)

        query = f"SELECT * FROM {dataset_id}.{etl_tracker} WHERE file_name='{file_name}' AND status ='{success}';"

        rows = pd.read_gbq(query, credentials=credentials)

        if not rows.empty:

            move_file(file_name, 'Duplicate')
            file_path = f"gs://{output_bucket}/Duplicate/{file_name}"
            send_email(sender_email, sender_password, receiver_email,
                       subject, 'Duplicate', file_path, 0)

            logger.log_text(
                f"{file_name} is a duplicate file exiting the process")
            return "Received duplicate file so Job Exited"

        message = f"Hello, {file_name} has received into bucket Job is starting"
        start_time = datetime.now()
        file_path = f"gs://{bucket_name}/{file_name}"
        send_email(sender_email, sender_password, receiver_email,
                   subject, 'file received', file_path, 0)

        read_file_from_storage(bucket_name, file_name)

    except Exception as e:
        logger.log_text(f"Job has been failed with error {e}")
        move_file(file_name, "Failed")
        file_path = f"gs://{output_bucket}/Failed/{file_name}"
        send_email(sender_email, sender_password, receiver_email,
                   subject, 'Failed', file_path, 0)

        return e


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


# **************** ##################### **************


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


####################### ******************* ####################


log_name = f"{datetime.now().strftime('logfile_%H_%M_%d_%m_%Y.log')}"
print(log_name)


storage_client = storage.Client.from_service_account_json(json)
dlp_client = dlp.DlpServiceClient.from_service_account_file(json)
bq_client = bigquery.Client.from_service_account_json(json)
client = logging.Client.from_service_account_json(json)
logger = client.logger(log_name)
logger.log_text("Log Entry")
credentials = service_account.Credentials.from_service_account_file(json)
file_upload = datetime.now()
start_time = datetime.now()
end_time = datetime.now()
duration = float((end_time - start_time).microseconds)


def hello_pubsub(event, context):

    try:

        pubsub_message = str(base64.b64decode(event['data']).decode('utf-8'))
        logger.log_text(f'message : {pubsub_message}')

        file_upload = context.timestamp

        if pubsub_message.startswith('gs://'):

            bucket = pubsub_message.split("/")[2]
            file_name = pubsub_message.split("/")[-1]
            logger.log_text('bucket : {bucket}')

            logger.log_text(f'file name : {file_name}')
            bucket_name = storage_client.get_bucket(bucket)
            print(bucket_name)
            print(file_name)
            blob = bucket_name.blob(file_name)
            if not blob.exists():
                logger.log_text(f"{file_name} not available in {bucket}")
                file_path = pubsub_message
                send_email(sender_email, sender_password, receiver_email,
                           subject, 'File Unavailable', file_path, 0)
                return "process exited due to file unavailability"
        else:

            file_name = ""
            json_df = pd.read_json(pubsub_message)
            with pd.ExcelWriter(file_name, engine='xlsxwriter') as writer:
                json_df.to_excel(writer, index=False)

            bucket_name = bucket_id
            bucket = storage_client.get_bucket(bucket_name)
            blob = bucket.blob(file_name)
            blob.upload_from_filename(file_name)
            print("file Upload Completed")

        return process_start(bucket, file_name)

    except Exception as e:
        logger.log_text(f"Error in storage path parsing {e}")
        end_time = datetime.now()
        update_tracker_table(file_name, failed, "Error in Parsing the file path",
                             start_time, end_time, file_upload, duration, process)
        move_file(file_name, "Failed")
        file_path = f"gs://{output_bucket}/Failed/{file_name}"
        send_email(sender_email, sender_password, receiver_email,
                   subject, 'Failed', file_path, 0)
        return "Job Exited with error"


def read_file_from_storage(bucket_name, file_name):

    logger.log_text(f"Reading file {file_name}")

    file_path = f"gs://{bucket_name}/{file_name}"

    try:
        storage_df = pd.read_excel(file_path)
        storage_columns = set(storage_df.columns)
        if mandatory_column not in storage_columns:

            file_path = f"gs://{output_bucket}/failed/{file_name}"
            rows_processed = 0
            send_email(sender_email, sender_password, receiver_email,
                       subject, failed, file_path, rows_processed)
            move_file(file_name, "Insufficient_Data")
            end_time = datetime.now()
            update_tracker_table(file_name, failed, "Mandatory Column is missing in file",
                                 start_time, end_time, file_upload, duration, process)
            return "Mandatory column is missing. Pipeline execution stopped"

        storage_df = storage_df.applymap(str)
        storage_df = storage_df.applymap(mask_sensitive_data)
        logger.log_text(f"File Reading Completed")
        end_time = datetime.now()
        update_tracker_table(file_name, progress, "File Reading completed",
                             start_time, end_time, file_upload, duration, process)
        return read_data_from_bigquery(project_id, table_id, dataset_id, storage_df, file_name)

    except Exception as e:
        logger.log_text(f"Job has been failed with error {e}")
        end_time = datetime.now()
        update_tracker_table(file_name, failed, "Error in file reading",
                             start_time, end_time, file_upload, duration, process)
        move_file(file_name, "Failed")
        file_path = f"gs://{output_bucket}/failed/{file_name}"
        rows_processed = 0
        send_email(sender_email, sender_password, receiver_email,
                   subject, failed, file_path, rows_processed)
        return "Job Exited with error"


def read_data_from_bigquery(project_id, table_id, dataset, storage_df, file_name):

    logger.log_text(f"Reading gbq table {table_id}")

    query = f"select * from {project_id}.{dataset}.{table_id}"

    try:

        gbq_df = pd.read_gbq(query, credentials=credentials)
        # print(gbq_df)
        logger.log_text(f"Table Reading Completed")
        end_time = datetime.now()
        update_tracker_table(
            file_name, progress, "Table {table_id} to dataframe load completed", start_time, end_time, file_upload, duration, process)
        return load_data_gbq_table(storage_df, gbq_df, table_id, dataset, file_name)
    except Exception as e:
        logger.log_text(f"Job has been failed with error {e}")
        end_time = datetime.now()
        update_tracker_table(
            file_name, failed, f"Error in reading Table {table_id} ", start_time, end_time, file_upload, duration, process)
        move_file(file_name, "Failed")
        file_path = f"gs://{output_bucket}/failed/{file_name}"
        rows_processed = 0
        send_email(sender_email, sender_password, receiver_email,
                   subject, failed, file_path, rows_processed)
        return e


def load_data_gbq_table(storage_df, gbq_df, table_name, dataset_name, file_name):

    logger.log_text(f"Table Load is Starting")

    try:
        table_ref = bq_client.dataset(dataset_id).table(table_id)
        table = bq_client.get_table(table_ref)

        new_schema = table.schema

        storage_columns = set(storage_df.columns)
        gbq_columns = set(gbq_df.columns)
        new_columns = storage_columns - gbq_columns

        types = storage_df.dtypes.to_dict()

        for column, type in types.copy().items():

            if column not in new_columns:
                del types[column]
        # print(len(types))
        # print(types)
        for column, type in types.items():
            # print(column, type)

            if type == 'int64':
                type = 'INT64'

            elif type == 'float64':
                type = 'FLOAT64'

            elif type == 'bool':
                type = 'BOOLEAN'

            else:
                type = 'STRING'
            # print(column, type)
            new_schema.append(bigquery.SchemaField(column, type))

        table.schema = new_schema
        bq_client.update_table(table, ["schema"])

        tmp_schema = []
        for field in new_schema:

            col = {'name': f'{field.name}', 'type': f'{field.field_type}'}

            tmp_schema.append(col)

        temp_table = 'temp'
        tamep_table_ref = bq_client.dataset(dataset_id).table(temp_table)
        temp = bigquery.Table(tamep_table_ref, schema=new_schema)
        bq_client.delete_table(temp, not_found_ok=True)
        temp = bq_client.create_table(temp)
        # print(storage_df)
        storage_df.to_gbq(f'{temp}', table_schema=tmp_schema, if_exists='replace',
                          credentials=credentials)

        source_list = storage_df.columns.to_list()
        comma_seperated = ','.join(source_list)
        # print(comma_seperated)
        temp_data = []
        for column in source_list:
            st = f'target.{column} = source.{column}'
            temp_data.append(st)

        assign = ','.join(temp_data)
        # print("")
        # print(assign)

        # print(" ")

        merge_query = f""" 

                MERGE `{project_id}.{dataset_id}.{table_id}` AS target
            USING (
                SELECT *
                FROM `{project_id}.{dataset_id}.temp`
            ) AS source
            ON target.Id = source.Id
            WHEN MATCHED THEN
                UPDATE SET {assign}
            WHEN NOT MATCHED THEN
                INSERT ({comma_seperated}) Values({comma_seperated});

        
        
        """

        print(merge_query)

        job = bq_client.query(merge_query)
        print(job.result())

        bq_client.delete_table(tamep_table_ref, not_found_ok=True)
        print("temp table is removed")
        logger.log_text("temp table is removed")

        end_time = datetime.now()
        update_tracker_table(
            file_name, success, f"file {file_name} loaded to table {table_name}", start_time, end_time, file_upload, duration, process)
        move_file(file_name, "Success")
        file_path = f"gs://{output_bucket}/Success/{file_name}"
        rows_processed = storage_df.shape[0]
        send_email(sender_email, sender_password, receiver_email,
                   subject, success, file_path, rows_processed)

        logger.log_text(f'file has been moved')
        return "Job execution has been completed"
    except Exception as e:
        logger.log_text(f"Job has been failed with error {e}")
        end_time = datetime.now()
        update_tracker_table(file_name, failed, f"Table load  is Incomplete",
                             start_time, end_time, file_upload, duration, process)
        move_file(file_name, "Failed")
        file_path = f"gs://{output_bucket}/failed/{file_name}"
        rows_processed = 0
        send_email(sender_email, sender_password, receiver_email,
                   subject, failed, file_path, rows_processed)
        return e


def move_file(file_name, folder):

    bucket = storage_client.get_bucket(bucket_id)
    blob_old = bucket.blob(file_name)

    out = storage_client.get_bucket(output_bucket)

    blob_new = bucket.copy_blob(
        blob_old, out, new_name=f'{folder}/{file_name}')

    blob_old.delete()


def update_tracker_table(file_name, status, stage, start_time, end_time, file_upload, duration, process):

    query = f"SELECT * FROM {dataset_id}.{etl_tracker} WHERE file_name='{file_name}';"
    logger.log_text(query)
    print(query)
    rows = pd.read_gbq(query, credentials=credentials)
    duration = float((end_time - start_time).microseconds)
    start_time = start_time
    end_time = end_time
    duration = duration
    file_upload = file_upload

    if rows.empty:

        query = f"INSERT INTO {dataset_id}.{etl_tracker} (file_name,status,stage,start_time,end_time,duration_in_ms,upload_time,process) VALUES ('{file_name}','{status}','{stage}','{start_time}','{end_time}',{duration},'{file_upload}','{process}');"

        logger.log_text(query)
        print(query)
        job = bq_client.query(query).result()

    query = f"UPDATE {dataset_id}.{etl_tracker} SET status='{status}',stage='{stage}',start_time='{start_time}',end_time='{end_time}',duration_in_ms={duration},upload_time='{file_upload}',process='{process}'  WHERE file_name='{file_name}' ;"
    logger.log_text(query)
    bq_client.query(query).result()


def send_email(sender_email, sender_password, receiver_email, subject, status, file_path, number_of_rows_processed):

    smtp_server = sm_server
    smtp_port = port
    sender_email = sender_email
    receiver_email = receiver_email
    subject = subject

    try:
        file_name = file_path.split("/")[-1]

        if status == 'Duplicate':

            subject = "File Already Processed"

            body = f"""
            <html>
            <body>
                Hi, 
                
                <p>file has already processed. Moving to Duplicate folder</p>
                <p>Please check "{dataset_id}.{etl_tracker}" table for more details</p>
                <p></p> 
                <table>
                    <tr>
                        <th>File Path : </th>
                        <td><a href="https://storage.cloud.google.com/{output_bucket}/Duplicate/{file_name}">{file_path}</a></td>
                    </tr>
                
                </table>
                Thanks
            </body>
            </html>
            """

        elif status == 'file received':

            subject = 'New file received to bucket'

            body = f"""
            <html>
            <body>
                Hi, 
                
                <p>file received into the bucket, Job is starting. Please find the received file path</p>
                
                <table>
                    <tr>
                        <th>File Path : </th>
                        <td><a href="https://storage.cloud.google.com/{bucket_id}/{file_name}">{file_path}</a></td>
                    </tr>
                
                </table>

            </body>
            </html>
            """

        elif status == 'File Unavailable':

            subject = 'File Not Available'

            body = f"""
            <html>
            <body>
                Hi, 
                
                <p>The file "{file_path}" is not available in the bucket. Job exited</p>
                
               
               Thanks

            </body>
            </html>
            """

        else:

            stat = status.upper()
            subject = f"Job {stat} "

            folder = status.capitalize()

            body = f"""
            <html>
            <body>
                Hi, 
                
                <p>Job {status} Please find the status and file path below.</p>
                <p>Please check "{dataset_id}.{etl_tracker}" table for more details</p>
                
                <table>
                    <tr>
                        <th>File Path : </th>
                        <td><a href="https://storage.cloud.google.com/{output_bucket}/{folder}/{file_name}">{file_path}</a></td>
                    </tr>

                    <tr>
                        <th>Rows Processed : </th>
                        <td>{number_of_rows_processed}</td>
                    </tr>
                
                </table>

            </body>
            </html>
            """

            # Create a multipart message
        message = MIMEMultipart('alternative')
        message['From'] = sender_email
        message['To'] = receiver_email
        message['Subject'] = subject

        # Attach the HTML content
        message.attach(MIMEText(body, 'html'))

        # Connect to the SMTP server and send the email
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            # Replace with your email password
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, receiver_email, message.as_string())

        print('Email sent successfully!')
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

            move_file(file_name, 'Duplicate')

            file_path = f"gs://{output_bucket}/Duplicate/{file_name}"
            send_email(sender_email, sender_password, receiver_email,
                       subject, 'Duplicate', file_path, 0)

            logger.log_text(
                f"{file_name} is a duplicate file exiting the process")
            return "Received duplicate file so Job Exited"

        file_path = f"gs://{bucket_name}/{file_name}"
        send_email(sender_email, sender_password, receiver_email,
                   subject, 'file received', file_path, 0)

        read_file_from_storage(bucket_name, file_name)

    except Exception as e:
        logger.log_text(f"Job has been failed with error {e}")
        move_file(file_name, "Failed")
        file_path = f"gs://{output_bucket}/Failed/{file_name}"
        send_email(sender_email, sender_password, receiver_email,
                   subject, 'Failed', file_path, 0)

        return e
