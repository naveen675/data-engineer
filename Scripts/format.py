import base64
import pandas as pd
import io
from datetime import datetime
from google.cloud import storage, bigquery, logging, dlp
from config import project_id, dataset_id, table_id, etl_tracker, bucket_id, output_bucket, sender_email, sender_password, receiver_email, subject, sm_server, port, json, progress, success, failed, mandatory_column, process
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
import gcsfs
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
file_upload = datetime.now()
start_time = datetime.now()
end_time = datetime.now()
duration = float((end_time - start_time).microseconds)


def hello_pubsub(event, context):

    try:

        pubsub_message = str(base64.b64decode(event['data']).decode('utf-8'))
        logger.log_text(f'message : {pubsub_message}')
        bucket = ''
        file_name = ''

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

            name = str(datetime.now().strftime('%Y%m%d-%H%M%S'))
            file_name = f"{name}.xlsx"
            json_df = pd.read_json(pubsub_message)
            print(json_df)
            logger.log_text("json load to df")
            bucket_name = bucket_id
            with gcsfs.GCSFileSystem().open(f'{bucket_name}/{file_name}', 'wb') as file:
                json_df.to_excel(file, index=False)
            bucket = bucket_name
            print("file Upload Completed")

        print(f"bucket : {bucket}")
        print(f"File name : {file_name}")
        return process_start(bucket, file_name)

    except Exception as e:
        logger.log_text(f"Error in storage path parsing {e}")
        end_time = datetime.now()
        update_tracker_table(file_name, failed, "Invalid File path or Data",
                             start_time, end_time, file_upload, duration, process, 'inactive')
        move_file(file_name, "Failed")
        file_path = f"gs://{output_bucket}/Failed/{file_name}"
        send_email(sender_email, sender_password, receiver_email,
                   subject, 'Failed', file_path, 0)
        return "Job Exited with error"


# def read_file_from_storage(bucket_name, file_name,storage_df):

#     try:

#         logger.log_text(f"Reading file {file_name}")

#         file_path = f"gs://{output_bucket}/failed/{file_name}"
#         query = f"SELECT * FROM {dataset_id}.{etl_tracker} WHERE file_name='{file_name}' ;"
#         logger.log_text(query)
#         print(query)
#         new_file = pd.read_gbq(query, credentials=credentials)
#         print("New File")
#         storage_df = storage_df

#         storage_df = storage_df.applymap(str)
#         storage_df = storage_df.applymap(mask_sensitive_data)
#         print("Sensitive Masked")
#         if not new_file.empty:

#             query = f"select * from {project_id}.{dataset_id}.{table_id};"
#             print(query)

#             gbq_df = pd.read_gbq(query, credentials=credentials)
#             gbq_df = gbq_df.reset_index(drop=True)
#             storage_df = storage_df.astype(str)
#             matched = gbq_df['Id'].isin(storage_df['Id'])
#             matched_df = gbq_df[matched]
#             print(matched_df)
#             print("Stirage")
#             print(storage_df)
#             if storage_df.shape[0] > matched_df.shape[0]:
#                 print("New row in the existing file")
#                 logger.log_text("New row in the existing file")

#             else:
#                 columns = storage_df.columns.to_list()
#                 print(columns)
#                 df_excel_sorted = storage_df.sort_values(by=['Id']).reset_index(drop=True)
#                 df_matched_sorted = matched_df.sort_values(by=['Id']).reset_index(drop=True)
#                 columns_to_compare = columns
#                 print("Columns Sorted")
#                 df_excel_selected = df_excel_sorted[columns_to_compare]
#                 df_matched_selected = df_matched_sorted[columns_to_compare]
#                 df_excel_selected = df_excel_selected.astype(str)
#                 print("Selected specific columns")
#                 no_of_rows_changed = df_matched_selected.compare(df_excel_selected).shape[0]
#                 print(f"No of rows changed : {no_of_rows_changed}")
#                 logger.log_text(f"No of rows changed : {no_of_rows_changed}")
#                 if no_of_rows_changed == 0:

#                     move_file(file_name, 'Duplicate')

#                     send_email(sender_email, sender_password, receiver_email,
#                                 subject, 'Duplicate', file_path, 0)

#                     logger.log_text(
#                             f"{file_name} is already processed exiting the process")
#                     print("{file_name} is already processed exiting the process")
#                     return "Received duplicate file so Job Exited"

#         logger.log_text(f"File Reading Completed")
#         print(f"File Reading Completed")
#         end_time = datetime.now()
#         update_tracker_table(file_name, progress, "File Reading completed",
#                                 start_time, end_time, file_upload, duration, process)
#             # update_tracker_table(file_name,progress,"File Reading completed")
#         return read_data_from_bigquery(project_id, table_id, dataset_id, storage_df, file_name)

#     except Exception as e:
#         logger.log_text(f"Job has been failed with error {e}")
#         end_time = datetime.now()
#         update_tracker_table(file_name, failed, "Error in file reading",
#                              start_time, end_time, file_upload, duration, process)
#         move_file(file_name, "Failed")
#         file_path = f"gs://{output_bucket}/failed/{file_name}"
#         rows_processed = 0
#         send_email(sender_email, sender_password, receiver_email,
#                    subject, failed, file_path, rows_processed)
#         return "Job Exited with error"


def read_data_from_bigquery(project_id, table_id, dataset, storage_df, file_name):

    logger.log_text(f"Reading gbq table {table_id}")

    query = f"select * from {project_id}.{dataset}.{table_id}"

    try:

        gbq_df = pd.read_gbq(query, credentials=credentials)
        # print(gbq_df)
        logger.log_text(f"Table Reading Completed")
        end_time = datetime.now()
        update_tracker_table(
            file_name, progress, "Table {table_id} to dataframe load completed", start_time, end_time, file_upload, duration, process, 'active')
        return load_data_gbq_table(storage_df, gbq_df, table_id, dataset, file_name)
    except Exception as e:
        logger.log_text(f"Job has been failed with error {e}")
        end_time = datetime.now()
        update_tracker_table(
            file_name, failed, f"Error in reading Table {table_id} ", start_time, end_time, file_upload, duration, process, 'inactive')
        move_file(file_name, "Failed")
        file_path = f"gs://{output_bucket}/failed/{file_name}"
        rows_processed = 0
        send_email(sender_email, sender_password, receiver_email,
                   subject, failed, file_path, rows_processed)
        return e


def load_data_gbq_table(storage_df, gbq_df, table_name, dataset_name, file_name):

    rows_processed = 0

    try:
        file_path = f"gs://{bucket_id}/{file_name}"
        table_ref = bq_client.dataset(dataset_id).table(table_id)
        table = bq_client.get_table(table_ref)

        storage_df = storage_df.applymap(str)
        storage_df = storage_df.applymap(mask_sensitive_data)
        print("Sensitive Masked")

        gbq_df = gbq_df.reset_index(drop=True)
        storage_df = storage_df.astype(str)
        matched = gbq_df['Id'].isin(storage_df['Id'])
        matched_df = gbq_df[matched]
        print(matched_df)
        print("Stirage")
        print(storage_df)
        if storage_df.shape[0] > matched_df.shape[0]:
            print("New row in the existing file")
            rows_processed = rows_processed + \
                (storage_df.shape[0] - matched_df.shape[0])
            logger.log_text("New row in the existing file")

        else:
            columns = storage_df.columns.to_list()
            print(columns)
            df_excel_sorted = storage_df.sort_values(
                by=['Id']).reset_index(drop=True)
            df_matched_sorted = matched_df.sort_values(
                by=['Id']).reset_index(drop=True)
            columns_to_compare = columns
            print("Columns Sorted")
            df_excel_selected = df_excel_sorted[columns_to_compare]
            df_matched_selected = df_matched_sorted[columns_to_compare]
            df_excel_selected = df_excel_selected.astype(str)
            print("Selected specific columns")
            no_of_rows_changed = df_matched_selected.compare(
                df_excel_selected).shape[0]
            rows_processed = rows_processed + no_of_rows_changed
            print(f"No of rows changed : {no_of_rows_changed}")
            logger.log_text(f"No of rows changed : {no_of_rows_changed}")
            if no_of_rows_changed == 0:

                move_file(file_name, 'Duplicate')
                update_tracker_table(
                    file_name, failed, f"already processed ", start_time, end_time, file_upload, duration, process, 'inactive')
                send_email(sender_email, sender_password, receiver_email,
                           subject, 'Duplicate', file_path, 0)

                logger.log_text(
                    f"{file_name} is already processed exiting the process")
                print("{file_name} is already processed exiting the process")
                return "Received duplicate file so Job Exited"

        logger.log_text(f"Table Load is Starting")
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
        logger.log_text(merge_query)
        print(merge_query)

        job = bq_client.query(merge_query)
        print(job.result())

        bq_client.delete_table(tamep_table_ref, not_found_ok=True)
        print("temp table is removed")

        end_time = datetime.now()
        update_tracker_table(
            file_name, success, f"file {file_name} loaded to table {table_name}", start_time, end_time, file_upload, duration, process, 'inactive')
        # update_tracker_table(file_name,success,f"file {file_name} loaded to table {table_name}")
        move_file(file_name, "Success")
        file_path = f"gs://{output_bucket}/Success/{file_name}"

        send_email(sender_email, sender_password, receiver_email,
                   subject, success, file_path, rows_processed)

        logger.log_text(f'file has been moved')
        return "Job execution has been completed"
    except Exception as e:
        logger.log_text(f"Job has been failed with error {e}")
        end_time = datetime.now()
        update_tracker_table(file_name, failed, f"Table load  is Incomplete",
                             start_time, end_time, file_upload, duration, process, 'inactive')
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


def update_tracker_table(file_name, status, stage, start_time, end_time, file_upload, duration, process, pipeline):

    query = f"SELECT * FROM {dataset_id}.{etl_tracker} WHERE file_name='{file_name}' AND pipeline='active';"
    logger.log_text(query)
    print(query)
    rows = pd.read_gbq(query, credentials=credentials)
    file_upload = start_time
    duration = float((end_time - start_time).microseconds)
    start_time = start_time
    end_time = end_time
    duration = duration
    file_upload = file_upload

    if rows.empty:

        query = f"INSERT INTO {dataset_id}.{etl_tracker} (file_name,status,stage,start_time,end_time,duration_in_ms,upload_time,process,pipeline) VALUES ('{file_name}','{status}','{stage}','{start_time}','{end_time}',{duration},'{file_upload}','{process}','active');"

        logger.log_text(query)
        print(query)
        job = bq_client.query(query).result()
    pipeline = pipeline
    query = f"UPDATE {dataset_id}.{etl_tracker} SET status='{status}',stage='{stage}',start_time='{start_time}',end_time='{end_time}',duration_in_ms={duration},upload_time='{file_upload}',process='{process}', pipeline='{pipeline}'  WHERE file_name='{file_name}' AND pipeline='active' ;"
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

        else:

            stat = status.upper()
            subject = f"Job {stat} "

            folder = status.capitalize()

            query = f"select * from {project_id}.{dataset_id}.etl_tracker where file_name='{file_name}';"

            gbq_df = pd.read_gbq(query, credentials=credentials)

            status = gbq_df['status'].values[0]
            start_time = gbq_df['start_time'].values[0]
            end_time = gbq_df['end_time'].values[0]
            duration = gbq_df['duration_in_ms'].values[0]

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

                    <tr>
                        <th>Start Time : </th>
                        <td>{start_time}</td>
                    </tr>

                    <tr>
                        <th>End Time : </th>
                        <td>{end_time}</td>
                    </tr>

                     <tr>
                        <th>Duration(ms) :  </th>
                        <td>{duration}</td>
                    </tr>

                    <tr>
                        <th>Process type : </th>
                        <td>{process}</td>
                    </tr>

                     <tr>
                        <th>Status : </th>
                        <td>{status}</td>
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

    start_time = datetime.now()
    end_time = datetime.now()
    duration = float((end_time - start_time).microseconds)
    file_upload = start_time

    try:

        bucket_name = bucket_name
        file_name = file_name
        print(file_name)

        file_path = f"gs://{bucket_name}/{file_name}"
        storage_df = pd.read_excel(file_path)

        storage_columns = set(storage_df.columns)
        if mandatory_column not in storage_columns:

            file_path = f"gs://{output_bucket}/Insufficient_Data/{file_name}"
            rows_processed = 0

            move_file(file_name, "Insufficient_Data")
            end_time = datetime.now()
            update_tracker_table(file_name, failed, "Mandatory Column is missing in file",
                                 start_time, end_time, file_upload, duration, process, 'inactive')
            send_email(sender_email, sender_password, receiver_email,
                       subject, failed, file_path, rows_processed)
            return "Mandatory column is missing. Pipeline execution stopped"

        start_time = datetime.now()

        send_email(sender_email, sender_password, receiver_email,
                   subject, 'file received', file_path, 0)

        read_data_from_bigquery(project_id, table_id,
                                dataset_id, storage_df, file_name)

    except Exception as e:
        logger.log_text(f"Job has been failed with error {e}")
        move_file(file_name, "Failed")
        file_path = f"gs://{output_bucket}/Failed/{file_name}"
        send_email(sender_email, sender_password, receiver_email,
                   subject, 'Failed', file_path, 0)
        update_tracker_table(file_name, failed, "Failed At Intial Stage",
                             start_time, end_time, file_upload, duration, process, 'inactive')

        return e
