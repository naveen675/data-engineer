import pandas as pd
import io
from datetime import datetime
from google.cloud import storage, bigquery, logging, dlp
from config import project_id, dataset_id, table_id, etl_tracker, bucket_id, output_bucket, sender_email, sender_password, receiver_email, subject, sm_server, port, json, progress, success, failed, file_name
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


def slip_gcs():

    file_path = f"gs://{bucket_id}/{file_name}"

    bucket = file_path.split("/")[2]
    print(bucket)
    object_name = "/".join(file_path.split("/")[3:])

    file = file_path.split("/")[-1]
    print(file)


slip_gcs()


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
        # print(gbq_df)
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


def process_start(event, context):

    try:

        bucket_name = event['bucket']
        file_name = event['name']
        print(file_name)

        query = f"SELECT * FROM {dataset_id}.{etl_tracker} WHERE file_name='{file_name}' AND status ='{success}';"

        rows = pd.read_gbq(query, credentials=credentials)

        if not rows.empty:

            message = f"Hello, {file_name} is a duplicate file moving this file to duplicate folder,Pipeline Execution Aborted "
            send_email(sender_email, sender_password,
                       receiver_email, subject, message)

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




##### ********  ########## ******* ############

import pandas as pd
import io
from datetime import datetime
from google.cloud import storage, bigquery, logging, dlp
from config import project_id, dataset_id, table_id,etl_tracker, bucket_id,output_bucket, sender_email, sender_password, receiver_email, subject, sm_server, port, json, progress,success,failed,mandatory_column
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


def read_file_from_storage(bucket_name, file_name):

    logger.log_text(f"Reading file {file_name}")

    file_path = f"gs://{bucket_name}/{file_name}"

    try:
        storage_df = pd.read_excel(file_path)
        storage_columns = set(storage_df.columns)
        if mandatory_column not in storage_columns:

            file_path = f"gs://{output_bucket}/failed/{file_name}"
            rows_processed = 0
            send_email(sender_email, sender_password, receiver_email, subject,failed,file_path,rows_processed)
            move_file(file_name,"Insufficient_Data")
            update_tracker_table(file_name,failed,"Mandatory Column is missing in file")
            return "Mandatory column is missing. Pipeline execution stopped"

        storage_df = storage_df.applymap(str)
        storage_df = storage_df.applymap(mask_sensitive_data)
        logger.log_text(f"File Reading Completed")
        update_tracker_table(file_name,progress,"File Reading completed")
        return read_data_from_bigquery(project_id, table_id, dataset_id,storage_df,file_name)

    except Exception as e:
        logger.log_text(f"Job has been failed with error {e}")
        update_tracker_table(file_name,failed, "Error in file reading")
        move_file(file_name,"Failed")
        file_path = f"gs://{output_bucket}/failed/{file_name}"
        rows_processed = 0
        send_email(sender_email, sender_password, receiver_email, subject,failed,file_path,rows_processed)
        return "Job Exited with error"
    

def read_data_from_bigquery(project_id, table_id, dataset,storage_df,file_name):

    logger.log_text(f"Reading gbq table {table_id}")

    query = f"select * from {project_id}.{dataset}.{table_id}"

    try:

        gbq_df = pd.read_gbq(query, credentials=credentials)
        # print(gbq_df)
        logger.log_text(f"Table Reading Completed")
        update_tracker_table(file_name,progress,f"Table {table_id} to dataframe load completed")
        return load_data_gbq_table(storage_df, gbq_df, table_id, dataset,file_name)
    except Exception as e:
        logger.log_text(f"Job has been failed with error {e}")
        update_tracker_table(file_name,failed,f"Error in reading Table {table_id} ")
        move_file(file_name,"Failed")
        file_path = f"gs://{output_bucket}/failed/{file_name}"
        rows_processed = 0
        send_email(sender_email, sender_password, receiver_email, subject,failed,file_path,rows_processed)
        return e
    




def load_data_gbq_table(storage_df, gbq_df, table_name, dataset_name,file_name):

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


        
        update_tracker_table(file_name,success,f"file {file_name} loaded to table {table_name}")
        move_file(file_name,"Success")
        file_path = f"gs://{output_bucket}/Success/{file_name}"
        rows_processed = storage_df.shape[0]
        send_email(sender_email, sender_password, receiver_email, subject,success,file_path,rows_processed)
        
        logger.log_text(f'file has been moved')
        return "Job execution has been completed"
    except Exception as e:
        logger.log_text(f"Job has been failed with error {e}")
        update_tracker_table(file_name,failed,f"Table load  is Incomplete")
        move_file(file_name,"Failed")
        file_path = f"gs://{output_bucket}/failed/{file_name}"
        rows_processed = 0
        send_email(sender_email, sender_password, receiver_email, subject,failed,file_path,rows_processed)
        return e
    


def move_file(file_name,folder):

        bucket = storage_client.get_bucket(bucket_id)
        blob_old = bucket.blob(file_name)

        out = storage_client.get_bucket(output_bucket)

        blob_new = bucket.copy_blob(blob_old, out, new_name=f'{folder}/{file_name}')

        blob_old.delete()
    
def update_tracker_table(file_name, status,stage):

    query = f"SELECT * FROM {dataset_id}.{etl_tracker} WHERE file_name='{file_name}';"

    rows = pd.read_gbq(query, credentials=credentials)

    if rows.empty:

        query = f"INSERT INTO {dataset_id}.{etl_tracker} (file_name,status,stage) VALUES ('{file_name}','{status}','{stage}');"

        job = bq_client.query(query).result()

    query = f"UPDATE {dataset_id}.{etl_tracker} SET status='{status}',stage='{stage}' WHERE file_name='{file_name}' ;"

    bq_client.query(query).result()


    





def send_email(sender_email, sender_password, receiver_email, subject, status,file_path,number_of_rows_processed):

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

            move_file(file_name,'Duplicate')
            file_path =  f"gs://{output_bucket}/Duplicate/{file_name}"
            send_email(sender_email, sender_password, receiver_email, subject,'Duplicate',file_path, 0 )

            logger.log_text(f"{file_name} is a duplicate file exiting the process")
            return "Received duplicate file so Job Exited"

        

        message = f"Hello, {file_name} has received into bucket Job is starting"
        file_path = f"gs://{bucket_name}/{file_name}" 
        send_email(sender_email, sender_password, receiver_email, subject,'file received',file_path,0 )


        

        read_file_from_storage(bucket_name, file_name)

    except Exception as e:
        logger.log_text(f"Job has been failed with error {e}")
        move_file(file_name,"Failed")
        file_path =  f"gs://{output_bucket}/Failed/{file_name}"
        send_email(sender_email, sender_password, receiver_email, subject,'Failed',file_path,0 )

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

