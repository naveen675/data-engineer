import pandas as pd
from google.cloud import dlp
from config import project_id, dataset_id, table_id, bucket_id, file_name, sender_email, sender_password, receiver_email, subject, sm_server, port, json


df = pd.read_csv('addresses.csv')


dlp_client = dlp.DlpServiceClient.from_service_account_file(json)


inspect_config = {
    "info_types": [
        {"name": "PHONE_NUMBER"},
        {"name": "EMAIL_ADDRESS"},

    ]
}


def mask_sensitive_data(str):

    item = {"value": str}

    # Get your project ID from the config file


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
        print(response.result.findings[0].info_type.name)
        return masked_text
    else:

        return str


df = df.applymap(str)
df = df.applymap(mask_sensitive_data)
print(df.to_csv('out.csv'))
