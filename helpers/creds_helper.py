import gspread, os
import pandas as pd
from google.cloud import bigquery
from cryptography.fernet import Fernet

# Function to get config
def get_config(os_key, marketplace):
    gc        = gspread.service_account(filename=os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
    sh        = gc.open_by_key('1SizN344HBT-QK2Fg9rUVQEMATvlWVndOWdVv0fWtedg')
    worksheet = sh.worksheet(marketplace)
    records   = worksheet.get_all_records()
    
    if os_key != "all":
        config    = [i for i in records if i['os_key'] == os_key][0]
    else:
        dataframe = pd.DataFrame(worksheet.get_all_records())
        config = dataframe['os_name'].values.tolist()

    return config

# Function to get credentials

def get_creds(email_os, marketplace):
    bq_client = bigquery.Client()
    fernet_key = os.getenv('FERNET_KEY')

    if marketplace.lower() == 'tokopedia':
        query = f"""
        SELECT * FROM `sirclo-prod.bronze_vaultwarden.tokopedia_credentials` WHERE REGEXP_REPLACE(os_email, " ", "") like '%{email_os}%'
        """
    elif marketplace.lower() == 'shopee':
        query = f"""
            SELECT * FROM `sirclo-prod.bronze_vaultwarden.shopee_credentials` WHERE os_email like '{email_os.replace(' ','')}' and ned_email is not null
        """ 
    else:
        pass

    try:
        df = bq_client.query(query).to_dataframe()

        fernet = Fernet(fernet_key)

        df["os_password"] = df["os_password"].apply(lambda x: fernet.decrypt(x.encode()) if x is not None and x != "" else x)
        df["os_password"] = df["os_password"].str.decode('utf-8')
        creds = df.values.tolist()[0]
        return creds
    
    except Exception as e:
        print(f"There might be some error for {email_os}")
        print(f"With query like this:\n{query}")
        print(str(e))