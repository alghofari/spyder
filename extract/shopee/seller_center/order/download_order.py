import requests
from datetime import date
from extract.shopee.seller_center.helper.helper import get_json

def download_content(official_store_name: str) -> bytes:
    """
    Download the excel content
    Args:
        official_store_name (str): official store name for GCS
    Returns:
        res.content (bytes): response content from shopee (excel in bytes)
    """
    # Get cookies from GCS
    json_object = get_json(official_store_name)
    cookies     = json_object['cookies'] 
    report_id   = json_object['report_id'] 

    # Call the API from Shopee and get excel content
    spc_cds = [{'value':f"{cookie['name']}={cookie['value']}"} for cookie in cookies if cookie['name'] == 'SPC_CDS']
    cookie = '; '.join([f"{cookie['name']}={cookie['value']}" for cookie in cookies])
    
    url = f'https://seller.shopee.co.id/api/v3/settings/download_report/?&{spc_cds[0]["value"]}&SPC_CDS_VER=2&report_id={report_id}'

    headers = {
        'authority' : 'seller.shopee.co.id',
        'accept': 'application/json, application/force-download, text/plain, */*',
        'accept-language': 'en-US,en;q=0.9',
        'cookie': str(cookie),
        'referer': 'https://seller.shopee.co.id/portal/sale/order',
        'sec-ch-ua': '"Chromium";v="110", "Not A(Brand";v="24", "Google Chrome";v="110"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
    }

    res = requests.get(url=url, headers=headers)
    return res.content


def main(start_date, end_date, official_store_name, storage_client):
    """
    Upload excel download to GCS
    Args:
        start_date (str): start date for data to be extracted.
        end_date (str): end date for data to be extracted.
        official_store_name (str): official store name for GCS
        storage_client (any): GCS client for sirclo-data-prod
    Returns:
        None
    """
    df = download_content(official_store_name)
    
    client = storage_client.get_bucket('sirclo-data-marketplace')
    blob   = client.blob(f'assets/excel/shopee/sales/{official_store_name}/{date.today()}/Shopee_Order_{start_date}_{end_date}.xlsx')

    with blob.open(mode='wb') as f:
        f.write(df)