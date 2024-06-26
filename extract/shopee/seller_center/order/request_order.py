import sys
import json
import time
import requests

from extract.shopee.seller_center.helper.helper import get_json

def main(start_date, end_date, official_store_name, storage_client):
    """
    Request content from shopee order by API
    Args:
        start_date (str): start date for data to be extracted.
        end_date (str): end date for data to be extracted.
        official_store_name (str): official store name for GCS
        storage_client (any): GCS client for sirclo-data-prod
    Returns:
        None
    """
    # Get cookies from GCS
    json_object = get_json(official_store_name)
    cookies = json_object['cookies'] 
    
    # Call the API from Shopee and get report id
    spc_cds = [{'value':f"{cookie['name']}={cookie['value']}"} for cookie in cookies if cookie['name'] == 'SPC_CDS']
    cookie = '; '.join([f"{cookie['name']}={cookie['value']}" for cookie in cookies])

    url = f'https://seller.shopee.co.id/api/v3/order/request_order_report?{spc_cds[0]["value"]}&SPC_CDS_VER=2&start_date={start_date}&end_date={end_date}&language=id&screening_condition=order_creation_date'
    headers = {
        'authority' : 'seller.shopee.co.id',
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-US,en;q=0.9',
        'cookie': cookie,
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
    report_id = res.json()['data']['report_id']
    res.close()

    # Place report ID and cookies back on GCS (report ID needed to download the file)
    report_id     = {
        "cookies"   : cookies,
        "report_id" : report_id
    }

    json_object     = json.dumps(report_id, indent=4)
    client          = storage_client.get_bucket('sirclo-data-marketplace')
    blob            = client.blob(f'assets/excel/shopee/sales/{official_store_name}/credentials/cookies.json')
    with blob.open(mode='w') as f:
        f.write(json_object)

    # Time function
    print(f"Waiting request for {official_store_name} order")
    for remaining in range(120, 0, -1):
        sys.stdout.write("\r")
        sys.stdout.write("{:2d} seconds remaining.".format(remaining))
        sys.stdout.flush()
        time.sleep(1)

    sys.stdout.write("\rRequest complete!            \n")