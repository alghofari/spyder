import sys
import time
import requests

from datetime import date
from datetime import datetime
from datetime import timedelta

from extract.shopee.seller_center.helper.helper import get_json


def request_file(start_time, end_time, official_store_name):
    """
    Request content from shopee ads by API
    Args:
        start_time (float): start time for data to be extracted in UNIX.
        end_time (float): end time for data to be extracted in UNIX.
        official_store_name (str): official store name for GCS
    Returns:
        file_id (str): file id needed to download file
    """
    # Get cookies from GCS
    json_object = get_json(official_store_name)
    cookies = json_object['cookies']

    # Call the API from Shopee and get report id
    spc_cds = [{'value':f"{cookie['name']}={cookie['value']}"} for cookie in cookies if cookie['name'] == 'SPC_CDS']
    cookie = '; '.join([f"{cookie['name']}={cookie['value']}" for cookie in cookies])

    url = f"https://seller.shopee.co.id/api/marketing/v3/pas/report_file/export/?report_type=0&start_time={int(start_time)}&end_time={int(end_time)}&SPC_CDS={spc_cds[0]['value']}&SPC_CDS_VER=2"

    headers = {
            'authority' : 'seller.shopee.co.id',
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-US,en;q=0.9',
            'cookie': cookie,
            'referer': f'https://seller.shopee.co.id/portal/marketing/pas/assembly?from={int(start_time)}&to={int(end_time)}&type=all&group=custom',
            'sec-ch-ua': '"Chromium";v="112", "Google Chrome";v="112", "Not:A-Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
        }

    res = requests.get(
        url = url, headers= headers
    )
    print(res.json())
    file_id = res.json()['data']['fileid']

    print(f"Waiting request for {official_store_name} ads")
    for remaining in range(120, 0, -1):
        sys.stdout.write("\r")
        sys.stdout.write("{:2d} seconds remaining.".format(remaining))
        sys.stdout.flush()
        time.sleep(1)

    sys.stdout.write("\rRequest complete!            \n")
    
    return file_id
    

def download_file_to_gcs(start_time, end_time, file_id, official_store_name, storage_client):
    """
    Request content from shopee ads by API
    Args:
        start_time (float): start time for data to be extracted in UNIX.
        end_time (float): end time for data to be extracted in UNIX.
        file_id (str): file id needed to download file
        official_store_name (str): official store name for GCS
        storage_client (any): storage client for sirclo-data-prod
    Returns:
        None
    """
    # Get cookies from GCS
    json_object = get_json(official_store_name)
    cookies = json_object['cookies']

    # Call the API from Shopee and get excel content
    spc_cds = [{'value':f"{cookie['name']}={cookie['value']}"} for cookie in cookies if cookie['name'] == 'SPC_CDS']
    cookie = '; '.join([f"{cookie['name']}={cookie['value']}" for cookie in cookies])

    url = f"https://seller.shopee.co.id/api/marketing/v3/pas/report_file/?SPC_CDS={spc_cds[0]['value']}&SPC_CDS_VER=2&fileid={file_id}"

    headers = {
            'authority' : 'seller.shopee.co.id',
            'accept': 'application/json, application/force-download, */*',
            'accept-language': 'en-US,en;q=0.9',
            'cookie': cookie,
            'referer': f'https://seller.shopee.co.id/portal/marketing/pas/assembly?from={int(start_time)}&to={int(end_time)}&type=all&group=custom',
            'sec-ch-ua': '"Chromium";v="112", "Google Chrome";v="112", "Not:A-Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
        }

    res = requests.get(
        url = url, headers= headers
    )

    client = storage_client.get_bucket('sirclo-data-marketplace')
    df = res.content

    # Write excel to gcs
    blob = client.blob(f'assets/excel/shopee/ads/{official_store_name}/{date.today()}/Shopee_Ads_{(datetime.utcfromtimestamp(start_time)+timedelta(hours=7)).strftime("%Y-%m-%d")}_{(datetime.utcfromtimestamp(end_time)+timedelta(hours=7)).strftime("%Y-%m-%d")}.csv')

    with blob.open(mode='wb') as f:
        f.write(df)

def main(start_date, end_date, official_store_name, storage_client):
    """
    Get ads data by API
    Args:
        start_date (str): start date for data to be extracted.
        end_date (str): end date for data to be extracted.
        official_store_name (str): official store name for GCS
        storage_client (any): GCS client for sirclo-data-prod
    Returns:
        None
    """
    start_time = datetime.timestamp(datetime.strptime(start_date,"%Y-%m-%d"))
    end_time   = datetime.timestamp(datetime.strptime(end_date,"%Y-%m-%d"))

    file_id = request_file(start_time, end_time, official_store_name)
    download_file_to_gcs(start_time, end_time, file_id, official_store_name, storage_client)