import os
import re
import json
import pathlib
import requests
import logging
import pandas as pd
from urllib.parse import urlparse, parse_qs, unquote
import time
import gspread
import random
from bs4 import BeautifulSoup
from helpers.cloud_storage_helper import upload_blob_to_gcs, list_blob_gcs, download_blob_to_local
from helpers.webdriver_helper import set_webdriver, connect_to_vpn, connect_to_oxylab
from helpers.time_helper import sleep_condition
from pyvirtualdisplay import Display
from helpers.bigquery_helper import df_to_bq, client

USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36'


def get_proxies(list_ip):
    proxy = random.choice(list_ip)
    proxies = {"http": proxy, "https": proxy}
    return proxies


def find_url(html_item):
    try:
        value = html_item.find("a", {"class": "css-54k5sq"}, href=True)['href']
        value = re.sub('.*' + 'r=', '', value)
        value = re.sub('%3F.*', '', value)
        value = unquote(value)
    except Exception:
        try:
            value = html_item.find("a", {"class": "css-89jnbj"}, href=True)['href']
            value = re.sub('.*' + 'r=', '', value)
            value = re.sub('%3F.*', '', value)
            value = unquote(value)
        except Exception:
            value = None
    return value


def get_url(driver, page: int, category: str, sort_by="5"):
    list_url = []
    driver.get(f"https://tokopedia.com/p/{category}?page={page}&ob={sort_by}")
    time.sleep(5)
    for i in range(20):
        driver.execute_script("window.scrollBy(0,250)")

    driver.execute_script("window.scrollBy(0,50)")
    html = driver.page_source
    html = BeautifulSoup(html, 'html.parser')
    all_items = html.findAll('div', {'class': 'css-bk6tzz e1nlzfl2'})
    for item_html in all_items:
        url = find_url(item_html)
        list_url.append(url)

    return list_url


def dict_to_gcs(data: dict, bucket_name: str, filename: str, blob_name: str):
    with open(filename, "w") as file:
        json.dump(data, file, indent=4)
    upload_blob_to_gcs(bucket_name=bucket_name, local_file_name=filename, gcs_blob_name=blob_name)
    return


def get_request_raw(url):
    headers = requests.utils.default_headers()
    headers['User-Agent'] = USER_AGENT
    res = requests.get(url, headers=headers)
    if not res.ok:
        raise Exception('Failed to get request products to the Category Page')
    return res.text


def post_gql_request(data, list_proxy):
    headers = requests.utils.default_headers()
    list_ua = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36']
    headers['User-Agent'] = random.choice(list_ua)
    headers['content-type'] = 'application/json'
    headers['origin'] = 'https://www.tokopedia.com'
    headers['x-tkpd-akamai'] = 'pdpGetLayout'
    headers['x-tkpd-lite-service'] = 'zeus'
    headers['x-device'] = 'desktop'
    if not list_proxy:
        res = requests.post(
            'https://gql.tokopedia.com/',
            json=data, headers=headers
        )
    else:
        proxies = list_proxy
        res = requests.post(
            'https://gql.tokopedia.com/', proxies=proxies,
            json=data, headers=headers
        )

    return res.json()[0]


def get_product_data(url, list_proxy):
    url_parsed = urlparse(url)
    if url_parsed.netloc == 'www.tokopedia.com':
        shopdomain, product_key = (
            path for path in url_parsed.path.split('/') if path)
    elif url_parsed.netloc == 'ta.tokopedia.com':
        queries = parse_qs(url_parsed.query)
        if queries.get('r'):
            shopdomain, product_key = (
                path for path in urlparse(queries['r'][0]).path.split('/') if path)
    else:
        return

    res = post_gql_request([
        {
            "operationName": "PDPGetLayoutQuery",
            "variables": {
                "shopDomain": shopdomain,
                "productKey": product_key,
                "layoutID": "",
                "apiVersion": 1,
                "userLocation": {
                    "addressID": "0",
                    "postalCode": "",
                    "latlon": ""
                },
                "extParam": ""
            },
            "query": "fragment ProductVariant on pdpDataProductVariant {\n  errorCode\n  parentID\n  defaultChild\n  sizeChart\n  variants {\n    productVariantID\n    variantID\n    name\n    identifier\n    option {\n      picture {\n        urlOriginal: url\n        urlThumbnail: url100\n        __typename\n      }\n      productVariantOptionID\n      variantUnitValueID\n      value\n      hex\n      __typename\n    }\n    __typename\n  }\n  children {\n    productID\n    price\n    priceFmt\n    optionID\n    productName\n    productURL\n    picture {\n      urlOriginal: url\n      urlThumbnail: url100\n      __typename\n    }\n    stock {\n      stock\n      isBuyable\n      stockWordingHTML\n      minimumOrder\n      maximumOrder\n      __typename\n    }\n    isCOD\n    isWishlist\n    campaignInfo {\n      campaignID\n      campaignType\n      campaignTypeName\n      campaignIdentifier\n      background\n      discountPercentage\n      originalPrice\n      discountPrice\n      stock\n      stockSoldPercentage\n      startDate\n      endDate\n      endDateUnix\n      appLinks\n      isAppsOnly\n      isActive\n      hideGimmick\n      isCheckImei\n      __typename\n    }\n    thematicCampaign {\n      additionalInfo\n      background\n      campaignName\n      icon\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\n\nfragment ProductMedia on pdpDataProductMedia {\n  media {\n    type\n    urlThumbnail: URLThumbnail\n    videoUrl: videoURLAndroid\n    prefix\n    suffix\n    description\n    __typename\n  }\n  videos {\n    source\n    url\n    __typename\n  }\n  __typename\n}\n\nfragment ProductHighlight on pdpDataProductContent {\n  name\n  price {\n    value\n    currency\n    __typename\n  }\n  campaign {\n    campaignID\n    campaignType\n    campaignTypeName\n    campaignIdentifier\n    background\n    percentageAmount\n    originalPrice\n    discountedPrice\n    originalStock\n    stock\n    stockSoldPercentage\n    threshold\n    startDate\n    endDate\n    endDateUnix\n    appLinks\n    isAppsOnly\n    isActive\n    hideGimmick\n    __typename\n  }\n  thematicCampaign {\n    additionalInfo\n    background\n    campaignName\n    icon\n    __typename\n  }\n  stock {\n    useStock\n    value\n    stockWording\n    __typename\n  }\n  variant {\n    isVariant\n    parentID\n    __typename\n  }\n  wholesale {\n    minQty\n    price {\n      value\n      currency\n      __typename\n    }\n    __typename\n  }\n  isCashback {\n    percentage\n    __typename\n  }\n  isTradeIn\n  isOS\n  isPowerMerchant\n  isWishlist\n  isCOD\n  isFreeOngkir {\n    isActive\n    __typename\n  }\n  preorder {\n    duration\n    timeUnit\n    isActive\n    preorderInDays\n    __typename\n  }\n  __typename\n}\n\nfragment ProductCustomInfo on pdpDataCustomInfo {\n  icon\n  title\n  isApplink\n  applink\n  separator\n  description\n  __typename\n}\n\nfragment ProductInfo on pdpDataProductInfo {\n  row\n  content {\n    title\n    subtitle\n    applink\n    __typename\n  }\n  __typename\n}\n\nfragment ProductDetail on pdpDataProductDetail {\n  content {\n    title\n    subtitle\n    applink\n    showAtFront\n    isAnnotation\n    __typename\n  }\n  __typename\n}\n\nfragment ProductDataInfo on pdpDataInfo {\n  icon\n  title\n  isApplink\n  applink\n  content {\n    icon\n    text\n    __typename\n  }\n  __typename\n}\n\nfragment ProductSocial on pdpDataSocialProof {\n  row\n  content {\n    icon\n    title\n    subtitle\n    applink\n    type\n    rating\n    __typename\n  }\n  __typename\n}\n\nquery PDPGetLayoutQuery(\n  $shopDomain: String\n  $productKey: String\n  $layoutID: String\n  $apiVersion: Float\n  $userLocation: pdpUserLocation\n  $extParam: String\n) {\n  pdpGetLayout(\n    shopDomain: $shopDomain\n    productKey: $productKey\n    layoutID: $layoutID\n    apiVersion: $apiVersion\n    userLocation: $userLocation\n    extParam: $extParam\n  ) {\n    name\n    pdpSession\n    basicInfo {\n      alias\n      isQA\n      id: productID\n      shopID\n      shopName\n      minOrder\n      maxOrder\n      weight\n      weightUnit\n      condition\n      status\n      url\n      needPrescription\n      catalogID\n      isLeasing\n      isBlacklisted\n      menu {\n        id\n        name\n        url\n        __typename\n      }\n      category {\n        id\n        name\n        title\n        breadcrumbURL\n        isAdult\n        detail {\n          id\n          name\n          breadcrumbURL\n          isAdult\n          __typename\n        }\n        __typename\n      }\n      txStats {\n        transactionSuccess\n        transactionReject\n        countSold\n        paymentVerified\n        itemSoldPaymentVerified\n        __typename\n      }\n      stats {\n        countView\n        countReview\n        countTalk\n        rating\n        __typename\n      }\n      __typename\n    }\n    components {\n      name\n      type\n      position\n      data {\n        ...ProductMedia\n        ...ProductHighlight\n        ...ProductInfo\n        ...ProductDetail\n        ...ProductSocial\n        ...ProductDataInfo\n        ...ProductCustomInfo\n        ...ProductVariant\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n}\n"
        }
    ], list_proxy)
    return res


def choose_vpn_filename():
    bucket = 'sirclo-data-marketplace'
    prefix = 'assets/ovpn/'
    vpn_profile_list = list_blob_gcs(bucket_name=bucket, prefix_name=prefix)
    vpn_profile_list.remove(prefix)

    picked_profile = random.choice(vpn_profile_list)
    print(f"the profile used is {picked_profile}")

    # download vpn profile from cloud storage
    vpn_filename = picked_profile.split('/')[-1]
    download_blob_to_local(bucket_name=bucket, local_file_name=vpn_filename, gcs_blob_name=picked_profile)
    time.sleep(5)
    return vpn_filename


def get_products(url_product: list, list_proxy, vpn_use):
    products_data = []
    try:
        for url in url_product:
            products_data.append(get_product_data(url, list_proxy))
            sleep_condition(1, 2)
    except Exception as e:
        print("Error on getting item url: ", url, e)
        if vpn_use == "yes":
            vpn_filename = choose_vpn_filename()
            connect_to_vpn(vpn_filename)
    return products_data


def get_categories():
    google_application_credentials = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
    gc = gspread.service_account(filename=google_application_credentials)
    sh = gc.open_by_key('1jvrwFwpunb25RYeRxVtg_dZqrF8o-XVmByKwl-LFv5A')
    worksheet = sh.worksheet("Tokopedia Schedule")
    return worksheet.col_values(1)[1:]


def get_csv_column(filename, column_name):
    import csv
    with open(filename, 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        column = [row[column_name] for row in reader]
    return column


def parsing_json(data: dict):
    # excluded_columns = []
    data = data["data"]["pdpGetLayout"]
    df = pd.json_normalize(data["basicInfo"])
    for row in data["components"]:
        if row["name"] == "product_content":
            content = row["data"][0]
            df_content = pd.json_normalize(content)
            df = pd.concat([df, df_content], axis=1)
    column_names = list(df.columns.values)
    column_names = [text.replace('__', '').replace('.', '__').lower() for text in column_names]
    df.columns = column_names
    return df


def main(start: int, end: int, bucket_name: str, base_path: str, run_date: str, timestamp, table_name: str, schema_path: str, categories_csv: str, proxy_use="no", past_data="no", vpn_use="no", bootstrap_servers: str = 'kafka-cp-kafka-headless.kafka:9092', kafka_topic: str = 'spyder_tokopedia_item', sort_by: str = "5", retry_count=0):
    if past_data == "yes":
        with open("./extract/tokopedia/search_item/past_data_insert_query.sql", 'r') as file:
            query_insert = file.read()
        insert_job = client.query(query_insert)
        insert_job.result()
        print("Data from query has been loaded")
    # try:
    #     from kafka import KafkaProducer
    #     producer = KafkaProducer(
    #     bootstrap_servers=[bootstrap_servers],
    #     value_serializer=lambda x: json.dumps(x).encode('utf-8')
    #     )
    # except:
    #     producer = None
    opts = ['--headless']
    driver_type = "webdriver"
    list_proxy = []
    proxies = []
    if proxy_use == "yes":
        opts = []
        proxies = connect_to_oxylab()
        opts.append(f'--proxy-server={proxies}')
        list_proxy.append(proxies['https'])
        driver_type = "seleniumwire"
        # response = requests.get("http://10.103.255.45:8000/indo")
        # response_dict = response.json()
        # list_proxy = response_dict["address"]
    if vpn_use == "yes":
        vpn_filename = choose_vpn_filename()
        connect_to_vpn(vpn_filename)
    base_dir = os.getcwd() + f'/{base_path}/{run_date}'
    pathlib.Path(base_dir).mkdir(parents=True, exist_ok=True)
    short_category = None
    list_url = None
    categories = get_csv_column(categories_csv, "Category URL")
    # if start == 1 and retry_count == 0:
    #     categories = get_csv_column("./extract/tokopedia/search_item/tokopedia_categories.csv", "Category URL")
    #     random.shuffle(categories)
    # else:
    #     categories = [category]
    # max_retry = 3
    # proxies = get_proxies(list_proxy)

    for i in range(len(categories)):
        try:
            if driver_type == "seleniumwire":
                display = Display(size=(1920, 1080))
                display.start()
            driver = set_webdriver(additional_opts=opts, driver_type=driver_type)
        except Exception as e:
            print("Crashed webdriver...!", e)
            continue

        url_category = categories[i]
        print(url_category)
        category = url_category.split('/', 4)[4]
        # message = {'marketplace': 'tokopedia', 'job_type': 'extract', 'page_type': 'search_item',
        # 'start_page': start, 'end_page': end, 'category_name': category,
        # 'bucket_name': bucket_name, 'base_path': base_path, 'sort_by': sort_by,'run_date': run_date}
        for page in range(start, end):
            # if message["start_page"] != page or len(categories) > 1:
            #     retry_count = 0
            #     category = category.split('/',4)[4]
            #     message["category_name"] = category
            print(f"Doing page number {page}")
            try:
                list_url = []
                driver.get(f"https://www.tokopedia.com/p/{category}?ob={sort_by}&page={page}")
                time.sleep(5)
                for i in range(20):
                    driver.execute_script("window.scrollBy(0,250)")
                driver.execute_script("window.scrollBy(0,50)")
                html = driver.page_source
                html = BeautifulSoup(html, 'html.parser')
                all_items = html.findAll('div', {'class': 'css-bk6tzz e1nlzfl2'})
                for item_html in all_items:
                    url = find_url(item_html)
                    list_url.append(url)
            except Exception as e:
                print("Error while getting category page ", e)
                if vpn_use == "yes":
                    vpn_filename = choose_vpn_filename()
                    connect_to_vpn(vpn_filename)
                # if producer is None:
                #     print(e)
                #     continue
                # else:
                #     if retry_count <= max_retry:
                #         retry_count = retry_count + 1
                #         message["start_page"] = page
                #         message["end_page"] = page + 1
                #         message["retry_count"] = retry_count
                #         producer.send(kafka_topic, value=message)
                #         print(f'Producer send {kafka_topic}')
                #         continue
                #     else:
                #         print("Max Retry Exceeded")
                #         break

            try:
                products = get_products(list_url, proxies, vpn_use)
                df_final = pd.DataFrame()
                print(f"Products has been retrieved, {len(products)}")
                for j in range(len(products)):
                    short_category = category.rsplit('/', 2)[-1]
                    filename = f"{base_dir}/{short_category}_{page}_{j}.json"
                    blob_name = f"{base_path}/{run_date}/{short_category}_{page}_{j}.json"
                    try:
                        dict_to_gcs(products[j], bucket_name, filename, blob_name)
                    except Exception as e:
                        print("Error while uploading to GCS", e)
                    data = products[j]
                    try:
                        df = parsing_json(data)
                        df.insert(0, 'category_name', short_category)
                        df.insert(0, 'load_timestamp', timestamp)
                        df = df.drop("typename", axis=1)
                        df_final = pd.concat([df_final, df], ignore_index=True)
                    except Exception as e:
                        print("Error while parsing json", e)
                        continue
                try:
                    df_to_bq(df_final, table_name, schema_path, "load_timestamp", "search_item")
                except Exception as e:
                    print("wrong schema for category ", short_category, e)
                    continue
                # end = time.time()
                # print("The time of execution of above program is :",(end - start) * 10 ** 3, "ms")
                print(f"Data has been inserted to {table_name}")
            except Exception as e:
                print("Error while getting products from list url", e)
                continue
    driver.quit()
