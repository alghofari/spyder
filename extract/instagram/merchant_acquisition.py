import json
import os
import random
import re
import time
from datetime import datetime
from urllib.request import Request, urlopen

import gspread
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from google.cloud import bigquery
from pyvirtualdisplay import Display
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.wait import WebDriverWait

from helpers.webdriver_helper import set_webdriver


def extract_data(hashtag_data, hashtag):
    scrape_hashtags = []
    usernames = []
    for sections in range( 0 , len(hashtag_data['data']['top']['sections'])):
        for medias in range( 0 , len(hashtag_data['data']['top']['sections'][sections]['layout_content']['medias']) ):
            scrape_hashtags.append(hashtag + " | " + hashtag_data['data']['top']['sections'][sections]['layout_content']['medias'][medias]['media']['user']['username'] + " | " + hashtag_data['data']['top']['sections'][sections]['layout_content']['medias'][medias]['media']['code'] )
            usernames.append(hashtag_data['data']['top']['sections'][sections]['layout_content']['medias'][medias]['media']['user']['username'])
    for sections in range( 0 , len(hashtag_data['data']['recent']['sections'])):
        for medias in range( 0 , len(hashtag_data['data']['recent']['sections'][sections]['layout_content']['medias']) ):
            scrape_hashtags.append(hashtag + " | " + hashtag_data['data']['recent']['sections'][sections]['layout_content']['medias'][medias]['media']['user']['username'] + " | " + hashtag_data['data']['recent']['sections'][sections]['layout_content']['medias'][medias]['media']['code'] )
            usernames.append(hashtag_data['data']['recent']['sections'][sections]['layout_content']['medias'][medias]['media']['user']['username'])
    return scrape_hashtags, usernames


def scrape_hashtag(hashtag, driver):
    url = f'https://www.instagram.com/explore/tags/{hashtag}/?__a=1&__d=1'
    driver.get(url)
    resp_body = driver.find_element(By.TAG_NAME, "body").text
    data_json = json.loads(resp_body)
    hashtag_data = data_json
    scrape_hashtags, usernames = extract_data(hashtag_data, hashtag)
    return scrape_hashtags, usernames


def main_scrape_hashtags(hashtag, driver):
    scrape_hashtags_all= []
    usernames_all = []
    time.sleep(1)
    scrape_hashtags, usernames = scrape_hashtag(hashtag, driver)
    scrape_hashtags_all.append(scrape_hashtags)
    usernames_all.append(usernames)
    unique_usernames_set = {x for l in usernames_all for x in l}
    unique_usernames = list(unique_usernames_set)
    scrape_hashtags_results = scrape_hashtags_all
    return scrape_hashtags_results, usernames_all, unique_usernames


def scrape(username, driver):
    url = f'https://instagram.com/{username}/?__a=1&__d=1'
    driver.get(url)
    resp_body = driver.find_element(By.TAG_NAME, "body").text
    data_json = json.loads(resp_body)
    user_data = data_json['graphql']['user']
    return user_data


def extract_data_profile(user_data):
    profile = pd.DataFrame(np.random.randint(0,100,size=(1, 11)), columns=['name', 'username', 'followers','following','business_category','category', 'is_business_account','business_email','business_phone_number','biography','external_url'])
    profile['name'][0] = user_data['full_name']
    profile['username'][0]  = user_data['username']
    profile['followers'][0] = user_data['edge_followed_by']['count']
    profile['following'][0] = user_data['edge_follow']['count']
    profile['business_category'][0] = user_data['business_category_name']
    profile['category'][0] = user_data['category_name']
    if 'is_business_account' in user_data:
        profile['is_business_account'][0] = user_data['is_business_account']
    else:
        profile['is_business_account'][0] = False
    profile['business_email'][0] = user_data['business_email']
    profile['business_phone_number'][0] = user_data['business_phone_number']
    profile['biography'][0] = user_data['biography']
    if 'external_url' in user_data:
        profile['external_url'][0] = user_data['external_url']
    else:
        profile['external_url'][0] = "no links"
    return profile


def extract_data_post(user_data):
    post_nodes = pd.DataFrame(
        np.random.randint(0, 100, size=(len(user_data['edge_owner_to_timeline_media']['edges']), 7)),
        columns=['node_content', 'owner_username', 'shortcode', 'posted_at', 'post_caption', 'post_n_comments',
                 'post_n_likes'])
    if len(user_data['edge_owner_to_timeline_media']['edges']) > 0:
        for post in range(0, len(user_data['edge_owner_to_timeline_media']['edges'])):
            post_nodes['node_content'] = json.dumps(user_data['edge_owner_to_timeline_media']['edges'][post])
            post_nodes['shortcode'][post] = user_data['edge_owner_to_timeline_media']['edges'][post]['node'][
                'shortcode']
            timestamp = int(user_data['edge_owner_to_timeline_media']['edges'][post]['node']['taken_at_timestamp'])
            post_nodes['posted_at'][post] = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M')

            post_nodes['post_n_comments'][post] = user_data['edge_owner_to_timeline_media']['edges'][post]['node']['edge_media_to_comment']['count']
            post_nodes['post_n_likes'][post] = user_data['edge_owner_to_timeline_media']['edges'][post]['node']['edge_liked_by']['count']
            if len(user_data['edge_owner_to_timeline_media']['edges'][post]['node']['edge_media_to_caption']['edges']) > 0:
                if user_data['edge_owner_to_timeline_media']['edges'][post]['node']['edge_media_to_caption']['edges'][0]['node']['text']:
                    post_nodes['post_caption'][post] = user_data['edge_owner_to_timeline_media']['edges'][post]['node']['edge_media_to_caption']['edges'][0]['node']['text']
            post_nodes['owner_username'][post] = user_data['edge_owner_to_timeline_media']['edges'][post]['node']['owner']['username']
    return post_nodes


def main_scrape_account(usernames, driver):
    profiles2 =  pd.DataFrame(np.random.randint(0,100,size=(1, 11)) , columns=['name', 'username', 'followers','following','business_category','category','is_business_account','business_email','business_phone_number','biography','external_url'])
    posts2 =  pd.DataFrame(np.random.randint(0,100,size=(1, 7)) , columns=['node_content', 'owner_username', 'shortcode','posted_at','post_caption','post_n_comments','post_n_likes'])
    for username in usernames:
        time.sleep(1)
        user_data = scrape(username, driver)
        profile = extract_data_profile(user_data)
        post_nodes = extract_data_post(user_data)
        profiles2 = pd.concat([profiles2, profile])
        posts2 = pd.concat([posts2, post_nodes])
    return profiles2, posts2


def main(hashtags, driver):
    scrape_hashtags_results, usernames_all, unique_usernames = main_scrape_hashtags(hashtags, driver)
    profiles2, posts2 = main_scrape_account(unique_usernames, driver)
    return scrape_hashtags_results, profiles2, posts2


def login(instagram_username, instagram_password, driver):
    driver.get("http://www.instagram.com")
    user = WebDriverWait(driver, 10).until(ec.element_to_be_clickable((By.CSS_SELECTOR, "input[name='username']")))
    password = WebDriverWait(driver, 10).until(ec.element_to_be_clickable((By.CSS_SELECTOR, "input[name='password']")))
    user.clear()
    user.send_keys(instagram_username)
    time.sleep(5)
    password.clear()
    password.send_keys(instagram_password)
    time.sleep(5)
    WebDriverWait(driver, 5).until(ec.element_to_be_clickable((By.CSS_SELECTOR, "button[type='submit']"))).click()
    time.sleep(5)


def get_hashtags_list():
    gc = gspread.service_account(filename=os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
    sheet = gc.open_by_key('1PAz5PQe4lXtdfCGO-5TiiwbOUUt0vJyAsm6RdUVjfXw')
    worksheet = sheet.worksheet("merchant_acquisition_hashtags")
    return worksheet.col_values(1)


def write_to_table(schema_path, dataframe, table_id):
    client = bigquery.Client()
    schema_file = open(schema_path)
    schema_json = json.load(schema_file)
    job_config = bigquery.LoadJobConfig(
        schema=schema_json,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_APPEND",
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY
        )
    )
    job = client.load_table_from_dataframe(dataframe, table_id, job_config=job_config)
    job.result()


def extract_phone_number_from_bio(rows):
    string = str(rows)
    string2 = string.replace("-", "").replace(" ", "").replace(".", "")
    numbers = re.findall(r'\d+', string2)
    possible_phone_number = ""

    for j in range(0, len(numbers)):
        if (numbers[j][0:2] == "08" or numbers[j][0:3] == " '08" or numbers[j][0:3] == "628" or numbers[j][0:5] == " '628") and len(numbers[j]) >= 7:
            possible_phone_number = possible_phone_number, " , ", str(numbers[j])

    possible_phone_number = re.findall(r'\d+', str(possible_phone_number))
    return ','.join(possible_phone_number)


def extract_external_link(rows):
    if ("http" in str(rows)) and ("http://api.whatsapp.com/" not in str(rows)):
        #  tell the website that you are a scraper, so you don't get blocked
        user_agents_list = [
            'Mozilla/5.0 (iPad; CPU OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.83 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36'
        ]
        try:
            req = Request(url=rows, headers={'User-Agent': random.choice(user_agents_list)})
            whtml = urlopen(req).read()
            soup = BeautifulSoup(whtml, "lxml")
            links = ""
            for a in soup.find_all('a', href=True):
                links += a['href'] + " ||| "
            return links
        except Exception as e:
            print(e)
            return "Links not found"
    elif "http://api.whatsapp.com/" in str(rows):
        return str(rows)
    else:
        return "Links not found"


def extract_phone_number_from_link(rows):
    if "628" in str(rows):
        links = str(rows)
        index628_start = links.find("628")
        index_finish = index628_start + 20
        whatsapp1 = links[index628_start:index_finish]
        whatsapp1 = re.findall(r'\d+', whatsapp1)
        return ','.join(whatsapp1)
    else:
        return "Whatsapp not found"


def run(instagram_user, instagram_pass):
    display = Display(size=(1920, 1080))
    display.start()

    pd.set_option('mode.chained_assignment', None)
    driver = set_webdriver(additional_opts=['--headless'], driver_type="webdriver")

    login(instagram_user, instagram_pass, driver)
    hashtags_list = get_hashtags_list()
    print(hashtags_list)
    for hashtag_name in hashtags_list:
        hashtags_results, profiles, posts = main(hashtag_name, driver)

        profiles = profiles.iloc[1:]
        profiles['whatsapp_number_from_insta_bio'] = profiles.apply(lambda row : extract_phone_number_from_bio(row['biography']), axis=1)
        profiles['links'] = profiles.apply(lambda row : extract_external_link(row['external_url']), axis=1)
        profiles['whatsapp_number_from_insta_external_links'] = profiles.apply(lambda row : extract_phone_number_from_link(row['links']), axis=1)
        profiles = profiles.drop(columns=['links'])
        profiles = profiles.astype(str)
        profiles = profiles.rename(columns={"followers": "followers_count", "following": "following_count"})
        profile_schema = os.getcwd() + "/extract/instagram/schema/merchant_acquisition/profile.json"
        write_to_table(profile_schema, profiles, "sirclo-prod.bronze_instagram.profile_merchant_acquisition")

        posts = posts.iloc[1:]
        posts = posts.astype(str)
        post_schema = os.getcwd() + "/extract/instagram/schema/merchant_acquisition/post.json"
        write_to_table(post_schema, posts, "sirclo-prod.bronze_instagram.post_merchant_acquisition")

        hashtag_frames = []
        for hashtag_result in hashtags_results:
            new_hashtags_results = [i.split(' | ') for i in hashtag_result]
            print(new_hashtags_results)
            hashtag_dataframe = pd.DataFrame(new_hashtags_results, columns=['hashtag', 'username', 'shortcode'])
            hashtag_frames.append(hashtag_dataframe)
        hashtag_final_result = pd.concat(hashtag_frames, ignore_index=True)
        hashtag_schema = os.getcwd() + "/extract/instagram/schema/merchant_acquisition/hashtag.json"
        write_to_table(hashtag_schema, hashtag_final_result, "sirclo-prod.bronze_instagram.hashtag_merchant_acquisition")

    driver.quit()
    display.stop()