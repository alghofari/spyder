import os
import pandas as pd

from datetime import date
from datetime import timedelta
from selenium.webdriver.common.by import By
from helpers.time_helper import sleep_condition

from selenium.webdriver.common.action_chains import ActionChains
from extract.shopee.seller_center.helper.helper import click_action

def get_stream_data(driver, storage_client, config):
    """

    Function to get traffic data by element

    Args:

        driver (any): chromedriver.Driver used

        storage_client (any): storage.Client for sirclo-data-prod

        official_store_name (str): official store name for GCS

    Returns:

        None

    """
    driver.get("https://seller.shopee.co.id/datacenter/marketing/content/livestreaming")
    sleep_condition(30,60)
    
    official_store_name, _, seller_center_pass, _, _ = config
    try:
        click_action(driver, css_selector=".shopee-input__inner.shopee-input__inner--large .shopee-input__input").send_keys(seller_center_pass)
        sleep_condition(10,20)
        click_action(driver, css_selector=".lm-actions > button:nth-child(2)").click()
        sleep_condition(50, 70)
    except:
        pass

    sleep_condition(30,50)
    
    days_button = driver.find_element(By.CSS_SELECTOR, '.track-click-normal-export.shopee-button.shopee-button--normal')
    ActionChains(driver).move_to_element(days_button).click(days_button).perform()

    try:
        click_action(driver, css_selector=".shopee-input__inner.shopee-input__inner--large .shopee-input__input").send_keys(seller_center_pass)
        sleep_condition(10,20)
        click_action(driver, css_selector=".lm-actions > button:nth-child(2)").click()
        sleep_condition(50, 70)
    except:
        pass
    
    sleep_condition(50, 70)
    
    for path, subdirs, files in os.walk(f"{os.getcwd()}"):
        for file in files:
            if file.endswith(".xlsx"):
                path = f"{os.getcwd()}/download_folder/{file}"

                start_date = date.today()
                end_date   = start_date - timedelta(days=30)

                client  = storage_client.get_bucket('sirclo-data-marketplace')
                blob    = client.blob(f'assets/excel/shopee/live_stream/{official_store_name}/{date.today()}/Shopee_Live_Stream_{end_date}_{start_date}.xlsx')
                
                blob.upload_from_filename(path)

                os.remove(path)


def get_stream_detail_data(driver, storage_client, config):
    official_store_name, _, _, _, _ = config
    detailed_streams = driver.find_elements(By.CSS_SELECTOR, ".shopee-button.shopee-button--link.shopee-button--normal.track-click-list-details.track-click-list-details-0")
    df_all = pd.DataFrame()
    
    # This blocks for multiple page
    
    # max_pages = driver.find_elements(By.CSS_SELECTOR, ".shopee-pager__page")
    # max_pages = int(max_pages[-1].text) if max_pages else None    
    # if max_pages:
    #     for page in range(1,max_pages):
            
    #         for stream in detailed_streams:
    #             ActionChains(driver).move_to_element(stream).click(stream).perform()
                
    #             sleep_condition(40,60)
    #             driver.switch_to.window(driver.window_handles[1])
    #             download_button = driver.find_element(By.XPATH, '/html/body/div[1]/div[2]/div/div/div[2]/div/div/div/div/div/div[2]/div[1]/button')
    #             ActionChains(driver).move_to_element(download_button).click(download_button).perform()
                
    #             sleep_condition(20,30)
    #             for path, subdirs, files in os.walk(f"{os.getcwd()}"):
    #                 for file in files:
    #                     if file.endswith(".xlsx"):
    #                         path = f"{os.getcwd()}/download_folder/{file}"
    #                         df = pd.read_excel(path, usecols='A:X')
    #                         df = df.head(1)
    #                         df_all = pd.concat([df_all, df])
                            
    #                         os.remove(path)
                
    #             driver.close()
                
    #             sleep_condition(70,90)
    #             driver.switch_to.window(driver.window_handles[0])
    #             sleep_condition(10,20)
                
    #         next_button = driver.find_element(By.CSS_SELECTOR, ".shopee-button.shopee-button--small.shopee-button--frameless.shopee-button--block.shopee-pager__button-next")
    #         ActionChains(driver).move_to_element(next_button).click(next_button).perform()
            
    #         sleep_condition(5,10)
    # else:
    
    for stream in detailed_streams:
        ActionChains(driver).move_to_element(stream).click(stream).perform()
            
        sleep_condition(10,20)

    try:
        for handle in driver.window_handles[::-1]:
            driver.switch_to.window(handle)
            print(driver.current_url)
            if driver.current_url.split("/")[-1] != "livestreaming" or driver.current_url.split("/")[-1] != "conversations":
            
                sleep_condition(20,30)
                download_button = driver.find_element(By.XPATH, '/html/body/div[1]/div[2]/div/div/div[2]/div/div/div/div/div/div[2]/div[1]/button')
                ActionChains(driver).move_to_element(download_button).click(download_button).perform()
                
                sleep_condition(20,30)
                for path, subdirs, files in os.walk(f"{os.getcwd()}"):
                    for file in files:
                        if file.endswith(".xlsx"):
                            path = f"{os.getcwd()}/download_folder/{file}"
                            df = pd.read_excel(path, usecols='A:X')
                            df = df.head(1)
                            df_all = pd.concat([df_all, df])
                            
                            os.remove(path)
                
                driver.close()
                
                sleep_condition(70,90)
            else:
                continue
    except:
        pass

    if not df_all.empty:
        df_path = f"{os.getcwd()}/download_folder/livestream_detail.xlsx"
        df_all = df_all.to_excel(df_path, index=False)
        
        start_date = date.today()
        end_date   = start_date - timedelta(days=30)

        client  = storage_client.get_bucket('sirclo-data-marketplace')
        blob    = client.blob(f'assets/excel/shopee/live_stream_detail/{official_store_name}/{date.today()}/Shopee_Live_Stream_Detail_{end_date}_{start_date}.xlsx')
        
        blob.upload_from_filename(f"{os.getcwd()}/download_folder/livestream_detail.xlsx")

        os.remove(df_path)
    

def main(driver, storage_client, config):
    get_stream_data(driver, storage_client, config)
    get_stream_detail_data(driver, storage_client, config)