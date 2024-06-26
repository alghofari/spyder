import time

from selenium.webdriver.common.by import By


def skip_pop_up(driver, element_type, element_value):
    try:
        driver.find_element(by=element_type, value=element_value).click()
        time.sleep(1)
    except Exception as e:
        print(e)
        pass


def download_data(driver):
    # go to service analysis page
    driver.get("https://seller-id.tiktok.com/compass/service-analytics")
    time.sleep(15)

    # data compass navigation menu is upgraded
    skip_pop_up(driver, element_type="css selector", element_value="button.sc-jrAGrp:nth-child(1) > button:nth-child(1) > span:nth-child(1)")
    skip_pop_up(driver, element_type="xpath", element_value="/html/body/div[4]/div[4]/div/div[2]/div/button[1]/button/span")

    # click chat details
    driver.find_element(by=By.CSS_SELECTOR, value="#theme-arco-tabs-0-tab-1").click()
    time.sleep(5)

    # click export button
    driver.find_element(by=By.CSS_SELECTOR, value="div.mb-16:nth-child(2) > div:nth-child(1) > button:nth-child(1) > span:nth-child(2)").click()
    time.sleep(20)


def main(driver, store_name):
    try:
        download_data(driver)
    except Exception as e:
        print(f"not able to collect service analysis data from {store_name}")
        print(e)
        pass
