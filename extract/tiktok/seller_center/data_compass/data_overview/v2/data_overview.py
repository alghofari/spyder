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
    # go to data overview page
    driver.get("https://seller-id.tiktok.com/compass/data-overview")
    time.sleep(15)

    # data compass navigation menu is upgraded
    skip_pop_up(driver, element_type="css selector", element_value="button.sc-bkzZxe:nth-child(1) > button:nth-child(1) > span:nth-child(1)")
    skip_pop_up(driver, element_type="xpath", element_value="/html/body/div[5]/div[4]/div/div[2]/div/button[1]/button/span")

    # Ketahui selengkapnya tentang performa toko Anda.
    skip_pop_up(driver, element_type="css selector", element_value=".theme-m4b-button-dark-outline")
    skip_pop_up(driver, element_type="xpath", element_value="/html/body/div[1]/section/section/div/main/div[3]/span/div[1]/div/div/div[2]/div/div/div/div[2]/button[1]")

    # click date picker button
    driver.find_element(by=By.CSS_SELECTOR, value=".theme-arco-picker").click()
    time.sleep(5)

    # click date filter button
    driver.find_element(by=By.CSS_SELECTOR, value="button.theme-arco-btn-secondary:nth-child(2)").click()
    time.sleep(5)

    # click export button
    driver.find_element(by=By.CSS_SELECTOR, value="button.theme-arco-btn-text:nth-child(1)").click()
    time.sleep(20)


def main(driver, store_name):
    try:
        download_data(driver)
    except Exception as e:
        print(f"not able to collect data overview data from {store_name}")
        print(e)
        pass
