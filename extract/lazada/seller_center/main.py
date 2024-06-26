import os

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from extract.lazada.seller_center import login
from extract.lazada.seller_center import order as extract_order
from transform.lazada.seller_center.orders import order as transform_order


def set_webdriver(download_path):
    preferences = {
        "credentials_enable_service": False,
        "download.default_directory": download_path,
        "download.prompt_for_download": False,
        "profile.password_manager_enabled": False
    }

    options = webdriver.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--headless")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_experimental_option("prefs", preferences)

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()),
                              options=options)

    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

    user_agents = ["Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"]
    for i in range(len(user_agents)):
        driver.execute_cdp_cmd("Network.setUserAgentOverride", {"userAgent": user_agents[i]})

    return driver


def execute(store_name):
    assets_path = os.getcwd() + "/assets"
    download_path = f"{assets_path}/data/lazada/{store_name}"
    os.makedirs(download_path, exist_ok=True)

    # extract
    driver = set_webdriver(download_path)
    login.main(driver, store_name)
    extract_order.main(driver, store_name)
    driver.quit()

    # transform and load
    transform_order.main(download_path, store_name)
