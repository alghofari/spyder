import os

from selenium import webdriver
from selenium.webdriver.firefox.options import Options

from extract.propsid import login, publisher, video_publisher


def set_webdriver(browser_path: str) -> webdriver:
    options = Options()
    options.add_argument("-headless")
    options.add_argument("--width=1920")
    options.add_argument("--height=1080")

    return webdriver.Firefox(
        executable_path=browser_path,
        options=options
    )


def execute(username: str, password: str):
    assets_path = os.getcwd() + "/assets"
    browser_path = f"{assets_path}/browser/geckodriver"
    driver = set_webdriver(browser_path)

    try:
        login.main(driver, username, password)
        publisher.main(driver)
        video_publisher.main(driver)
    except Exception as e:
        print(e)
        driver.quit()
