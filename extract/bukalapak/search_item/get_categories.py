import os
import pathlib
import random
from datetime import datetime
import requests

import gspread
import pandas as pd
from seleniumwire import webdriver
from seleniumwire.utils import decode
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
import time
import re


def capitalize_and_remove_numbers(text):
    # Capitalize the first letter of each word in the string
    capitalized_text = text.title()

    # Remove numbers from the string using regular expressions
    cleaned_text = re.sub(r'\d+', '', capitalized_text)

    return cleaned_text.strip()


# Function to setup webdriver chrome config
def set_webdriver(proxy_ip=''):
    chrome_options = webdriver.ChromeOptions()
    # chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--no-sandbox")
    if proxy_ip != '':
        chrome_options.add_argument(f"--proxy-server={proxy_ip}")
    chrome_options.add_experimental_option("excludeSwitches", ['enable-automation'])
    chrome_options.set_capability("browserVersion", "109")
    return webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_options
    )

driver = set_webdriver()

driver.get("https://www.bukalapak.com/c?from=nav_header")
time.sleep(3)

# Parse the HTML
soup = BeautifulSoup(driver.page_source, 'html.parser')
driver.quit()

# Find all anchor tags with class "category-list" and extract the href attribute
href_list = [a['href'] for a in soup.find_all('a', class_='category-list ut-category-subcategory')]
link_list = [f"https://www.bukalapak.com{link}" for link in href_list]
print(link_list)

# Clean result above
category = [capitalize_and_remove_numbers(link.split('/')[-1].replace('-',' ')) for link in href_list]
print(category)

# Make a dataframe 

data = {'Category': category, 'url': link_list}
df = pd.DataFrame(data)
df.to_csv("list_categories.csv", index=False)

