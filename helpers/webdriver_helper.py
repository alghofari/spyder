from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from fake_useragent import UserAgent
import undetected_chromedriver as uc
import seleniumwire.undetected_chromedriver as seleniumwire_webdriver
import os
import time
import requests
import urllib.request
import random

def set_webdriver(additional_opts=None, exp_opt_pref=None, driver_type='undetected_chromedriver'):
    if exp_opt_pref is None:
        exp_opt_pref = {}

    if additional_opts is None:
        additional_opts = []

    options = Options()

    default_opts = [
        '--no-sandbox',
        '--single-process',
        '--disable-dev-shm-usage',
        '--disable-blink-features=AutomationControlled',
        '--ignore-certificate-errors',
        '--ignore-ssl-errors=yes',
        '--allow-insecure-localhost',
        '--version'
        '--109.0.5414.119'
        'window-size=1920,1080',
        'disable-infobars',
        'user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
    ]

    for default_opt in default_opts:
        options.add_argument(default_opt)

    for additional_opt in additional_opts:
        options.add_argument(additional_opt)

    caps = options.to_capabilities()
    caps["acceptInsecureCerts"] = True

    driver = None

    if driver_type == 'undetected_chromedriver':
        driver = uc.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=options,
            desired_capabilities=caps, 
            version_main=109
        )
    elif driver_type == 'webdriver':
        options.add_experimental_option('useAutomationExtension', False)
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("prefs", exp_opt_pref)

        caps['browserVersion'] = "109"

        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            desired_capabilities=caps,
            options=options
        )
    elif driver_type == 'seleniumwire':
        driver = seleniumwire_webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=options,
            desired_capabilities=caps,
            version_main=109
        )

    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source":
            "const newProto = navigator.__proto__;"
            "delete newProto.webdriver;"
            "navigator.__proto__ = newProto;"
    })

    return driver

def get_public_ip():
    try:
        response = requests.get('https://api64.ipify.org?format=json')  # Using ipify API
        if response.status_code == 200:
            return response.json()['ip']
        else:
            return "Unable to fetch IP address."
    except Exception as e:
        return str(e)


def connect_to_vpn(openvpn_config):
    # openvpn_config = os.environ.get('OPENVPN_INDO_CONFIG')
    ip_address = get_public_ip()
    print("IP address is:", ip_address)
    openvpn_credentials = os.environ.get('OPENVPN_CREDENTIALS')
    command = f"openvpn --config {openvpn_config} --auth-user-pass {openvpn_credentials} --daemon"
    os.system(command)
    print("Starting VPN...")
    time.sleep(15)
    ip_address = get_public_ip()
    print("IP address is:", ip_address)

def connect_to_oxylab(): 
    entry = 'http://customer-rsirclo-cc-id:Kp8VCzN9NiTS%@pr.oxylabs.io:7777'
    proxies = {
        "http": entry,
        "https": entry,
    }
    print("finish connect to oxylab")

    response = requests.request(
    'GET',
    'https://ip.oxylabs.io',
    proxies=proxies,
    )

    print(response.text)

    return proxies

# Function to generate a random user-agent header
def get_random_user_agent():
    ua = UserAgent()
    headers = {'User-Agent': ua.random}
    return headers