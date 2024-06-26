from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec


def main(driver: webdriver, username: str, password: str):
    driver.get("https://dash.props.id/accounts/login/")
    username_input = WebDriverWait(driver, 10).until(
        ec.element_to_be_clickable((By.CSS_SELECTOR, "input[name='login']")))
    password_input = WebDriverWait(driver, 10).until(
        ec.element_to_be_clickable((By.CSS_SELECTOR, "input[name='password']")))
    username_input.clear()
    username_input.send_keys(username)
    password_input.clear()
    password_input.send_keys(password)
    WebDriverWait(driver, 2).until(ec.element_to_be_clickable((By.CSS_SELECTOR, "button[type='submit']"))).click()
