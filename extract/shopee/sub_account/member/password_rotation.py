import time

from selenium.webdriver.chrome.webdriver import WebDriver

from extract.shopee.seller_center.helper.helper import click_action


def main(driver: WebDriver, config: tuple):
    _, vaultwarden_email, seller_center_pass, official_store_id, ned_email, main_account_email, main_account_password, main_account_otp_id = config

    # open member page
    driver.get("https://subaccount.shopee.com/member")
    time.sleep(5)

    # search subakun
    click_action(
        driver,
        css_selector="body > div.app > div > div.page-container.member-list > div.member-filter.list-filter > div.shopee-input.search > div > input"
    ).send_keys("subakun")
    time.sleep(5)

    # click rincian button
    click_action(
        driver,
        css_selector="body > div.app > div > div.page-container.member-list > div.shopee-complex-table.member-table.list-table > div > div.shopee-table > div.shopee-table__body-container > div.shopee-table__main-body > div > div > div.shopee-scrollbar__content > table > tbody > tr > td.is-last > div > button"
    ).click()
    time.sleep(5)

    # click ubah password button
    click_action(
        driver,
        css_selector="body > div.app > div > div.member-detail > div.detail-container > div.detail-info > div:nth-child(2) > div > div:nth-child(2) > div.info-block-item-content > button"
    ).click()
    time.sleep(5)

    # click reset password oleh subakun
    click_action(
        driver,
        css_selector="body > div:nth-child(5) > div > div > div > div > div > div:nth-child(1) > div > div.reset-member-pwd__content > div:nth-child(3) > label > span.shopee-radio__label"
    ).click()
    time.sleep(5)
    time.sleep(300)
