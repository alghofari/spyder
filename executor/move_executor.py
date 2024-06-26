from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.by import By
from helpers.time_helper import sleep_condition


def passing_url_to_move(driver, wait, url_to_find, url_identifier, start_sleep, end_sleep):
    """
    This function is used to auto move into different page based on url in the tags in page

    Params: 
    driver            (driver)      | Required  : WebDriver command that will drive the process of scrapping specific define tags
    wait              (condition)   | Required  : Combination of wait time on specific range time, to give spare time for perform the specific define tags
    url_to_find       (str)         | Required  : Name of the url based on specific url that will be combine into main url (Ex: seller.tokopedia.com/ -> myshop_order?status=new_order)
    url_identifier    (str)         | Required  : Name of the status on specific url (Ex: "new_order")
    start_sleep       (int)         | Required  : Number to start the range interval the randomize wait time
    end_sleep         (int)         | Required  : Number to end the range interval the randomize wait time

    Exampe:
    passing_pop_up(driver, wait, "Lewati", "button", 5, 10)

    """
    # Start the process by define the variable
    sleep_condition(start_sleep, end_sleep)

    # Get all the tags in the page
    search_url_in_page_source = driver.page_source

    # Define and get the xpath based on the tags
    element_url = f'//a[contains(@href, "{url_to_find}")]'
    print(element_url)

    # Find the url tags in the page
    if url_to_find in search_url_in_page_source:
        print(f"url {url_to_find} is already located in the body page")

        try:
            # Wait and move to other url
            # wait_appear_pop_up_identifier = wait.until(EC.visibility_of_element_located((By.XPATH, element_url)))
            # wait_click_pop_up_identifier
            wait.until(ec.element_to_be_clickable((By.XPATH, element_url)))

            sleep_condition(start_sleep, end_sleep)
            # find_tags_pop_up_identier
            driver.find_element("xpath", element_url).click()

            print(f"We already clicked the url {url_to_find}")

            sleep_condition(start_sleep, end_sleep)
            # check_move_url = wait.until(EC.url_contains(f"{url_identifier}"))

            url_to_move = url_to_find.replace("/", "")
            full_url = f"{url_identifier}{url_to_move}"
            print(f"Start to scrapping in page {full_url}")

        except Exception as e:
            print(e)
            print(f"{element_url} can not be performed and visible, try to look the condition now")
    else:
        print(f"There is no {url_to_find} in page source, try to look at the webpage")