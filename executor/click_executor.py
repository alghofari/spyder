from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.by import By
from selenium.common.exceptions import WebDriverException
from helpers.time_helper import sleep_condition


def passing_pop_up(driver, wait, pop_up_identifier, tags_identifier, start_sleep, end_sleep, gcs_logger):
    """
    This function is used to wait, get, and perform in specific pop up based on tags identifier

    Params: 
    driver            (driver)      | Required  : WebDriver command that will drive the process of scrapping specific define tags
    wait              (condition)   | Required  : Combination of wait time on specific range time, to give spare time for perform the specific define tags
    pop_up_identifier (str)         | Required  : Name of specific value that identify the tags (Ex: "Lewati")
    tags_identifier   (str)         | Required  : Name of tags on specific button on the pop up (Ex: "button")
    start_sleep       (int)         | Required  : Number to start the range interval the randomize wait time
    end_sleep         (int)         | Required  : Number to end the range interval the randomize wait time

    Exampe:
    passing_url_to_move(driver, wait, "/myshop_order?status=new_order", "myshop_order", 3, 9)
    """
    # Start the process by define the variable
    sleep_condition(start_sleep, end_sleep)

    # Get all the tags in the page
    search_in_page_source = driver.page_source

    # Define the xpath tags
    element_to_find = f"//{tags_identifier}[contains(., '{pop_up_identifier}')]"
    gcs_logger.log(element_to_find)

    # Find the tags in the page
    if pop_up_identifier in search_in_page_source:
        gcs_logger.log(f"Pop up {pop_up_identifier} is appear on the body page")
        try:
            # Wait and click the tags
            wait.until(ec.visibility_of_element_located((By.XPATH, element_to_find)))
            # wait_click_pop_up_identifier
            wait.until(ec.element_to_be_clickable((By.XPATH, element_to_find)))
            # find_tags_pop_up_identier
            driver.find_element("xpath", element_to_find).click()

            sleep_condition(start_sleep, end_sleep)
            gcs_logger.log(f"Pop Up {pop_up_identifier} is already clicked, you will be moved to next step")

        except WebDriverException  as e:
            gcs_logger.error(str(e))
            gcs_logger.log(f"{pop_up_identifier} is not able to be perform in {element_to_find} XPATH tag")

    else:
        gcs_logger.log(f"{pop_up_identifier} is not located in the process")


def passing_click_element(driver, wait, button_identifier, tags_identifier, text_identifier, start_sleep, end_sleep, gcs_logger):
    """
    This function is used to click specific element based on the specific tags

    Params:
    driver            (driver)      | Required  : WebDriver command that will drive the process of scrapping specific define tags
    wait              (condition)   | Required  : Combination of wait time on specific range time, to give spare time for perform the specific define tags
    button_identifier (str)         | Required  : Name of tags on specific button to be click on the pop up (Ex: "button")
    tags_identifier   (str)         | Required  : Name of the tags on specific button to be identify specifically (Ex: "." -> for search all pages, "text()" -> for specific)
    text_identifier   (str)         | Required  : Name of the text identifier on the tags, same goes to the name of click button in front-end (Ex: "Minta Laporan")
    start_sleep       (int)         | Required  : Number to start the range interval the randomize wait time
    end_sleep         (int)         | Required  : Number to end the range interval the randomize wait time

    Example:
    passing_click_element(driver, wait, "button", ".", "Minta Laporan", start_sleep = 4, end_sleep = 9)
    """
    # Start the process by define the variable
    sleep_condition(start_sleep, end_sleep)

    # Get all the tags in the page
    search_in_page_source = driver.page_source

    # Define and get the xpath based on the tags
    button_to_find = f'//{button_identifier}[contains({tags_identifier}, "{text_identifier}")]'

    # Find the button tags in the page
    if button_identifier in search_in_page_source:
        gcs_logger.log(f"Button {text_identifier} is appear on the body page")

        try:
            # wait_click_button_identifier
            wait.until(ec.element_to_be_clickable((By.XPATH, button_to_find)))

            sleep_condition(start_sleep, end_sleep)
            # find_tags_button_identier
            driver.find_element("xpath", button_to_find).click()

            sleep_condition(start_sleep, end_sleep)
            gcs_logger.log(f"Button {text_identifier} is already clicked, you will be moved to next step")

        except WebDriverException  as e:
            gcs_logger.error(str(e))
            gcs_logger.log(f"Button for search {text_identifier} is not ready to be performed, check condition")

    else:
        gcs_logger.log("There might be pop up appear before click the button")
        gcs_logger.log(f"Button {text_identifier} is not existed, check the webpage, is it possible there is pop up")