from selenium import webdriver
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from selenium.webdriver import ActionChains, FirefoxProfile
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from utils.logger import Logger


class WebScraper:
    def __init__(self):
        self.logger = Logger().get_logger()
        self.driver = None

    def start_browser(self, browser_path, download_path, browser_profile_path=None):
        try:
            browser_profile = FirefoxProfile(browser_profile_path) if browser_profile_path else FirefoxProfile()

            options = Options()
            options.set_preference("browser.download.folderList", 2)
            options.set_preference("browser.download.manager.showWhenStarting", False)
            options.set_preference("browser.download.dir", download_path)
            options.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/x-gzip")
            options.add_argument("--width=1920")
            options.add_argument("--height=1080")
            options.profile = browser_profile

            self.driver = webdriver.Firefox(
                executable_path=browser_path,
                options=options
            )
            self.logger.info("Browser started successfully.")
        except WebDriverException as e:
            self.logger.error(f"Error starting the browser: {e}")
            raise

    def stop_browser(self):
        try:
            self.driver.quit()
            self.logger.info("Browser stopped successfully.")
        except WebDriverException as e:
            self.logger.error(f"Error stopping the browser: {e}")
            raise

    def navigate_to_url(self, url):
        try:
            self.driver.get(url)
            self.logger.info(f"Navigated to {url}.")
        except WebDriverException as e:
            self.logger.error(f"Error navigating to {url}: {e}")
            raise

    def find_element_by_keyword(self, keywords):
        try:
            # Create an XPath expression with OR conditions for each keyword
            keyword_conditions = " or ".join([f"contains(text(), '{keyword}')" for keyword in keywords])
            xpath_expression = f"//*[{keyword_conditions}]"

            # Wait until the page loads and locate elements containing any of the keywords
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, xpath_expression))
            )
            self.logger.info(f"Page loaded, looking for elements containing any of the keywords: {', '.join(keywords)}")

            # Find elements that contain any of the keywords in their text
            elements = self.driver.find_elements(By.XPATH, xpath_expression)

            if not elements:
                self.logger.info("Trying to find elements by CSS selector.")
                css_selector = ",".join(
                    [f"[href*='{keyword}'], [class*='{keyword}'], [id*='{keyword}']" for keyword in keywords])
                elements = self.driver.find_elements(By.CSS_SELECTOR, css_selector)

            if elements:
                self.logger.info(f"Found the element containing one of the keywords: {', '.join(keywords)}.")
                return elements[0]
            else:
                self.logger.warning(f"No elements found with any of the keywords: {', '.join(keywords)}.")

        except TimeoutException:
            self.logger.error(
                f"Timed out waiting for page to load or for elements containing any of the keywords: {', '.join(keywords)} to appear.")
        except NoSuchElementException:
            self.logger.error(f"No element found with any of the keywords: {', '.join(keywords)}.")
        except WebDriverException as e:
            self.logger.error(f"WebDriverException occurred: {e}")
        except Exception as e:
            self.logger.error(f"An unexpected error occurred: {e}")

    def find_element_by_locator(self, by, value):
        try:
            # Wait until the element located by the given locator is present
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((by, value))
            )
            self.logger.info(f"Page loaded, looking for element by {by} with value '{value}'.")

            # Find the element by the specified locator
            element = self.driver.find_element(by, value)

            if element:
                self.logger.info(f"Found the element by {by} with value '{value}'.")
                return element
            else:
                self.logger.warning(f"No element found by {by} with value '{value}'.")

        except TimeoutException:
            self.logger.error(f"Timed out waiting for page to load or for element by {by} with value '{value}' to appear.")
        except NoSuchElementException:
            self.logger.error(f"No element found by {by} with value '{value}'.")
        except WebDriverException as e:
            self.logger.error(f"WebDriverException occurred: {e}")
        except Exception as e:
            self.logger.error(f"An unexpected error occurred: {e}")

    def get_action_chains(self):
        try:
            action_chains = ActionChains(self.driver)
            self.logger.info("ActionChains instance created successfully.")
            return action_chains
        except WebDriverException as e:
            self.logger.error(f"Error creating ActionChains instance: {e}")
            raise
