from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec

from helpers.time_helper import sleep_condition
from helpers.otp_helper import otp_code

from executor.click_executor import passing_click_element as pce


# --- Function for login process ----
def login(driver, url, mail_creds, pass_creds, official_store_id, marketplace, gcs_logger):
    gcs_logger.log(f"Start Login into {url}")
    # go to login page
    driver.get(url=url)
    wait = WebDriverWait(driver, 30)

    # Click izinkan semua cookies
    wait.until(ec.visibility_of_element_located(
        (By.XPATH, '//*[@id="onetrust-accept-btn-handler"]')))  # Wait username section appear
    wait.until(ec.element_to_be_clickable((By.XPATH, '//*[@id="onetrust-accept-btn-handler"]')))
    driver.find_element("xpath", '//*[@id="onetrust-accept-btn-handler"]').click()
    sleep_condition(7, 9)

    # Fill username section
    wait.until(ec.visibility_of_element_located((By.XPATH, '//*[@id="email-phone"]')))  # Wait username section appear
    wait.until(ec.element_to_be_clickable((By.XPATH, '//*[@id="email-phone"]')))  # Wait username section clickable
    driver.find_element("xpath", '//*[@id="email-phone"]').click()  # Click the username section
    sleep_condition(7, 9)
    driver.find_element("xpath", '//*[@id="email-phone"]').send_keys(mail_creds)  # Send the username
    sleep_condition(7, 9)
    gcs_logger.log(f"Username for {mail_creds} filled")

    # Apply the next section
    wait.until(ec.element_to_be_clickable((By.XPATH, '//*[@id="email-phone-submit"]')))
    driver.find_element("xpath", '//*[@id="email-phone-submit"]').click()
    sleep_condition(7, 9)

    # Fill password section
    wait.until(
        ec.visibility_of_element_located((By.XPATH, '//*[@id="password-input"]')))  # Wait password section appear
    wait.until(ec.element_to_be_clickable((By.XPATH, '//*[@id="password-input"]')))  # Wait password section clickable
    driver.find_element("xpath", '//*[@id="password-input"]').click()  # Click the password section
    sleep_condition(7, 9)
    driver.find_element("xpath", '//*[@id="password-input"]').send_keys(pass_creds)  # Send the password
    sleep_condition(7, 9)
    gcs_logger.log(f"Password for {mail_creds} filled")

    # Click login section 
    gcs_logger.log(f"Password finish to be collected for {mail_creds}, try to click masuk to start generating OTP Code")
    try:
        gcs_logger.log("Try to click masuk trough full xpath")
        wait.until(
            ec.element_to_be_clickable(
                (By.XPATH, '//*[@id="merchant-root"]/div[2]/section/div[2]/form/button'))).click()
        gcs_logger.log("Complete click masuk trough full xpath")
    except Exception as e:
        gcs_logger.log(f"{str(e)}")
        raise Exception("There might be error while try to click login-button Masuk")

    sleep_condition(13, 23)

    # Generate condition login (OTP Process)
    if "Email atau kata sandi salah" not in driver.page_source:
        gcs_logger.log("Password is correct, going to generate OTP Code")

        if "SMS ke" in driver.page_source:
            try:
                gcs_logger.log("Start to click SMS Ke using button pce helper")
                pce(driver, wait, 'b', 'text()', 'SMS ke', 5, 6, gcs_logger)  # Wait to send message
                gcs_logger.log(f"Button SMS Ke is already clicked, you will be moved to next step")
            except Exception as e:
                print(str(e))

        elif "E-mail ke" in driver.page_source:
            try:
                gcs_logger.log("Start to click E-mail Ke using button pce helper")
                pce(driver, wait, 'b', 'text()', 'E-mail ke', 5, 6, gcs_logger)  # Wait to send message
                gcs_logger.log(f"Button E-mail Ke is already clicked, you will be moved to next step")
            except Exception as e:
                print(str(e))

        elif "Berkendala dengan metode di atas?" in driver.page_source:
            gcs_logger.log(f"Page Change into -> Berkendala dengan metode di atas?")
            pce(driver, wait, 'button', 'text()', 'Gunakan Metode SMS', 5, 6, gcs_logger)  # Wait to send message
            sleep_condition(5, 7)

        else:
            gcs_logger.log(f"Page Change into -> Choose SMS or Whastsapp")
            try:
                gcs_logger.log("Start to click SMS Ke to send the OTP code using xpath")
                wait.until(
                    ec.element_to_be_clickable(
                        (By.XPATH, '//*[@id="merchant-root"]/div[2]/section/div/div/div[2]/div[2]/div/b')))
                driver.find_element("xpath",
                                    '//*[@id="merchant-root"]/div[2]/section/div/div/div[2]/div[2]/div/b').click()
                gcs_logger.log(f"Button SMS Ke is already clicked, you will be moved to next step")
            except Exception as e:
                print(str(e))

        if "Kode verifikasi telah dikirim melalui SMS" in driver.page_source or "Masukkan Kode Verifikasi" in driver.page_source:
            gcs_logger.log("Kode verifikasi (OTP) telah dikirim melalui SMS")
            get_otp = otp_code(official_store_id=official_store_id, marketplace=marketplace.capitalize())
            get_otp = str(get_otp)
            sleep_condition(5, 7)

            # wait_visibility_otp
            gcs_logger.log(f"OTP has been collected, wait and send the OTP number into page")
            try:
                wait.until(
                    ec.visibility_of_element_located(
                        (By.XPATH, '//*[@id="merchant-root"]/div[2]/section/div/div/input')))
                driver.find_element("xpath", '//*[@id="merchant-root"]/div[2]/section/div/div/input').send_keys(
                    get_otp)
            except Exception as e:
                wait.until(
                    ec.visibility_of_element_located(
                        (By.XPATH, '//*[@id="zeus-root"]/div/div[2]/section/div/div/input')))
                driver.find_element("xpath", '//*[@id="zeus-root"]/div/div[2]/section/div/div/input').send_keys(
                    get_otp)
                gcs_logger.log(e)

            gcs_logger.log(f"OTP has been sent, wait to move into main page")
            sleep_condition(30, 50)
            gcs_logger.log("Successful login into the main page")

        else:
            raise Exception("Kode verifikasi might not be sent, there some error when send the otp code")
    else:
        raise Exception("Email atau kata sandi salah, check the credentials")
