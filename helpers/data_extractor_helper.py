import os
import re


# --- Function for check downloaded data ---
def check_downloaded_data():
    """
    This function is used to check if the downloaded data already exists in the local environment

    Params:
    start_date (str) | Required: Start date based on extracted date that we choose (Format: %Y%m%d)
    end_date   (str) | Required: End date based on extracted date that we choose (Format: %Y%m%d)

    Example:
    check_downloaded_data('20221011', '202210117') -> Get last 7 days from today
    """
    # Define the variable
    current_directory = os.getcwd()
    result_path = f"{current_directory}/assets/excel/tokopedia/sales"
    list_file = os.listdir(result_path)
    downloaded_data = ''

    # Check file that contains format downloaded data title in local environment
    for file in list_file:
        if bool(re.search('.xlsx', file)):
            # Define the name based on the specific file
            downloaded_data = file

    # Set condition to define existance of the data
    if downloaded_data != '':
        return True
    else:
        return False
