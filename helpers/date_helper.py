import calendar
from datetime import date, timedelta, datetime

dict_download_month = {
    "Jan": "Jan",
    "Feb": "Feb",
    "Mar": "Mar",
    "Apr": "Apr",
    "May": "Mei",
    "Jun": "Jun",
    "Jul": "Jul",
    "Aug": "Agu",
    "Sep": "Sep",
    "Oct": "Okt",
    "Nov": "Nov",
    "Dec": "Des"
}


# --- Define function to get today ---
def get_today():
    """
    Function for get today date

    Params:
        None

    Return:
        Today date format in date/timestamp data type

    Sample:
        get_today()
    """
    dateetl = date.today()
    dateformat = dateetl.isoformat()

    return dateformat


# --- Define function to select date in menu ---
def date_selector(days_interval):
    text_identifier, start_date, end_date, interval_download_data = None, None, None, None

    # Define condition for each interval to be text_identifier
    if days_interval == '1':
        text_identifier = 'Hari ini'
        start_date = datetime.strftime(datetime.now(), '%Y%m%d')
        end_date = datetime.strftime(datetime.now(), '%Y%m%d')

        start_data = " ".join(
            [dict_download_month.get(i, i) for i in datetime.strftime(datetime.now(), "%d %b %Y").split(" ")])
        end_data = " ".join(
            [dict_download_month.get(i, i) for i in datetime.strftime(datetime.now(), "%d %b %Y").split(" ")])

        interval_download_data = f"{start_data} - {end_data}"

    elif days_interval == '7':
        text_identifier = "7 hari terakhir"
        last_week = datetime.now() - timedelta(days=6)
        start_date = datetime.strftime(last_week, '%Y%m%d')
        end_date = datetime.strftime(datetime.now(), '%Y%m%d')

        start_data = " ".join(
            [dict_download_month.get(i, i) for i in datetime.strftime(last_week, "%d %b %Y").split(" ")])
        end_data = " ".join(
            [dict_download_month.get(i, i) for i in datetime.strftime(datetime.now(), "%d %b %Y").split(" ")])

        interval_download_data = f"{start_data} - {end_data}"

    elif days_interval == '30':
        text_identifier = "30 hari terakhir"
        last_month = datetime.now() - timedelta(days=29)
        start_date = datetime.strftime(last_month, '%Y%m%d')
        end_date = datetime.strftime(datetime.now(), '%Y%m%d')

        start_data = " ".join(
            [dict_download_month.get(i, i) for i in datetime.strftime(last_month, "%d %b %Y").split(" ")])
        end_data = " ".join(
            [dict_download_month.get(i, i) for i in datetime.strftime(datetime.now(), "%d %b %Y").split(" ")])

        interval_download_data = f"{start_data} - {end_data}"

    elif days_interval == 'current_week':
        text_identifier = "Per minggu"
        week_day = datetime.now().weekday()
        last_week = datetime.now() - timedelta(days=week_day)
        start_date = datetime.strftime(last_week, '%Y%m%d')
        end_date = datetime.strftime(datetime.now(), '%Y%m%d')

        start_data = " ".join(
            [dict_download_month.get(i, i) for i in datetime.strftime(last_week, "%d %b %Y").split(" ")])
        end_data = " ".join(
            [dict_download_month.get(i, i) for i in datetime.strftime(datetime.now(), "%d %b %Y").split(" ")])

        interval_download_data = f"{start_data} - {end_data}"

    elif days_interval == "current_month":
        text_identifier = "Per bulan"
        # start_date = datetime.strftime(last_week, '%Y%m%d')
        # end_date = datetime.strftime(datetime.now(), '%Y%m%d')
        current_year = datetime.now().year
        current_month = datetime.now().month
        last_day = datetime.now().replace(day=calendar.monthrange(current_year, current_month)[1])
        first_day = datetime.now().replace(day=1)
        start_date = datetime.strftime(first_day, "%Y%m%d")
        end_date = datetime.strftime(last_day, "%Y%m%d")

        start_data = " ".join(
            [dict_download_month.get(i, i) for i in datetime.strftime(first_day, "%d %b %Y").split(" ")])
        end_data = " ".join([dict_download_month.get(i, i) for i in datetime.strftime(last_day, "%d %b %Y").split(" ")])

        interval_download_data = f"{start_data} - {end_data}"

    else:
        print('Put the right days interval')

    return text_identifier, start_date, end_date, interval_download_data

def last_day_of_month(year, month):
    _, last_day = calendar.monthrange(year, month)
    return last_day

def adjust_end_date(start_date_str, end_date_str):
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    # Check if the end_date is in the next month and start_date is not
    if start_date.month != end_date.month and start_date.year != end_date.year:
        last_day_of_previous_month = last_day_of_month(start_date.year, start_date.month)
        end_date = datetime(start_date.year, start_date.month, last_day_of_previous_month)

    return end_date.strftime("%Y-%m-%d")

# --- Function to define date period ---
def get_on_month_period_date():

    current_date = date.today()
    start_period = current_date.replace(day=1)
    start_period = start_period.strftime("%Y-%m-%d")

    if current_date.day == 1:
        end_period = start_period
    else:
        end_period = current_date.strftime("%Y-%m-%d")
        
    return start_period, end_period

# --- Function to validate month of range date ---
def is_valid_date_range(start_date_str, end_date_str):
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    diff_date = (end_date - start_date)
    total_diff_date = diff_date.days

    # Check if the date range is within 30 days and start_date and end_date are in the same month
    if  total_diff_date > 30 or (start_date.month != end_date.month and start_date.year != end_date.year):
        raise ValueError(
            "Date range should not exceed 30 days." +
            "Start and end dates must be in the same month.")
    else:
        return True