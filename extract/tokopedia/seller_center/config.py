from helpers.creds_helper import get_config, get_creds

def generate_tkpd_config(os_key: str):
    # get config from spreadsheet
    config              = get_config(os_key, "tokopedia")
    seller_center_email = config['mail_creds']
    official_store_name = config['os_name']
    seller_center_user  = config['username']
    tkpd_shop_id        = config['shop_id']
    time_sleep_interval = config['time_sleep_interval']
    date_interval_sales = config['date_interval_sales']
    date_interval_ads   = config['date_interval_ads']

    # get creds from vaultwarden and ned
    creds              = get_creds(seller_center_email, "tokopedia")
    vaultwarden_email  = creds[0]
    seller_center_pass = creds[1]
    official_store_id  = creds[2]

    tkpd_config = [
        vaultwarden_email, seller_center_pass, official_store_id, 
        official_store_name, seller_center_user, tkpd_shop_id,
        time_sleep_interval, date_interval_sales, date_interval_ads]

    return tkpd_config