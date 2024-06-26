import json
import os
import requests
import time

# --- Define configuration for credentials ---
config_path = os.environ['OTP_CONFIG']
config_json = open(config_path)
config = json.load(config_json)

# --- Define each credentials ---
url = config['url']
user_access = config['user_access']
pass_access = config['pass_access']


# --- Get JWT Token ---
def post_access_token():
    api_endpoint = "/api/token/"
    body = {"username": user_access,
            "password": pass_access}

    response = requests.post(f"{url}{api_endpoint}", json=body)
    res = response.json()
    access_token = res['access']

    return access_token


# --- Get list unclaimed OTP ---
def get_unclaimed_token():
    api_endpoint = "/api/search-otp/get-unclaimed-otp/"
    jwt_token = post_access_token()
    bearer_jwt_token = f"Bearer {jwt_token}"

    header = {"authorization": bearer_jwt_token}

    response = requests.get(f"{url}{api_endpoint}", headers=header)
    res = response.json()

    return res


# --- Claim OTP ---
def post_claim_otp(otp_id):
    api_endpoint = f"/api/otp/action-claim/{otp_id}/"
    jwt_token = post_access_token()
    bearer_jwt_token = f"Bearer {jwt_token}"

    header = {"authorization": bearer_jwt_token}

    response = requests.post(f"{url}{api_endpoint}", headers=header)
    res = response.json()

    return res


# --- Get history API ---
def get_history_otp():
    api_endpoint = "/api/search-otp/"
    jwt_token = post_access_token()
    bearer_jwt_token = f"Bearer {jwt_token}"

    header = {"authorization": bearer_jwt_token}

    response = requests.get(f"{url}{api_endpoint}", headers=header)
    res = response.json()

    return res

# --- Start the pipeline ---
def otp_code(official_store_id, marketplace):
    fix_id = []
    print("Start get OTP Token")
    time.sleep(30)

    unclaimed_otp = get_unclaimed_token()
    data_unclaim = unclaimed_otp['data']
    print(unclaimed_otp)

    for data in data_unclaim:
        store_id = data['store_id']
        marketplace_id = data['marketplace_id']
        if store_id == official_store_id and marketplace_id == marketplace:
            id_otp = data['id']
            print(id_otp)
            fix_id.append(id_otp)
            
    if fix_id != []:
        id_otp = fix_id[0]
        claim_otp = post_claim_otp(id_otp)
        fix_otp = claim_otp['data']['otp_number']
        print(fix_otp)
        return fix_otp
    
    else:
        raise ValueError("The OTP Number is not ready, please retry again after 30 minutes")