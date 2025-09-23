import pprint

import requests
import json
import os

def get_company_list(api_key: str) -> dict:

    base_url = "https://api.fiscal.ai"
    req_ext = "v1/companies-list"
    headers = {
        "X-API-KEY": api_key
    }

    print(f'{base_url}/{req_ext}')
    response = requests.get(f'{base_url}/{req_ext}', headers=headers)
    if response.status_code != 200:
        raise Exception(response.json())
    company_data = response.json()
    return company_data

key = os.environ.get("AK")
if key:
    company_list = get_company_list(key)
    pprint.pprint(company_list)
    print(len(company_list))
