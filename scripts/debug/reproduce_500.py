import sys
from pathlib import Path
import requests
import json

api_url = "http://localhost:3000/api/production/ehs"
params = {
    'fiscalYear': 'FY26',
    'fiscalMonth': 'FY26 M10 FEB'
}

print(f"Testing URL: {api_url} with params {params}")
try:
    res = requests.get(api_url, params=params)
    print(f"Status Code: {res.status_code}")
    if res.status_code != 200:
        print("Response Text:")
        print(res.text)
    else:
        print("Success! Response JSON sample:")
        data = res.json()
        print(json.dumps(data, indent=2)[:500])
except Exception as e:
    print(f"Request failed: {e}")
