import time
import requests
import json

def verify_performance():
    base_url = "http://localhost:3000/api/production"
    
    # 1. Verify Calendar API
    print("--- Verifying Calendar API ---")
    start = time.time()
    res_cal = requests.get(f"{base_url}/calendar")
    end = time.time()
    
    if res_cal.status_code == 200:
        data = res_cal.json()
        print(f"Status: 200 OK")
        print(f"Response Time: {end-start:.2f}s")
        print(f"Data Summary: {len(data['years'])} years, {len(data['months'])} months entries")
    else:
        print(f"Status: {res_cal.status_code}")
        print(f"Error: {res_cal.text}")

    # 2. Verify EHS API (with problematic params)
    print("\n--- Verifying EHS API (FY26) ---")
    start = time.time()
    res_ehs = requests.get(f"{base_url}/ehs", params={'fiscalYear': 'FY26', 'fiscalMonth': 'FY26 M10 FEB'})
    end = time.time()
    
    if res_ehs.status_code == 200:
        print(f"Status: 200 OK")
        print(f"Response Time: {end-start:.2f}s")
    else:
        print(f"Status: {res_ehs.status_code}")
        print(f"Error: {res_ehs.text}")

if __name__ == "__main__":
    verify_performance()
