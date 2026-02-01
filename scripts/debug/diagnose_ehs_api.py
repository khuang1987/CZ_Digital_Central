import sys
from pathlib import Path
import requests
import json

# Comprehensive API testing
api_url = "http://localhost:3000/api/production/ehs"

print("=" * 60)
print("EHS API COMPREHENSIVE DIAGNOSIS")
print("=" * 60)

# Test 1: Get current calendar info first
print("\n[1] Fetching calendar info...")
try:
    cal_res = requests.get("http://localhost:3000/api/production/calendar")
    cal_data = cal_res.json()
    print(f"Calendar API Status: {cal_res.status_code}")
    print(f"Current Fiscal Info: {cal_data.get('currentFiscalInfo')}")
    print(f"Available Years: {cal_data.get('years', [])[:5]}...")
    
    if cal_data.get('currentFiscalInfo'):
        current_fy = cal_data['currentFiscalInfo']['fiscal_year']
        current_fm = cal_data['currentFiscalInfo']['fiscal_month']
        print(f"\nWill test with: FY={current_fy}, FM={current_fm}")
    else:
        current_fy = cal_data.get('years', ['FY30'])[0]
        current_fm = None
        print(f"\nNo currentFiscalInfo, using FY={current_fy}")
except Exception as e:
    print(f"ERROR fetching calendar: {e}")
    current_fy = 'FY30'
    current_fm = None

# Test 2: Test with fiscal params (the actual params being used)
print("\n" + "=" * 60)
print("[2] Testing EHS API with ACTUAL fiscal params...")
print("=" * 60)

if current_fm:
    params = {
        'fiscalYear': current_fy,
        'fiscalMonth': current_fm
    }
else:
    params = {}

try:
    res = requests.get(api_url, params=params)
    print(f"\nHTTP Status: {res.status_code}")
    print(f"Request URL: {res.url}")
    
    if res.status_code == 200:
        data = res.json()
        print("\n✅ SUCCESS - API returned 200")
        print(f"\nResponse Structure:")
        print(f"  - greenCross: {type(data.get('greenCross'))} with {len(data.get('greenCross', []))} records")
        print(f"  - stats: {data.get('stats')}")
        print(f"  - areaHazards: {type(data.get('areaHazards'))} with {len(data.get('areaHazards', []))} records")
        print(f"  - filterOptions: {data.get('filterOptions')}")
        
        if data.get('filterOptions'):
            areas = data['filterOptions'].get('areas', [])
            print(f"\n  Filter Options - Areas ({len(areas)} total):")
            for i, area in enumerate(areas[:10]):
                print(f"    {i+1}. {area}")
            if len(areas) > 10:
                print(f"    ... and {len(areas) - 10} more")
        
        print(f"\n✅ ALL FIELDS PRESENT AND VALID")
        
    elif res.status_code == 500:
        print("\n❌ SERVER ERROR (500)")
        try:
            error_data = res.json()
            print(f"Error Message: {error_data.get('error')}")
        except:
            print(f"Error Text: {res.text[:500]}")
    else:
        print(f"\n❌ Unexpected status: {res.status_code}")
        print(f"Response: {res.text[:500]}")
        
except requests.exceptions.ConnectionError:
    print("\n❌ CONNECTION ERROR - Server might not be running on localhost:3000")
except Exception as e:
    print(f"\n❌ EXCEPTION: {type(e).__name__}: {e}")

print("\n" + "=" * 60)
print("DIAGNOSIS COMPLETE")
print("=" * 60)
