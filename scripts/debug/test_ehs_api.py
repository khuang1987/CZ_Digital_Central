import sys
from pathlib import Path
import requests

# Test the EHS API endpoint
api_url = "http://localhost:3000/api/production/ehs"

# Test 1: No params (current month)
print("=== Test 1: No params ===")
r1 = requests.get(api_url)
print(f"Status: {r1.status_code}")
if r1.status_code == 200:
    data = r1.json()
    print(f"Green Cross records: {len(data.get('greenCross', []))}")
    print(f"Stats: {data.get('stats')}")
    print(f"Area Hazards: {len(data.get('areaHazards', []))}")
    print(f"Filter Options - Areas: {len(data.get('filterOptions', {}).get('areas', []))}")
else:
    print(f"Error: {r1.text}")

# Test 2: With fiscal params
print("\n=== Test 2: Fiscal params (FY30, FY30 M10 FEB) ===")
params = {
    'fiscalYear': 'FY30',
    'fiscalMonth': 'FY30 M10 FEB'
}
r2 = requests.get(api_url, params=params)
print(f"Status: {r2.status_code}")
if r2.status_code == 200:
    data = r2.json()
    print(f"Green Cross records: {len(data.get('greenCross', []))}")
    print(f"Stats: {data.get('stats')}")
    print(f"Area Hazards: {len(data.get('areaHazards', []))}")
    print(f"Filter Options - Areas: {len(data.get('filterOptions', {}).get('areas', []))}")
else:
    print(f"Error: {r2.text}")
