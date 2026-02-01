import requests
import json

def diagnose_incidents():
    url = "http://localhost:3000/api/production/ehs"
    # Testing with FY26 M10 FEB
    params = {'fiscalYear': 'FY26', 'fiscalMonth': 'FY26 M10 FEB'}
    
    try:
        res = requests.get(url, params=params)
        data = res.json()
        
        print(f"--- EHS API Diagnosis ---")
        print(f"Stats: {data.get('stats')}")
        print(f"Incidents List: {len(data.get('incidentsList', []))} items")
        
        # If stats.incidents > 0 but list is empty, there is a mismatch
        if data.get('stats', {}).get('incidents', 0) > 0 and not data.get('incidentsList'):
            print("CRITICAL: Stats show incidents, but list is empty!")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    diagnose_incidents()
