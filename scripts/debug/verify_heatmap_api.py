import requests
import json

def verify_new_fields():
    url = "http://localhost:3000/api/production/ehs"
    params = {'fiscalYear': 'FY26', 'fiscalMonth': 'FY26 M10 FEB'}
    
    try:
        res = requests.get(url, params=params)
        data = res.json()
        
        incidents = data.get("incidentsList", [])
        heatmap = data.get("hazardHeatmap", [])
        
        print(f"--- API Verification ---")
        print(f"Status: {res.status_code}")
        print(f"Incidents List Count: {len(incidents)}")
        if incidents:
            print(f"Sample Incident: {incidents[0].get('title')} ({incidents[0].get('area')})")
            
        print(f"Heatmap Data Points: {len(heatmap)}")
        if heatmap:
            print(f"Sample Heatmap Point: {heatmap[0].get('area')} - {heatmap[0].get('month')}: {heatmap[0].get('count')}")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    verify_new_fields()
