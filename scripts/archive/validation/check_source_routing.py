import pandas as pd
import os

def check_routing_excel():
    file_path = r"C:/Users/huangk14/OneDrive - Medtronic PLC/General - CZ OPS生产每日产出登记/1303 Routing及机加工产品清单.xlsx"
    sheet_name = "1303 Routing"
    
    print(f"Checking file: {file_path}")
    if not os.path.exists(file_path):
        print("File not found!")
        return

    try:
        # Read the Excel file, specifically the Routing sheet
        # Loading only necessary columns to search for the Group
        df = pd.read_excel(file_path, sheet_name=sheet_name, dtype=str)
        
        # Normalize columns (strip whitespace)
        df.columns = df.columns.str.strip()
        
        target_group = "50208909"
        
        # Look for the group in likely columns. 
        # Based on previous knowledge, column might be 'Group' or 'Task List Group'
        # Let's search all columns or specific ones if we can guess.
        
        found = False
        for col in df.columns:
            if df[col].str.contains(target_group, na=False).any():
                print(f"Found Group {target_group} in column '{col}'")
                found = True
                
                # Show some sample rows
                sample = df[df[col] == target_group]
                print(sample.head().to_string())
                break
        
        if not found:
            print(f"Group {target_group} NOT found in sheet '{sheet_name}'.")
            
    except Exception as e:
        print(f"Error reading Excel: {e}")

if __name__ == "__main__":
    check_routing_excel()
