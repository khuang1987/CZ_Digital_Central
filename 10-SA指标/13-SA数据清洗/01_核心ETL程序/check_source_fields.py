"""
检查源数据字段结构
"""
import pandas as pd
import glob
import os

def check_source_fields():
    """检查源数据中的字段"""
    data_path = "C:/Users/huangk14/OneDrive - Medtronic PLC/CZ Production - 文档/General/POWER BI 数据源 V2/70-SFC导出数据/班组合格率数据/*.xlsx"
    
    data_files = glob.glob(data_path)
    if not data_files:
        print("未找到数据文件")
        return
    
    # 读取最新文件
    data_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    latest_file = data_files[0]
    
    print(f"检查文件: {os.path.basename(latest_file)}")
    
    try:
        df = pd.read_excel(latest_file, engine='openpyxl')
        print(f"数据行数: {len(df)}")
        print(f"字段列表:")
        for i, col in enumerate(df.columns, 1):
            print(f"  {i:2d}. {col}")
        
        # 检查员工相关字段
        employee_fields = []
        for col in df.columns:
            if any(keyword in str(col) for keyword in ['报工', '关闭', '工人', '员工', '操作员', 'Employee', 'Operator']):
                employee_fields.append(col)
        
        if employee_fields:
            print(f"\n🔍 发现员工相关字段:")
            for field in employee_fields:
                unique_values = df[field].dropna().unique()[:5]  # 显示前5个唯一值
                print(f"  - {field}: {unique_values}")
        else:
            print("\n❌ 未发现员工相关字段")
            
        # 显示前几行数据样本
        print(f"\n📊 数据样本 (前3行):")
        print(df.head(3).to_string())
        
    except Exception as e:
        print(f"读取文件失败: {e}")

if __name__ == "__main__":
    check_source_fields()
