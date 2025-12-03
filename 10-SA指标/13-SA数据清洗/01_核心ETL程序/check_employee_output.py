"""
检查员工级输出文件字段名称
"""
import pandas as pd
import os

def check_employee_output():
    """检查员工级输出文件的字段"""
    output_path = "C:/Users/huangk14/OneDrive - Medtronic PLC/CZ Production - 文档/General/POWER BI 数据源 V2/30-MES导出数据/publish/excel/SFC_Product_Inspection_Employee_latest.xlsx"
    
    if not os.path.exists(output_path):
        print(f"文件不存在: {output_path}")
        return
    
    try:
        df = pd.read_excel(output_path, engine='openpyxl')
        print(f"员工级输出文件检查: {os.path.basename(output_path)}")
        print(f"数据行数: {len(df)}")
        print(f"字段列表:")
        for i, col in enumerate(df.columns, 1):
            print(f"  {i:2d}. {col}")
        
        # 显示前几行数据样本
        print(f"\n📊 数据样本 (前3行):")
        print(df.head(3).to_string())
        
        # 检查是否有中文字段
        chinese_fields = []
        for col in df.columns:
            if any('\u4e00' <= char <= '\u9fff' for char in str(col)):
                chinese_fields.append(col)
        
        if chinese_fields:
            print(f"\n⚠️ 发现中文字段: {chinese_fields}")
        else:
            print(f"\n✅ 所有字段名称均为英文")
            
    except Exception as e:
        print(f"读取文件失败: {e}")

if __name__ == "__main__":
    check_employee_output()
