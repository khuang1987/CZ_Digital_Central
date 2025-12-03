"""
检查指定parquet文件的数据内容
"""
import pandas as pd
import os

def check_parquet_file(file_path):
    """检查parquet文件的数据内容"""
    if not os.path.exists(file_path):
        print(f"❌ 文件不存在: {file_path}")
        return
    
    try:
        # 读取parquet文件
        df = pd.read_parquet(file_path)
        
        print(f"📊 Parquet文件检查: {os.path.basename(file_path)}")
        print(f"文件路径: {file_path}")
        print(f"数据行数: {len(df)}")
        print(f"字段数量: {len(df.columns)}")
        
        if len(df) > 0:
            print(f"✅ 文件包含数据")
            print(f"\n📋 字段列表:")
            for i, col in enumerate(df.columns, 1):
                print(f"  {i:2d}. {col}")
            
            print(f"\n📊 数据样本 (前3行):")
            print(df.head(3).to_string())
            
            print(f"\n📈 数据类型:")
            print(df.dtypes.to_string())
            
            # 检查关键字段
            key_fields = ['BatchNumber', 'Employee', 'PassQuantity', 'FailQuantity']
            print(f"\n🔍 关键字段检查:")
            for field in key_fields:
                if field in df.columns:
                    unique_count = df[field].nunique()
                    null_count = df[field].isnull().sum()
                    print(f"  - {field}: {unique_count} 个唯一值, {null_count} 个空值")
                else:
                    print(f"  - {field}: ❌ 字段不存在")
        else:
            print(f"❌ 文件为空，无数据")
            
    except Exception as e:
        print(f"❌ 读取文件失败: {e}")

if __name__ == "__main__":
    file_path = "05_数据文件\\SFC_Product_Inspection_20251202_202542.parquet"
    check_parquet_file(file_path)
