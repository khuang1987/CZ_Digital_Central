"""检查 SAP Routing Excel 文件中 SetupTime 的原始值和单位"""
import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# 1303 工厂的 Routing 文件
routing_file_1303 = PROJECT_ROOT / "data_sources" / "sap" / "1303 Routing及机加工产品清单.xlsx"

print("=" * 100)
print("检查 SAP Routing Excel 文件中的 SetupTime")
print("=" * 100)

if routing_file_1303.exists():
    print(f"\n读取文件: {routing_file_1303.name}")
    
    # 读取机加工清单（SetupTime 来源）
    df_machining = pd.read_excel(routing_file_1303, sheet_name='机加工清单')
    
    print(f"\n机加工清单列名:")
    for i, col in enumerate(df_machining.columns):
        print(f"  {i}: {col}")
    
    # 查找 SetupTime 相关列
    setup_cols = [col for col in df_machining.columns if 'setup' in str(col).lower() or '调试' in str(col) or '准备' in str(col)]
    
    if setup_cols:
        print(f"\n找到 SetupTime 相关列: {setup_cols}")
        
        for col in setup_cols:
            print(f"\n【{col}】列统计:")
            print(f"  非空数量: {df_machining[col].notna().sum()}")
            print(f"  平均值: {df_machining[col].mean():.2f}")
            print(f"  最小值: {df_machining[col].min():.2f}")
            print(f"  最大值: {df_machining[col].max():.2f}")
            
            # 显示前10个非空值
            print(f"\n  前10个非空值:")
            non_null = df_machining[df_machining[col].notna()][['CFN', 'Group', 'Operation', col]].head(10)
            print(non_null.to_string(index=False))
            
            # 显示最大值的记录
            print(f"\n  最大值记录:")
            max_idx = df_machining[col].idxmax()
            max_row = df_machining.loc[max_idx, ['CFN', 'Group', 'Operation', col]]
            print(f"  CFN: {max_row['CFN']}")
            print(f"  Group: {max_row['Group']}")
            print(f"  Operation: {max_row['Operation']}")
            print(f"  {col}: {max_row[col]}")
    else:
        print("\n未找到 SetupTime 相关列，显示所有列名供参考:")
        print(df_machining.columns.tolist())
        
        # 显示前几行数据
        print("\n前5行数据:")
        print(df_machining.head())
    
    # 检查 Routing 表中的工时数据
    print("\n" + "=" * 100)
    print("检查 Routing 表中的工时数据")
    print("=" * 100)
    
    df_routing = pd.read_excel(routing_file_1303, sheet_name='Routing')
    
    # 查找工时相关列
    time_cols = [col for col in df_routing.columns if '工时' in str(col) or 'time' in str(col).lower() or 'EH' in str(col)]
    
    print(f"\n找到工时相关列: {time_cols}")
    
    for col in time_cols[:5]:  # 只显示前5个
        if col in df_routing.columns:
            print(f"\n【{col}】列统计:")
            print(f"  非空数量: {df_routing[col].notna().sum()}")
            print(f"  平均值: {df_routing[col].mean():.2f}")
            print(f"  最小值: {df_routing[col].min():.2f}")
            print(f"  最大值: {df_routing[col].max():.2f}")
            
            # 显示几个样本值
            print(f"\n  样本值（前5个非空）:")
            samples = df_routing[df_routing[col].notna()][['CFN', 'Operation', col]].head(5)
            print(samples.to_string(index=False))

else:
    print(f"\n文件不存在: {routing_file_1303}")

print("\n" + "=" * 100)
print("建议")
print("=" * 100)
print("\n根据上述数据，判断:")
print("1. SetupTime 的单位是小时还是分钟")
print("2. 如果是分钟，需要在 ETL 中除以 60")
print("3. 如果是秒，需要在 ETL 中除以 3600")
print("\n" + "=" * 100)
