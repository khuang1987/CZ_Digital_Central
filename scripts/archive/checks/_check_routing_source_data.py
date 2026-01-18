"""检查 SAP Routing 源文件中 SetupTime 的原始值和单位"""
import pandas as pd
from pathlib import Path

# 使用 OneDrive 中的源文件
routing_file_1303 = Path(r"c:\Users\huangk14\OneDrive - Medtronic PLC\General - CZ OPS生产每日产出登记\1303 Routing及机加工产品清单.xlsx")

print("=" * 100)
print("检查 SAP Routing 源文件中的 SetupTime 原始数据")
print("=" * 100)

if routing_file_1303.exists():
    print(f"\n读取文件: {routing_file_1303.name}")
    
    # 读取机加工清单（SetupTime 来源）
    print("\n读取【机加工清单】sheet...")
    df_machining = pd.read_excel(routing_file_1303, sheet_name='机加工清单')
    
    print(f"\n列名:")
    for i, col in enumerate(df_machining.columns):
        print(f"  {i}: {col}")
    
    # 查找 SetupTime 相关列
    setup_col = None
    for col in df_machining.columns:
        if 'setup' in str(col).lower() or '调试' in str(col) or '准备' in str(col):
            setup_col = col
            break
    
    if setup_col:
        print(f"\n找到 SetupTime 列: 【{setup_col}】")
        
        print(f"\n统计信息:")
        print(f"  总记录数: {len(df_machining)}")
        print(f"  非空数量: {df_machining[setup_col].notna().sum()}")
        print(f"  平均值: {df_machining[setup_col].mean():.2f}")
        print(f"  中位数: {df_machining[setup_col].median():.2f}")
        print(f"  最小值: {df_machining[setup_col].min():.2f}")
        print(f"  最大值: {df_machining[setup_col].max():.2f}")
        
        # 显示前20个非空值
        print(f"\n前20个非空值样本:")
        print("CFN              Group     Operation  SetupTime")
        print("-" * 70)
        non_null = df_machining[df_machining[setup_col].notna()][['CFN', 'Group', 'Operation', setup_col]].head(20)
        for _, row in non_null.iterrows():
            print(f"{str(row['CFN']):15s}  {str(row['Group']):8s}  {str(row['Operation']):9s}  {row[setup_col]:10.2f}")
        
        # 显示最大值的记录
        print(f"\n最大值记录（SetupTime = {df_machining[setup_col].max():.2f}）:")
        max_idx = df_machining[setup_col].idxmax()
        max_row = df_machining.loc[max_idx]
        print(f"  CFN: {max_row['CFN']}")
        print(f"  Group: {max_row['Group']}")
        print(f"  Operation: {max_row['Operation']}")
        print(f"  SetupTime: {max_row[setup_col]}")
        
        # 分析单位
        print("\n" + "=" * 100)
        print("单位分析")
        print("=" * 100)
        
        avg_val = df_machining[setup_col].mean()
        max_val = df_machining[setup_col].max()
        
        print(f"\n假设单位是【小时】:")
        print(f"  平均调试时间: {avg_val:.2f} 小时 = {avg_val/24:.2f} 天")
        print(f"  最大调试时间: {max_val:.2f} 小时 = {max_val/24:.2f} 天")
        if max_val > 100:
            print(f"  ⚠️ 最大值 {max_val:.0f} 小时 = {max_val/24:.1f} 天，不合理！")
        
        print(f"\n假设单位是【分钟】:")
        print(f"  平均调试时间: {avg_val:.2f} 分钟 = {avg_val/60:.2f} 小时")
        print(f"  最大调试时间: {max_val:.2f} 分钟 = {max_val/60:.2f} 小时 = {max_val/60/24:.2f} 天")
        if max_val/60 < 24:
            print(f"  ✓ 最大值 {max_val/60:.1f} 小时，合理！")
        
        print(f"\n假设单位是【秒】:")
        print(f"  平均调试时间: {avg_val:.2f} 秒 = {avg_val/3600:.2f} 小时")
        print(f"  最大调试时间: {max_val:.2f} 秒 = {max_val/3600:.2f} 小时")
        
    else:
        print("\n⚠️ 未找到 SetupTime 相关列")
        print("\n所有列名:")
        for col in df_machining.columns:
            print(f"  - {col}")
    
    # 检查 OEE 列
    print("\n" + "=" * 100)
    print("检查 OEE 数据")
    print("=" * 100)
    
    oee_col = None
    for col in df_machining.columns:
        if 'oee' in str(col).lower():
            oee_col = col
            break
    
    if oee_col:
        print(f"\n找到 OEE 列: 【{oee_col}】")
        print(f"  非空数量: {df_machining[oee_col].notna().sum()}")
        print(f"  平均值: {df_machining[oee_col].mean():.4f}")
        print(f"  最小值: {df_machining[oee_col].min():.4f}")
        print(f"  最大值: {df_machining[oee_col].max():.4f}")
        
        # 检查 OEE 是百分比还是小数
        max_oee = df_machining[oee_col].max()
        if max_oee > 1:
            print(f"\n  ⚠️ OEE 最大值 = {max_oee:.2f} > 1，单位可能是百分比（需要除以100）")
        else:
            print(f"\n  ✓ OEE 最大值 = {max_oee:.2f} <= 1，单位是小数")

else:
    print(f"\n⚠️ 文件不存在: {routing_file_1303}")

print("\n" + "=" * 100)
