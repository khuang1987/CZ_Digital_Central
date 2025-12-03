import pandas as pd
import re

def investigate_data_relationship():
    print("=== 调查班组合格率与SFC批次报工表数据关系 ===")
    
    # 1. 读取班组合格率聚合数据样本
    print("\n1. 分析班组合格率数据格式...")
    team_passrate_path = r'C:/Users/huangk14/OneDrive - Medtronic PLC/CZ Production - 文档/General/POWER BI 数据源 V2/30-MES导出数据/publish/SFC_Team_PassRate_latest.parquet'
    team_df = pd.read_parquet(team_passrate_path)
    
    print(f"班组合格率数据总量: {len(team_df)}")
    print("批次号样本分析:")
    batch_samples = team_df['批次号'].dropna().unique()[:20]
    for batch in batch_samples:
        print(f"  {batch}")
    
    print("\n工序编号样本分析:")
    operation_samples = team_df['工序编号'].dropna().unique()[:10]
    for op in operation_samples:
        print(f"  {op}")
    
    # 2. 读取SFC批次报工表数据样本
    print("\n2. 分析SFC批次报工表数据格式...")
    sfc_batch_path = r'C:/Users/huangk14/OneDrive - Medtronic PLC/CZ Production - 文档/General/POWER BI 数据源 V2/30-MES导出数据/publish/SFC_batch_report_latest.parquet'
    sfc_df = pd.read_parquet(sfc_batch_path)
    
    print(f"SFC批次报工表数据总量: {len(sfc_df)}")
    print("BatchNumber样本分析:")
    batch_samples = sfc_df['BatchNumber'].dropna().unique()[:20]
    for batch in batch_samples:
        print(f"  {batch}")
    
    print("\nOperation样本分析:")
    operation_samples = sfc_df['Operation'].dropna().unique()[:10]
    for op in operation_samples:
        print(f"  {op}")
    
    # 3. 尝试找到匹配模式
    print("\n3. 寻找批次号匹配模式...")
    
    # 提取班组合格率中的批次号模式
    team_batches = set(team_df['批次号'].dropna().astype(str).unique())
    sfc_batches = set(sfc_df['BatchNumber'].dropna().astype(str).unique())
    
    # 尝试不同的匹配策略
    strategies = {
        "直接匹配": lambda t, s: t == s,
        "包含匹配": lambda t, s: s in t,
        "反向包含匹配": lambda t, s: t in s,
        "提取核心批次号": lambda t, s: re.sub(r'^[A-Z]+', '', t) == s,
        "去除前缀后匹配": lambda t, s: re.sub(r'^IPQCK', '', t) == s
    }
    
    for strategy_name, strategy_func in strategies.items():
        matches = 0
        for team_batch in list(team_batches)[:100]:  # 只检查前100个以提高速度
            for sfc_batch in list(sfc_batches)[:100]:
                if strategy_func(team_batch, sfc_batch):
                    matches += 1
                    break
        
        print(f"  {strategy_name}: {matches}/100 批次号有匹配")
        if matches > 0:
            # 显示具体的匹配示例
            for team_batch in list(team_batches)[:20]:
                for sfc_batch in list(sfc_batches)[:20]:
                    if strategy_func(team_batch, sfc_batch):
                        print(f"    示例: {team_batch} <-> {sfc_batch}")
                        break
                if matches > 0:
                    break
    
    # 4. 检查数据源文件名模式
    print("\n4. 检查数据源文件模式...")
    print("班组合格率文件名: IGPR-*.xlsx")
    print("SFC批次报工文件名: SFC_batch_report_latest.parquet")
    print("推测: IGPR可能是Inspection Group Pass Report的缩写")
    
    # 5. 建议解决方案
    print("\n5. 建议解决方案:")
    print("  方案A: 检查是否有其他SFC批次报工文件包含IGPR格式的批次号")
    print("  方案B: 分析批次号转换规则，实现格式转换")
    print("  方案C: 暂时跳过数据增强，先验证核心聚合功能")

if __name__ == "__main__":
    investigate_data_relationship()
