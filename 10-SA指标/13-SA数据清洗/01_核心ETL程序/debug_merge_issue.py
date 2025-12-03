import pandas as pd
import numpy as np

def debug_merge_issue():
    print("=== 调试数据增强合并问题 ===")
    
    # 1. 读取班组合格率聚合数据（模拟）
    print("\n1. 模拟班组合格率聚合数据...")
    team_data = pd.DataFrame({
        '批次号': ['K25K4211', 'K25E3327', 'K25F3060'],
        '工序编号': ['0030', '0020', '0020'],
        '工序名称': ['清洗', '纵切', '纵切'],
        '班组': ['A组', 'B组', 'C组'],
        '合格数': [100, 95, 98],
        '不合格数': [2, 5, 3]
    })
    print("班组合格率数据:")
    print(team_data)
    print(f"班组合格率数据索引: {team_data.index}")
    
    # 2. 读取SFC批次报工表数据
    print("\n2. 读取SFC批次报工表数据...")
    sfc_batch_path = r'C:/Users/huangk14/OneDrive - Medtronic PLC/CZ Production - 文档/General/POWER BI 数据源 V2/30-MES导出数据/publish/SFC_batch_report_latest.parquet'
    sfc_batch_df = pd.read_parquet(sfc_batch_path)
    
    # 提取需要的字段
    time_data = sfc_batch_df[['BatchNumber', 'Operation', 'Operation description', 'TrackOutTime']].copy()
    print("SFC批次报工表样本:")
    print(time_data.head(3))
    print(f"SFC批次报工表索引: {time_data.index}")
    
    # 3. 去重处理
    print("\n3. 去重处理...")
    time_data_sorted = time_data.sort_values('TrackOutTime', ascending=False)
    time_data_unique = time_data_sorted.drop_duplicates(
        subset=['BatchNumber', 'Operation'], 
        keep='first'
    )
    print(f"去重后记录数: {len(time_data_unique)}")
    
    # 4. 测试合并
    print("\n4. 测试合并操作...")
    try:
        # 方法1: 直接合并
        print("尝试方法1: 直接合并...")
        merged1 = team_data.merge(
            time_data_unique[['BatchNumber', 'Operation', 'TrackOutTime']],
            left_on=['批次号', '工序编号'],
            right_on=['BatchNumber', 'Operation'],
            how='left'
        )
        print("方法1成功!")
        print(merged1)
        
    except Exception as e:
        print(f"方法1失败: {e}")
    
    try:
        # 方法2: 重置索引后合并
        print("\n尝试方法2: 重置索引后合并...")
        team_data_reset = team_data.reset_index(drop=True)
        time_data_reset = time_data_unique.reset_index(drop=True)
        
        merged2 = team_data_reset.merge(
            time_data_reset[['BatchNumber', 'Operation', 'TrackOutTime']],
            left_on=['批次号', '工序编号'],
            right_on=['BatchNumber', 'Operation'],
            how='left'
        )
        print("方法2成功!")
        print(merged2)
        
    except Exception as e:
        print(f"方法2失败: {e}")
    
    # 5. 检查数据类型
    print("\n5. 检查数据类型...")
    print("班组合格率数据类型:")
    print(team_data.dtypes)
    print("\nSFC批次报工表数据类型:")
    print(time_data_unique[['BatchNumber', 'Operation', 'TrackOutTime']].dtypes)

if __name__ == "__main__":
    debug_merge_issue()
