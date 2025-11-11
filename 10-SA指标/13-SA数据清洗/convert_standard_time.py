"""
标准时间表转换脚本
将CSV格式的标准时间表转换为Parquet格式
"""

import os
import sys
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# 输出目录：SharePoint同步文件夹
PUBLISH_DIR = r"C:\Users\huangk14\OneDrive - Medtronic PLC\CZ Production - 文档\General\POWER BI 数据源 V2\30-MES导出数据\publish"
os.makedirs(PUBLISH_DIR, exist_ok=True)


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """清洗DataFrame：处理N/A、空值等"""
    # 替换N/A和#N/A为None
    df = df.replace(["N/A", "#N/A", "N/A ", "N/A\t", "n/a", "#n/a"], None)
    df = df.replace("", None)
    
    # 去除字符串列的前后空格
    for col in df.columns:
        if df[col].dtype == 'object':
            # 先转换为字符串，处理空值
            df[col] = df[col].astype(str)
            # 替换特殊值
            df[col] = df[col].replace(['nan', 'None', 'NaN', 'NULL', 'null'], None)
            # 去除前后空格
            df[col] = df[col].str.strip()
            # 空字符串转为None
            df.loc[df[col] == '', col] = None
    
    return df


def convert_and_merge_standard_time(routing_csv: str, machining_csv: str, output_path: str) -> None:
    """
    转换并合并两个标准时间表
    1. 读取Routing表和机加工清单表
    2. 处理Routing表的单位转换和单位时间计算
    3. 合并机加工清单表的OEE、调试时间、备注字段
    4. 保存为合并后的Parquet文件
    """
    logging.info("=" * 80)
    logging.info("开始转换和合并标准时间表")
    logging.info("=" * 80)
    
    try:
        # 1. 读取Routing表
        logging.info(f"读取Routing CSV文件: {routing_csv}")
        routing_df = pd.read_csv(routing_csv, encoding='utf-8')
        logging.info(f"Routing表读取成功，共 {len(routing_df)} 行数据")
        
        # 清洗数据
        routing_df = clean_dataframe(routing_df)
        
        # 2. 标准化字段名称
        # Operation/activity -> Operation
        routing_df = routing_df.rename(columns={"Operation/activity": "Operation"})
        
        # Base Quantity -> Quantity
        routing_df = routing_df.rename(columns={"Base Quantity": "Quantity"})
        
        # 3. 处理Operation字段：转换为字符串并补零为4位
        def clean_operation(op):
            if pd.isna(op) or op is None:
                return None
            try:
                op_num = float(op)
                return f"{int(op_num):04d}"
            except (ValueError, TypeError):
                return str(op).strip().zfill(4) if str(op).strip() else None
        
        routing_df["Operation"] = routing_df["Operation"].apply(clean_operation)
        
        # 4. 处理Quantity字段：转换为数值，空值或无效值设为1
        routing_df["Quantity"] = pd.to_numeric(routing_df["Quantity"], errors='coerce')
        routing_df["Quantity"] = routing_df["Quantity"].fillna(1).replace(0, 1)
        
        # 5. 处理Machine和Labor：转换为数值，保持原值（不转换为小时）
        routing_df["Machine"] = pd.to_numeric(routing_df["Machine"], errors='coerce')
        routing_df["Labor"] = pd.to_numeric(routing_df["Labor"], errors='coerce')
        
        logging.info(f"Routing表处理完成，共 {len(routing_df)} 行数据")
        
        # 7. 读取机加工清单表
        logging.info(f"读取机加工清单CSV文件: {machining_csv}")
        machining_df = pd.read_csv(machining_csv, encoding='utf-8')
        logging.info(f"机加工清单表读取成功，共 {len(machining_df)} 行数据")
        
        # 清洗数据
        machining_df = clean_dataframe(machining_df)
        
        # 8. 标准化字段名称
        machining_df = machining_df.rename(columns={"Operation/activity": "Operation"})
        
        # 9. 处理Operation字段：转换为字符串并补零为4位
        def clean_operation(op):
            if pd.isna(op) or op is None:
                return None
            try:
                op_num = float(op)
                return f"{int(op_num):04d}"
            except (ValueError, TypeError):
                return str(op).strip().zfill(4) if str(op).strip() else None
        
        machining_df["Operation"] = machining_df["Operation"].apply(clean_operation)
        
        # 10. 处理OEE字段：转换为数值，保持空值（不设置默认值）
        machining_df["OEE"] = pd.to_numeric(machining_df["OEE"], errors='coerce')
        
        # 11. 处理调试时间字段：转换为数值（保持原值，单位：小时）
        machining_df["调试时间"] = pd.to_numeric(machining_df["调试时间"], errors='coerce')
        
        # 12. 合并两个表：按CFN、Operation、Group匹配（三个字段组合是唯一的）
        # 确保Group字段存在（如果不存在，创建空值）
        if "Group" not in routing_df.columns:
            routing_df["Group"] = ""
        if "Group" not in machining_df.columns:
            machining_df["Group"] = ""
        
        # 统一Group字段为字符串类型
        routing_df["Group"] = routing_df["Group"].astype(str).replace('nan', '').replace('None', '')
        machining_df["Group"] = machining_df["Group"].astype(str).replace('nan', '').replace('None', '')
        
        # 选择机加工清单表中需要合并的字段
        machining_value_cols = ["OEE", "调试时间"]
        if "备注" in machining_df.columns:
            machining_value_cols.append("备注")
        if "分类" in machining_df.columns:
            machining_value_cols.append("分类")
        
        # 合并（使用CFN、Operation、Group作为匹配键）
        merged_df = routing_df.merge(
            machining_df[["CFN", "Operation", "Group"] + machining_value_cols],
            on=["CFN", "Operation", "Group"],
            how="left"
        )
        
        # 重命名调试时间为Setup Time (h)
        merged_df = merged_df.rename(columns={"调试时间": "Setup Time (h)"})
        
        logging.info(f"合并完成，合并后共 {len(merged_df)} 行数据")
        logging.info(f"匹配到OEE: {merged_df['OEE'].notna().sum()} 行")
        logging.info(f"匹配到调试时间: {merged_df['Setup Time (h)'].notna().sum()} 行")
        
        # 8. 保存为Parquet
        merged_df.to_parquet(output_path, index=False, engine='pyarrow', compression='snappy')
        logging.info(f"已保存合并后的标准时间表Parquet文件: {output_path}, 行数: {len(merged_df)}")
        
    except Exception as e:
        logging.error(f"转换和合并标准时间表失败: {e}")
        raise


if __name__ == "__main__":
    # 确定CSV文件路径（相对于脚本位置）
    base_csv_dir = os.path.join(BASE_DIR, "..", "11数据模板")
    base_csv_dir = os.path.abspath(base_csv_dir)
    
    routing_csv = os.path.join(base_csv_dir, "1303 Routing及机加工产品清单_1303 Routing.csv")
    machining_csv = os.path.join(base_csv_dir, "1303 Routing及机加工产品清单_1303机加工清单.csv")
    
    # 输出文件路径（合并后的标准时间表，文件名包含当前日期）
    # 时间戳使用文件的生成日期（当前日期）
    timestamp = datetime.now().strftime("%Y%m%d")
    merged_parquet = os.path.join(PUBLISH_DIR, f"SAP_Routing_{timestamp}.parquet")
    
    logging.info(f"输出文件名: SAP_Routing_{timestamp}.parquet")
    
    try:
        # 检查文件是否存在
        if not os.path.exists(routing_csv):
            logging.error(f"Routing CSV文件不存在: {routing_csv}")
            sys.exit(1)
        
        if not os.path.exists(machining_csv):
            logging.error(f"机加工清单CSV文件不存在: {machining_csv}")
            sys.exit(1)
        
        # 转换并合并两个表
        convert_and_merge_standard_time(routing_csv, machining_csv, merged_parquet)
        
        logging.info("=" * 80)
        logging.info("转换和合并完成！")
        logging.info("=" * 80)
        
    except Exception as e:
        logging.exception(f"转换失败: {e}")
        sys.exit(1)

