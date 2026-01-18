"""
SFC数据清洗ETL脚本
功能：从SharePoint读取SFC Excel数据，进行数据处理、去重，输出为Parquet格式供Power BI使用
"""

import os
import sys
import time
import logging
import glob
import hashlib
import threading
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Set
import re
import warnings
warnings.filterwarnings('ignore')
from pathlib import Path

# Windows平台支持
try:
    import msvcrt
    HAS_MSVCRT = True
except ImportError:
    HAS_MSVCRT = False

import pandas as pd
import yaml

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    print("警告：未安装pyarrow，将无法保存Parquet格式")
    pq = None


WRITE_PARQUET_OUTPUT = False

# 项目根目录
project_root = str(Path(__file__).resolve().parents[4])

# 导入共享基础设施工具函数
from shared_infrastructure.utils.etl_utils import (
    setup_logging,
    setup_logging_with_rotation,
    load_config,
    save_to_parquet,
    save_to_dual_locations,
    read_sharepoint_excel,
    IncrementalProcessor
)
from shared_infrastructure.utils.path_resolver import (
    get_config_path,
    get_log_path,
    get_state_path,
    get_path_resolver
)
from shared_infrastructure.utils.db_utils import get_default_db_manager

# 获取路径解析器
resolver = get_path_resolver()

# 配置路径
CONFIG_PATH = get_config_path("sfc_batch_report", "sfc", os.path.dirname(os.path.abspath(__file__)))
LOG_PATH = get_log_path("sfc")
STATE_PATH = get_state_path("sfc_batch_report", "sfc", os.path.dirname(os.path.abspath(__file__)))

# 基础目录（用于动态路径解析）
BASE_DIR = os.path.dirname(os.path.abspath(__file__))


ETL_NAME = 'sfc_batch_report'


def extract_resource_code(resource_str: Any) -> Optional[str]:
    """
    从Resource字段提取资源代码
    格式: "CZM 769 线切割 WEDM" -> "769"
    格式: "CZM M022 包装 Package" -> "M022"
    格式: "CZM Q002 器械终检 Instrument Final ins" -> "Q002"
    格式: "CZM O015 镀铬（外协） Chrome Coating" -> "O015"
    规则: 去除第一个空格前的"CZM"，提取第一个空格和第二个空格之间的内容
    只保留数字或M/Q/O开头带数字的内容
    """
    if pd.isna(resource_str) or resource_str is None:
        return None
    
    resource_str = str(resource_str).strip()
    
    # 如果为空，返回None
    if not resource_str:
        return None
    
    # 按空格分割
    parts = resource_str.split()
    
    # 如果分割后少于2个部分，无法提取
    if len(parts) < 2:
        return None
    
    # 提取第一个空格和第二个空格之间的内容（即parts[1]）
    code = parts[1]
    
    # 验证代码格式：只保留纯数字，或者以M/Q/O开头后跟数字的内容
    # 例如: "769", "625", "M022", "Q002", "O015"
    if re.match(r'^(\d+|M\d+|Q\d+|O\d+)$', code):
        return code
    
    # 如果格式不匹配，返回None（不保留其他格式）
    return None


def extract_group_number(group_str: Any) -> Optional[str]:
    """
    从Group字段提取数字部分
    例如: "CZM 50210978/0010 CZM 纵切车/CZM 纵切车" -> "50210978"
    规则: 提取第一个连续的数字序列（通常是最长的数字）
    """
    if pd.isna(group_str) or group_str is None:
        return None
    
    group_str = str(group_str).strip()
    
    if not group_str:
        return None
    
    # 使用正则表达式提取所有数字序列
    numbers = re.findall(r'\d+', group_str)
    
    if not numbers:
        return None
    
    # 返回最长的数字序列（通常是Group编号）
    # 如果只有一个数字，直接返回
    if len(numbers) == 1:
        return numbers[0]
    
    # 如果有多个数字，返回最长的（通常是Group编号，如50210978）
    longest_number = max(numbers, key=len)
    return longest_number


def standardize_operation_name(op_name: str) -> str:
    """工序名称标准化"""
    if pd.isna(op_name) or op_name is None:
        return ""
    
    op_str = str(op_name).strip()
    
    if op_str.startswith("数控铣"):
        return "数控铣"
    elif "锯" in op_str or "下料" in op_str:
        return "锯"
    elif "镀铬" in op_str:
        return "镀铬"
    elif op_str.startswith("纵切"):
        return "纵切"
    elif op_str.startswith("数控车"):
        return "数控车"
    elif op_str.startswith("线切割"):
        return "线切割"
    elif op_str.startswith("氩弧焊"):
        return "氩弧焊"
    elif "打标" in op_str:
        return "打标"
    elif op_str.startswith("深孔"):
        return "深孔钻"
    elif op_str.startswith("激光焊"):
        return "激光焊"
    elif op_str.startswith("钳工"):
        return "钳工"
    elif op_str.startswith("车削"):
        return "车削"
    elif op_str.startswith("装配"):
        return "装配"
    elif op_str.startswith("真空"):
        return "真空热处理"
    elif op_str.startswith("热处理") or op_str.startswith("非真空热处理"):
        return "热处理"
    else:
        return op_str


def process_sfc_data(df: pd.DataFrame, cfg: Dict[str, Any]) -> pd.DataFrame:
    """
    处理SFC基础数据
    对应Power Query: e4_批次报工记录_SFC.pq
    """
    if df.empty:
        return df
    
    result = df.copy()
    
    # 1. 字段映射
    column_mapping = cfg.get("sfc_mapping", {})
    rename_dict = {}
    if column_mapping:
        rename_dict = {k: v for k, v in column_mapping.items() if k in result.columns}
    if rename_dict:
        result = result.rename(columns=rename_dict)

    # 2. 过滤子批次：批次号以 -001/-002 结尾的记录
    if "BatchNumber" in result.columns:
        bn = result["BatchNumber"].astype("string").str.strip()
        keep_mask = ~bn.str.contains(r"-0\d+$", na=False, regex=True)
        result = result.loc[keep_mask].copy()
        result["BatchNumber"] = bn.loc[keep_mask]

    # 3. 类型转换
    
    logging.debug(f"SFC映射后列名: {list(result.columns)}")
    
    # 2. 类型转换
    type_config = cfg.get("sfc_types", {})
    for col, dtype in type_config.items():
        if col in result.columns:
            try:
                if dtype == "datetime":
                    result[col] = pd.to_datetime(result[col], errors='coerce')
                elif dtype == "date":
                    result[col] = pd.to_datetime(result[col], errors='coerce').dt.date
                elif dtype == "int":
                    result[col] = pd.to_numeric(result[col], errors='coerce').astype("Int64")
                elif dtype == "float":
                    result[col] = pd.to_numeric(result[col], errors='coerce').astype("float64")
                elif dtype == "string":
                    result[col] = result[col].astype(str).str.strip()
            except Exception as e:
                logging.warning(f"SFC类型转换失败 {col} -> {dtype}: {e}")
    
    # 3. Operation 转为可空整数
    if "Operation" in result.columns:
        op_num = pd.to_numeric(result["Operation"], errors='coerce')
        op_num = op_num.where(op_num.isna() | (op_num == op_num.round()))
        result["Operation"] = op_num.round().astype("Int64")
    
    # 4. 工序名称标准化
    if "Operation description" in result.columns:
        result["Operation description"] = result["Operation description"].apply(standardize_operation_name)
    
    # 5. 批次号转为文本
    if "BatchNumber" in result.columns:
        result["BatchNumber"] = result["BatchNumber"].astype("string").str.strip()
    
    # 5.5. 从Group字段提取数字部分（如果存在）
    if "Group" in result.columns:
        result["Group"] = result["Group"].apply(extract_group_number)
        logging.info(f"已从Group字段提取数字部分")
    
    # 6. 处理machine字段（机台号）
    # 源数据中明确有"机台号"字段，直接使用该字段
    if "machine" in result.columns:
        # 字段映射已生效，直接处理machine字段
        pass
    elif "机台号" in result.columns:
        # 如果字段映射还没生效，使用原始列名
        result = result.rename(columns={"机台号": "machine"})
        logging.info("检测到'机台号'列，已重命名为'machine'")
    else:
        # 如果找不到机台号字段，记录警告
        logging.warning(f"未找到'机台号'字段。当前列名: {list(result.columns)}")
        result["machine"] = None
        return result
    
    # 处理machine字段：转换为数字，N/A或空值替换为null
    def process_machine(val):
        if pd.isna(val) or val is None:
            return None
        val_str = str(val).strip()
        # 处理N/A或空值
        if val_str.upper() in ["N/A", "NA", ""]:
            return None
        # 尝试转换为数字（整数）
        try:
            # 如果是数字字符串，转换为整数
            return int(float(val_str))
        except (ValueError, TypeError):
            # 如果不是数字，返回None
            return None
    
    before_count = result["machine"].notna().sum()
    result["machine"] = result["machine"].apply(process_machine)
    after_count = result["machine"].notna().sum()
    logging.debug(f"已处理machine字段：验证前 {before_count} 条非空，验证后 {after_count} 条非空（N/A和空值已替换为null，数字已转换为整数）")
    
    # 7. 清理CheckInTime字段：处理N/A和空值
    if "CheckInTime" in result.columns:
        # 将N/A、空字符串等转换为None
        result["CheckInTime"] = result["CheckInTime"].apply(
            lambda x: None if (pd.isna(x) or str(x).strip().upper() in ["N/A", "NA", ""]) else x
        )
        # 将CheckInTime重命名为Checkin_SFC
        result = result.rename(columns={"CheckInTime": "Checkin_SFC"})
    elif "Check In 时间" in result.columns:
        # 如果字段映射还没生效
        result["Check In 时间"] = result["Check In 时间"].apply(
            lambda x: None if (pd.isna(x) or str(x).strip().upper() in ["N/A", "NA", ""]) else x
        )
        result = result.rename(columns={"Check In 时间": "Checkin_SFC"})
    
    # 8. 处理EnterStepTime字段：处理N/A和空值
    if "EnterStepTime" in result.columns:
        # 将N/A、空字符串等转换为None
        result["EnterStepTime"] = result["EnterStepTime"].apply(
            lambda x: None if (pd.isna(x) or str(x).strip().upper() in ["N/A", "NA", ""]) else x
        )
        # 确保是datetime类型
        if result["EnterStepTime"].notna().any():
            result["EnterStepTime"] = pd.to_datetime(result["EnterStepTime"], errors='coerce')
    
    return result


def merge_standard_time_sfc(sfc_df: pd.DataFrame, cfg: Dict[str, Any]) -> pd.DataFrame:
    """
    合并标准时间表到SFC数据（从合并后的parquet文件读取）
    按CFN、Operation、Group匹配（如果SFC没有Group字段，则按CFN、Operation匹配）
    """
    if sfc_df.empty:
        return sfc_df

    try:
        db = get_default_db_manager()
        with db.get_connection() as conn:
            std_time_df = pd.read_sql_query('SELECT * FROM "sap_routing_latest"', conn)
        logging.info(f"读取SQLite标准时间表 sap_routing_latest: {len(std_time_df)} 行")
    except Exception as e:
        logging.warning(f"读取SQLite标准时间表失败: {e}，跳过合并")
        for col in ["OEE", "Setup Time (h)", "Effective Time (h)"]:
            sfc_df[col] = None
        return sfc_df
    
    # 检查必要字段（至少需要CFN或Material Number之一）
    required_cols = ["Operation"]
    missing_cols = [col for col in required_cols if col not in std_time_df.columns]
    if missing_cols:
        logging.warning(f"标准时间表缺少必要字段: {missing_cols}，跳过合并")
        for col in ["EH_machine(s)", "EH_labor(s)", "OEE", "Setup Time (h)"]:
            sfc_df[col] = None
        return sfc_df
    
    # 检查是否有CFN或Material Number字段
    has_cfn = "CFN" in std_time_df.columns
    has_material_number = "Material Number" in std_time_df.columns
    if not has_cfn and not has_material_number:
        logging.warning("标准时间表缺少CFN和Material Number字段，无法进行匹配")
        for col in ["EH_machine(s)", "EH_labor(s)", "OEE", "Setup Time (h)"]:
            sfc_df[col] = None
        return sfc_df
    
    # 检查SFC是否有CFN字段
    if "CFN" not in sfc_df.columns:
        logging.warning("SFC数据缺少CFN字段，无法进行匹配")
        for col in ["EH_machine(s)", "EH_labor(s)", "OEE", "Setup Time (h)"]:
            sfc_df[col] = None
        return sfc_df
    
    # 准备需要获取的字段：Machine、Labor、Quantity、OEE、Setup Time (h)
    value_cols = []
    if "Machine" in std_time_df.columns:
        value_cols.append("Machine")
    if "Labor" in std_time_df.columns:
        value_cols.append("Labor")
    if "Quantity" in std_time_df.columns:
        value_cols.append("Quantity")
    if "OEE" in std_time_df.columns:
        value_cols.append("OEE")
    if "Setup Time (h)" in std_time_df.columns:
        value_cols.append("Setup Time (h)")
    
    if not value_cols:
        logging.warning("标准时间表缺少有效字段，跳过合并")
        for col in ["EH_machine(s)", "EH_labor(s)", "OEE", "Setup Time (h)"]:
            sfc_df[col] = None
        return sfc_df
    
    # 确定是否使用Group字段（SFC可能没有Group字段）
    use_group = "Group" in std_time_df.columns and "Group" in sfc_df.columns
    
    # 第一步：使用SFC的CFN匹配标准表的CFN
    if "CFN" in sfc_df.columns and "CFN" in std_time_df.columns:
        # 准备合并键：SFC的CFN对应标准表的CFN
        if use_group:
            merge_cols = ["CFN", "Operation", "Group"]
        else:
            merge_cols = ["CFN", "Operation"]
        
        # 合并前统一数据类型
        for col in merge_cols:
            if col in sfc_df.columns and col in std_time_df.columns:
                # 统一转换为字符串类型
                sfc_df[col] = sfc_df[col].astype(str)
                std_time_df[col] = std_time_df[col].astype(str)
                # 处理NaN值（转换为空字符串）
                sfc_df[col] = sfc_df[col].replace('nan', '').replace('None', '')
                std_time_df[col] = std_time_df[col].replace('nan', '').replace('None', '')
        
        # 第一步：使用SFC的CFN匹配标准表的CFN
        result = sfc_df.merge(
            std_time_df[merge_cols + value_cols],
            on=merge_cols,
            how="left"
        )
    else:
        # 如果标准时间表没有CFN字段，直接使用SFC数据，后续用Material Number匹配
        result = sfc_df.copy()
        # 初始化标准时间字段为空
        for col in value_cols:
            result[col] = None
    
    # 计算单件时间（Machine和Labor除以Quantity，单位：秒）
    # 重命名为EH_machine(s)和EH_labor(s)
    if "Machine" in result.columns and "Quantity" in result.columns:
        # 确保Quantity不为0或空
        quantity = result["Quantity"].fillna(1).replace(0, 1)
        # 计算单件Machine时间（秒）
        result["EH_machine(s)"] = result["Machine"] / quantity
        # 删除原始Machine字段
        result = result.drop(columns=["Machine"])
    else:
        result["EH_machine(s)"] = None
    
    if "Labor" in result.columns and "Quantity" in result.columns:
        # 确保Quantity不为0或空
        quantity = result["Quantity"].fillna(1).replace(0, 1)
        # 计算单件Labor时间（秒）
        result["EH_labor(s)"] = result["Labor"] / quantity
        # 删除原始Labor字段
        result = result.drop(columns=["Labor"])
    else:
        result["EH_labor(s)"] = None
    
    # 删除Quantity字段（不再需要）
    if "Quantity" in result.columns:
        result = result.drop(columns=["Quantity"])
    
    # 第二步：对未匹配的记录，使用SFC的CFN匹配标准表的Material Number
    # 检查是否有未匹配的记录（EH_machine(s)和EH_labor(s)都为空）
    unmatched_mask = result["EH_machine(s)"].isna() & result["EH_labor(s)"].isna()
    unmatched_count = unmatched_mask.sum()
    
    if unmatched_count > 0 and "CFN" in sfc_df.columns and "Material Number" in std_time_df.columns:
        logging.info(f"第一步CFN匹配后，还有 {unmatched_count} 条记录未匹配，尝试使用Material Number匹配")
        
        # 获取未匹配的记录
        unmatched_df = result[unmatched_mask].copy()
        
        # 准备Material Number匹配的合并键：SFC的CFN对应标准表的Material Number
        if use_group:
            material_merge_cols_sfc = ["CFN", "Operation", "Group"]
            material_merge_cols_std = ["Material Number", "Operation", "Group"]
        else:
            material_merge_cols_sfc = ["CFN", "Operation"]
            material_merge_cols_std = ["Material Number", "Operation"]
        
        # 统一数据类型
        for i, sfc_col in enumerate(material_merge_cols_sfc):
            std_col = material_merge_cols_std[i]
            if sfc_col in unmatched_df.columns and std_col in std_time_df.columns:
                unmatched_df[sfc_col] = unmatched_df[sfc_col].astype(str)
                std_time_df[std_col] = std_time_df[std_col].astype(str)
                unmatched_df[sfc_col] = unmatched_df[sfc_col].replace('nan', '').replace('None', '')
                std_time_df[std_col] = std_time_df[std_col].replace('nan', '').replace('None', '')
        
        # 使用Material Number匹配
        material_value_cols = []
        if "Machine" in std_time_df.columns:
            material_value_cols.append("Machine")
        if "Labor" in std_time_df.columns:
            material_value_cols.append("Labor")
        if "Quantity" in std_time_df.columns:
            material_value_cols.append("Quantity")
        if "OEE" in std_time_df.columns:
            material_value_cols.append("OEE")
        if "Setup Time (h)" in std_time_df.columns:
            material_value_cols.append("Setup Time (h)")
        
        if material_value_cols:
            # 使用left_on和right_on避免列名冲突
            material_matched = unmatched_df.merge(
                std_time_df[material_merge_cols_std + material_value_cols],
                left_on=material_merge_cols_sfc,
                right_on=material_merge_cols_std,
                how="left"
            )
            
            # 计算单件时间
            if "Machine" in material_matched.columns and "Quantity" in material_matched.columns:
                quantity = material_matched["Quantity"].fillna(1).replace(0, 1)
                material_matched["EH_machine(s)"] = material_matched["Machine"] / quantity
                material_matched = material_matched.drop(columns=["Machine"])
            else:
                material_matched["EH_machine(s)"] = None
            
            if "Labor" in material_matched.columns and "Quantity" in material_matched.columns:
                quantity = material_matched["Quantity"].fillna(1).replace(0, 1)
                material_matched["EH_labor(s)"] = material_matched["Labor"] / quantity
                material_matched = material_matched.drop(columns=["Labor"])
            else:
                material_matched["EH_labor(s)"] = None
            
            if "Quantity" in material_matched.columns:
                material_matched = material_matched.drop(columns=["Quantity"])
            
            # 更新result中未匹配的记录
            # 只更新那些在material_matched中匹配成功的记录
            material_matched_mask = material_matched["EH_machine(s)"].notna() | material_matched["EH_labor(s)"].notna()
            if material_matched_mask.any():
                # 获取匹配成功的记录索引
                matched_indices = material_matched[material_matched_mask].index
                
                # 更新result中对应索引的记录
                for col in ["EH_machine(s)", "EH_labor(s)", "OEE", "Setup Time (h)"]:
                    if col in material_matched.columns:
                        result.loc[matched_indices, col] = material_matched.loc[matched_indices, col]
                
                material_matched_count = len(matched_indices)
                logging.info(f"第二步Material Number匹配成功: {material_matched_count} 条记录")
    
    # 确保OEE有默认值
    if "OEE" in result.columns:
        result["OEE"] = result["OEE"].fillna(0.77).replace(0, 0.77)
    
    # 统计匹配结果
    total_matched = (result["EH_machine(s)"].notna() | result["EH_labor(s)"].notna()).sum()
    logging.info(f"合并标准时间数据完成: {len(result)} 行")
    logging.info(f"匹配到标准时间: {total_matched} 行")
    if "EH_machine(s)" in result.columns:
        logging.info(f"匹配到EH_machine(s): {result['EH_machine(s)'].notna().sum()} 行")
    if "EH_labor(s)" in result.columns:
        logging.info(f"匹配到EH_labor(s): {result['EH_labor(s)'].notna().sum()} 行")
    if "OEE" in result.columns:
        logging.info(f"匹配到OEE: {result['OEE'].notna().sum()} 行")
    if "Setup Time (h)" in result.columns:
        logging.info(f"匹配到Setup Time (h): {result['Setup Time (h)'].notna().sum()} 行")
    
    return result


def calculate_sfc_lt(row: pd.Series) -> Optional[float]:
    """
    计算SFC的LT(d) - 实际加工时间，不扣除周末
    
    计算逻辑：
    - 0010工序：优先使用Checkin_SFC，如果为空则使用EnterStepTime，再为空则使用TrackInTime
    - 非0010工序：使用EnterStepTime
    单位：天（不扣除周末）
    """
    operation = row.get("Operation", "")
    trackout = row.get("TrackOutTime", None)
    checkin_sfc = row.get("Checkin_SFC", None)
    enter_step = row.get("EnterStepTime", None)
    trackin = row.get("TrackInTime", None)
    
    if pd.isna(trackout):
        return None
    
    # 确定开始时间
    start_time = None
    if operation == "0010":
        # 0010工序：优先使用Checkin_SFC，如果为空则使用EnterStepTime，再为空则使用TrackInTime
        if pd.notna(checkin_sfc):
            start_time = checkin_sfc
        elif pd.notna(enter_step):
            start_time = enter_step
        elif pd.notna(trackin):
            start_time = trackin
    else:
        # 非0010工序：使用EnterStepTime
        if pd.notna(enter_step):
            start_time = enter_step
    
    if start_time is None:
        # 如果开始时间为空，无法计算LT
        return None
    
    trackout_dt = pd.to_datetime(trackout)
    start_dt = pd.to_datetime(start_time)
    
    # 计算实际时间差（天），不扣除周末
    total_seconds = (trackout_dt - start_dt).total_seconds()
    total_hours = total_seconds / 3600
    
    # 转换为天数（不扣除周末）
    return round(total_hours / 24, 2)


def calculate_sfc_pt(row: pd.Series) -> Optional[float]:
    """
    计算SFC的PT(d) - 实际加工时间，与MES保持一致
    
    升级逻辑：避免将设备停产时间计入PT
    - 正常情况：PT = TrackOutTime - PreviousBatchEndTime
    - 特殊情况：如果EnterStepTime > PreviousBatchEndTime，说明中间有停产期
    - 升级后：PT = TrackOutTime - TrackInTime（不包括停产等待时间）
    
    根据业务逻辑：
    1. 如果EnterStepTime <= PreviousBatchEndTime：正常连续生产
       PT(d) = (TrackOutTime - PreviousBatchEndTime) / 24
    2. 如果EnterStepTime > PreviousBatchEndTime：中间有停产期
       PT(d) = (TrackOutTime - TrackInTime) / 24
    3. 如果PreviousBatchEndTime为空，使用TrackInTime，回退逻辑：TrackInTime → Checkin_SFC
    单位：天（不扣除周末）
    """
    trackout = row.get("TrackOutTime", None)
    previous_batch_end = row.get("PreviousBatchEndTime", None)
    trackin = row.get("TrackInTime", None)
    checkin_sfc = row.get("Checkin_SFC", None)
    enter_step = row.get("EnterStepTime", None)
    
    if pd.isna(trackout):
        return None
    
    # 升级逻辑：检查是否有停产期
    start_time = None
    
    # 检查是否存在停产期
    has_production_gap = False
    if pd.notna(enter_step) and pd.notna(previous_batch_end):
        enter_step_dt = pd.to_datetime(enter_step)
        previous_end_dt = pd.to_datetime(previous_batch_end)
        if enter_step_dt > previous_end_dt:
            has_production_gap = True
            logging.debug(f"检测到停产期: EnterStepTime {enter_step} > PreviousBatchEndTime {previous_batch_end}")
    
    # 根据是否有停产期选择开始时间
    if has_production_gap:
        # 有停产期：使用TrackInTime作为实际加工开始时间，与MES保持一致
        if pd.notna(trackin):
            start_time = trackin
            logging.debug(f"使用TrackInTime计算PT: {trackin}")
        elif pd.notna(checkin_sfc):
            # 回退到Checkin_SFC
            start_time = checkin_sfc
            logging.debug(f"TrackInTime为空，回退到Checkin_SFC: {checkin_sfc}")
        else:
            # 如果都为空，回退到PreviousBatchEndTime
            start_time = previous_batch_end
            logging.debug(f"TrackInTime和Checkin_SFC都为空，回退到PreviousBatchEndTime: {previous_batch_end}")
    else:
        # 正常连续生产：使用PreviousBatchEndTime
        if pd.notna(previous_batch_end):
            start_time = previous_batch_end
            logging.debug(f"正常生产，使用PreviousBatchEndTime: {previous_batch_end}")
        elif pd.notna(checkin_sfc):
            # 如果PreviousBatchEndTime为空，使用Checkin_SFC
            start_time = checkin_sfc
            logging.debug(f"PreviousBatchEndTime为空，使用Checkin_SFC: {checkin_sfc}")
        else:
            # 如果两者都为空，返回None
            return None
    
    trackout_dt = pd.to_datetime(trackout)
    start_dt = pd.to_datetime(start_time)
    
    # 确保结束时间大于开始时间
    if trackout_dt <= start_dt:
        return None
    
    # 计算实际时间差（天），不扣除周末
    total_seconds = (trackout_dt - start_dt).total_seconds()
    total_hours = total_seconds / 3600
    
    # 转换为天数（不扣除周末）
    return round(total_hours / 24, 2)


def get_weekend_period(date: datetime) -> tuple:
    """
    获取指定日期所在周的周末区间（周六8:00到周一8:00）
    返回: (saturday_8am, monday_8am)
    """
    weekday = date.weekday()  # Monday=0, Sunday=6
    
    # 找到本周六8点
    if weekday == 5:  # Saturday
        if date.hour >= 8:
            # 如果已经是周六8点之后，使用本周六8点
            saturday_8am = date.replace(hour=8, minute=0, second=0, microsecond=0)
        else:
            # 周六8点之前，使用上周六8点
            saturday_8am = date.replace(hour=8, minute=0, second=0, microsecond=0) - timedelta(days=7)
    elif weekday == 6:  # Sunday
        saturday_8am = date.replace(hour=8, minute=0, second=0, microsecond=0) - timedelta(days=1)
    elif weekday == 0:  # Monday
        if date.hour < 8:
            saturday_8am = date.replace(hour=8, minute=0, second=0, microsecond=0) - timedelta(days=2)
        else:
            saturday_8am = date.replace(hour=8, minute=0, second=0, microsecond=0) + timedelta(days=5)
    else:  # Tuesday to Friday
        days_to_saturday = 5 - weekday
        saturday_8am = date.replace(hour=8, minute=0, second=0, microsecond=0) + timedelta(days=days_to_saturday)
        # 如果还没到本周六，使用上周六
        if saturday_8am > date:
            saturday_8am = saturday_8am - timedelta(days=7)
    
    monday_8am = saturday_8am + timedelta(days=2)
    return saturday_8am, monday_8am


def calculate_weekend_hours(start: datetime, end: datetime) -> float:
    """
    计算从start到end之间的周末小时数
    周末定义：周六8:00到周一8:00之间的所有时间（48小时）
    """
    if end <= start:
        return 0.0
    
    total_hours = 0.0
    
    # 找到start和end之间的所有周末区间
    current_start = start
    processed_weekends = set()  # 记录已处理的周末区间（用周六8点作为key）
    
    while current_start < end:
        # 获取当前时间所在周的周末区间
        saturday_8am, monday_8am = get_weekend_period(current_start)
        
        # 避免重复处理同一个周末区间
        weekend_key = saturday_8am
        if weekend_key in processed_weekends:
            # 已经处理过这个周末，跳到下个周一8点
            current_start = monday_8am
            continue
        
        processed_weekends.add(weekend_key)
        
        # 计算周末区间与[start, end]的重叠部分
        overlap_start = max(current_start, saturday_8am)
        overlap_end = min(end, monday_8am)
        
        if overlap_start < overlap_end:
            # 有重叠，计算重叠小时数
            hours = (overlap_end - overlap_start).total_seconds() / 3600
            total_hours += hours
        
        # 移动到下一个可能的周末区间
        if current_start < monday_8am:
            current_start = monday_8am
        else:
            # 找到下一个周末区间
            next_saturday = saturday_8am + timedelta(days=7)
            if next_saturday < end:
                current_start = next_saturday
            else:
                break
    
    return round(total_hours, 2)


def calculate_sfc_st(row: pd.Series) -> Optional[float]:
    """
    计算SFC的ST(d) - 理论加工时间，不考虑周末
    
    计算逻辑：
    ST = (调试时间 + (合格数量 + 报废数量) × EH_machine或EH_labor / OEE + 0.5小时换批时间) / 24
    单位：天
    """
    # 使用TrackOutQuantity + ScrapQuantity
    trackout_qty = row.get("TrackOutQuantity", 0) or 0
    scrap_qty = row.get("ScrapQuantity", 0) or 0
    qty = trackout_qty + scrap_qty
    
    oee = row.get("OEE", 0.77) or 0.77
    setup_time = 0  # 调试时间（小时）
    
    if row.get("Setup") == "Yes" and pd.notna(row.get("Setup Time (h)")):
        setup_time = row.get("Setup Time (h)", 0) or 0
    
    # 获取单件时间（秒），优先使用EH_machine(s)，否则使用EH_labor(s)
    machine_time_s = row.get("EH_machine(s)", None)
    labor_time_s = row.get("EH_labor(s)", None)
    
    # 确定使用哪个时间
    if pd.notna(machine_time_s) and machine_time_s > 0:
        unit_time_s = machine_time_s
    elif pd.notna(labor_time_s) and labor_time_s > 0:
        unit_time_s = labor_time_s
    else:
        return None
    
    # 转换为小时
    unit_time_h = unit_time_s / 3600
    
    # 计算基础工时（理论时间，不考虑周末）
    # 公式：调试时间 + (数量 × 单件时间 / OEE) + 0.5小时换批时间
    base_hours = setup_time + (qty * unit_time_h / oee) + 0.5
    
    if base_hours == 0:
        return None
    
    # ST = 基础工时 / 24（不考虑周末）
    return round(base_hours / 24, 2)


def calculate_sfc_due_time(row: pd.Series) -> Optional[datetime]:
    """计算SFC的DueTime（包含周末调整）"""
    # SFC数据可能没有TrackInTime，使用Checkin_SFC作为开始时间
    start_time = row.get("TrackInTime", None) or row.get("Checkin_SFC", None)
    if pd.isna(start_time):
        return None
    
    start_dt = pd.to_datetime(start_time)
    
    setup_time = 0
    if row.get("Setup") == "Yes" and pd.notna(row.get("Setup Time (h)")):
        setup_time = row.get("Setup Time (h)", 0) or 0
    
    # 获取单件时间（秒），优先使用EH_machine(s)，否则使用EH_labor(s)
    machine_time_s = row.get("EH_machine(s)", None)
    labor_time_s = row.get("EH_labor(s)", None)
    
    # 确定使用哪个时间
    if pd.notna(machine_time_s) and machine_time_s > 0:
        unit_time_s = machine_time_s
    elif pd.notna(labor_time_s) and labor_time_s > 0:
        unit_time_s = labor_time_s
    else:
        return None
    
    # 转换为小时
    unit_time_h = unit_time_s / 3600
    
    # 使用TrackOutQuantity + ScrapQuantity
    trackout_qty = row.get("TrackOutQuantity", 0) or 0
    scrap_qty = row.get("ScrapQuantity", 0) or 0
    qty = trackout_qty + scrap_qty
    oee = row.get("OEE", 0.77) or 0.77
    
    total_hours = setup_time + unit_time_h * qty / oee
    due0 = start_dt + timedelta(hours=total_hours + 0.5)
    
    # 调整周末（考虑周末小时数）
    due_final = adjust_weekend_sfc(start_dt, due0)
    
    return due_final


def adjust_weekend_sfc(start: datetime, due: datetime) -> datetime:
    """
    调整周末，顺延到工作日（SFC版本）
    周末定义：周六8:00到周一8:00之间的所有时间
    如果截止时间落在周末，顺延到周一8:00
    """
    if due <= start:
        return due
    
    # 计算从start到due之间的周末小时数
    weekend_hours = calculate_weekend_hours(start, due)
    
    # 检查due是否落在周末区间内
    weekday = due.weekday()  # Monday=0, Sunday=6
    due_hour = due.hour
    
    # 判断是否在周末区间：周六8点之后，或周日，或周一8点之前
    is_in_weekend = False
    if weekday == 5 and due_hour >= 8:  # 周六8点及以后
        is_in_weekend = True
    elif weekday == 6:  # 周日全天
        is_in_weekend = True
    elif weekday == 0 and due_hour < 8:  # 周一8点之前
        is_in_weekend = True
    
    # 如果落在周末区间，顺延到周一8点
    if is_in_weekend:
        # 找到下一个周一8点
        if weekday == 5:  # 周六
            days_to_monday = 2
        elif weekday == 6:  # 周日
            days_to_monday = 1
        else:  # 周一（但还没到8点）
            days_to_monday = 7  # 下周一
        
        next_monday = (due.replace(hour=8, minute=0, second=0, microsecond=0) + 
                      timedelta(days=days_to_monday))
        return next_monday
    
    # 如果不在周末区间，直接加上周末小时数
    return due + timedelta(hours=weekend_hours)


def calculate_sfc_weekend_days(row: pd.Series) -> Optional[float]:
    """
    计算SFC的Weekend(d) - 周末扣除天数（单位：天）
    先按小时计算周末时间（周六8:00到周一8:00），然后转换为天数
    返回值：周末天数（保留2位小数）
    """
    start_time = row.get("TrackInTime", None) or row.get("Checkin_SFC", None)
    if pd.isna(start_time):
        return None
    
    start_dt = pd.to_datetime(start_time)
    
    setup_time = 0
    if row.get("Setup") == "Yes" and pd.notna(row.get("Setup Time (h)")):
        setup_time = row.get("Setup Time (h)", 0) or 0
    
    # 获取单件时间（秒），优先使用EH_machine(s)，否则使用EH_labor(s)
    machine_time_s = row.get("EH_machine(s)", None)
    labor_time_s = row.get("EH_labor(s)", None)
    
    # 确定使用哪个时间
    if pd.notna(machine_time_s) and machine_time_s > 0:
        unit_time_s = machine_time_s
    elif pd.notna(labor_time_s) and labor_time_s > 0:
        unit_time_s = labor_time_s
    else:
        return None
    
    # 转换为小时
    unit_time_h = unit_time_s / 3600
    
    # 使用TrackOutQuantity + ScrapQuantity
    trackout_qty = row.get("TrackOutQuantity", 0) or 0
    scrap_qty = row.get("ScrapQuantity", 0) or 0
    qty = trackout_qty + scrap_qty
    oee = row.get("OEE", 0.77) or 0.77
    
    total_hours = setup_time + unit_time_h * qty / oee
    due0 = start_dt + timedelta(hours=total_hours + 0.5)
    
    # 计算周末小时数
    weekend_hours = calculate_weekend_hours(start_dt, due0)
    
    # 转换为天数（保留2位小数）
    return round(weekend_hours / 24, 2)


def calculate_sfc_completion_status(row: pd.Series) -> Optional[str]:
    """
    计算SFC的CompletionStatus（基于PT和ST比较，与MES保持一致）
    
    逻辑：直接比较已计算的PT（实际加工时间）和ST（理论加工时间）
    - PT是已经计算好的实际加工时间（已排除停产期）
    - ST是已经计算好的理论加工时间
    - PT > ST + 8小时容差 + 换批/换型时间 + 非工作日时间 → Overdue
    - PT <= ST + 8小时容差 + 换批/换型时间 + 非工作日时间 → OnTime
    
    换批/换型时间规则：
    - 正常换批：固定0.5小时
    - 换型情况：Setup="Yes"时，使用标准换型时间（Setup Time字段）
    """
    # 获取已计算的PT和ST
    pt = row.get("PT(d)", None)
    st = row.get("ST(d)", None)
    tolerance_h = row.get("Tolerance(h)", 8.0)  # 默认8小时容差
    weekend_d = row.get("Weekend(d)", 0.0)  # 周末天数
    
    if pd.isna(pt) or pd.isna(st):
        return None
    
    # PT转换为小时
    pt_hours = pt * 24
    # ST转换为小时
    st_hours = st * 24
    # 周末转换为小时
    weekend_hours = weekend_d * 24
    
    # 检查是否需要使用标准换型时间
    changeover_time = 0.5  # 默认换批时间
    if row.get("Setup") == "Yes" and pd.notna(row.get("Setup Time (h)")):
        changeover_time = row.get("Setup Time (h)", 0.5) or 0.5  # 使用标准换型时间
    
    # 阈值 = ST + 容差 + 换批/换型时间 + 周末时间
    threshold = st_hours + tolerance_h + changeover_time + weekend_hours
    
    # 比较：PT（小时） > 阈值 → Overdue
    if pt_hours > threshold:
        return "Overdue"
    else:
        return "OnTime"


def calculate_sfc_tolerance_hours(row: pd.Series) -> Optional[float]:
    """计算容差小时数（固定8小时）"""
    due = row.get("DueTime", None)
    actual = row.get("TrackOutTime", None)
    
    if pd.isna(due) or pd.isna(actual):
        return None
    
    # 固定容差为8小时
    return 8.0


def extract_machine_number_sfc(machine: Any) -> Optional[int]:
    """
    从machine字段提取数字（SFC版本）
    注意：machine字段已经处理为整数或None，此函数主要用于兼容性
    """
    if pd.isna(machine) or machine is None:
        return None
    
    # 如果已经是整数，直接返回
    if isinstance(machine, (int, float)):
        return int(machine)
    
    # 如果是字符串，尝试提取数字
    code_str = str(machine)
    numbers = re.findall(r'\d+', code_str)
    if numbers:
        try:
            return int(numbers[0])
        except:
            return None
    return None


def calculate_previous_batch_end_time(df: pd.DataFrame) -> pd.DataFrame:
    """
    计算上批结束时间（PreviousBatchEndTime）
    
    逻辑：
    - 按 machine 分组
    - 在每个组内按 TrackOutTime 升序排序（最早的在前）
    - 使用 shift(1) 获取上一批的 TrackOutTime
    - 对于第一批（PreviousBatchEndTime 为空），使用 EnterStepTime 代替
    
    注意：此函数应在所有数据合并完成后调用，确保排序准确
    """
    if df.empty:
        df["PreviousBatchEndTime"] = None
        return df
    
    # 检查必需字段
    if "TrackOutTime" not in df.columns:
        logging.warning("缺少必需字段 TrackOutTime，无法计算 PreviousBatchEndTime")
        df["PreviousBatchEndTime"] = None
        return df
    
    result = df.copy()
    
    # 确保machine字段存在（如果不存在，创建为None）
    if "machine" not in result.columns:
        logging.warning("缺少machine字段，无法按machine分组计算 PreviousBatchEndTime")
        result["PreviousBatchEndTime"] = None
        return result
    
    # 检查machine字段是否有有效值
    valid_machine_count = result["machine"].notna().sum()
    if valid_machine_count == 0:
        logging.warning("machine字段全部为空，无法计算 PreviousBatchEndTime")
        result["PreviousBatchEndTime"] = None
        return result
    
    logging.info(f"开始计算 PreviousBatchEndTime：有效machine记录 {valid_machine_count} 条，总计 {len(result)} 条")
    
    # 按 machine 分组，按 TrackOutTime 升序排序（最早的在前）
    # 对于 machine 为空或 TrackOutTime 为空的记录，先处理
    result = result.sort_values(
        ["machine", "TrackOutTime"],
        na_position='last',  # 空值排在最后
        ascending=[True, True]  # machine 和 TrackOutTime 都升序
    )
    
    # 使用 shift(1) 获取上一批的 TrackOutTime
    # 只对machine不为空的记录进行分组计算
    result["PreviousBatchEndTime"] = None
    
    # 只对machine不为空的记录进行分组计算
    mask_valid = result["machine"].notna() & result["TrackOutTime"].notna()
    if mask_valid.any():
        # 对有效记录进行分组计算
        result.loc[mask_valid, "PreviousBatchEndTime"] = (
            result[mask_valid].groupby("machine")["TrackOutTime"].shift(1)
        )
        
        # 对于第一批（PreviousBatchEndTime 为空），使用 EnterStepTime 代替
        mask_first_batch = mask_valid & result["PreviousBatchEndTime"].isna()
        if "EnterStepTime" in result.columns and mask_first_batch.any():
            result.loc[mask_first_batch, "PreviousBatchEndTime"] = result.loc[mask_first_batch, "EnterStepTime"]
    
    # 将NaT转换为None（确保datetime类型的空值正确处理）
    # 对于datetime类型的列，NaT（Not a Time）需要转换为None才能正确保存为null
    if "PreviousBatchEndTime" in result.columns:
        # 先转换为object类型，然后替换NaT为None
        if pd.api.types.is_datetime64_any_dtype(result["PreviousBatchEndTime"]):
            result["PreviousBatchEndTime"] = result["PreviousBatchEndTime"].astype(object)
            result["PreviousBatchEndTime"] = result["PreviousBatchEndTime"].apply(lambda x: None if pd.isna(x) else x)
        elif result["PreviousBatchEndTime"].dtype == 'object':
            # 如果已经是object类型，直接替换NaT为None
            result["PreviousBatchEndTime"] = result["PreviousBatchEndTime"].apply(lambda x: None if pd.isna(x) else x)
    
    # 统计计算结果
    total_count = len(result)
    calculated_count = result["PreviousBatchEndTime"].notna().sum() if "PreviousBatchEndTime" in result.columns else 0
    valid_calculated = (mask_valid & result["PreviousBatchEndTime"].notna()).sum() if mask_valid.any() and "PreviousBatchEndTime" in result.columns else 0
    logging.info(f"计算 PreviousBatchEndTime 完成：总计 {total_count} 条，成功计算 {calculated_count} 条（其中有效machine记录 {valid_calculated} 条）")
    
    return result


def calculate_sfc_metrics(df: pd.DataFrame, cfg: Dict[str, Any]) -> pd.DataFrame:
    """
    计算SFC数据的所有指标字段
    对应Power Query: e2_批次报工记录_MES_后处理.pq（适配SFC数据）
    """
    if df.empty:
        return df
    
    result = df.copy()
    
    logging.debug(f"SFC数据列名: {list(result.columns)}")
    
    # 0. 计算 PreviousBatchEndTime（在计算LT/PT之前）
    # 注意：如果数据是增量处理的，这里只对新数据计算，最终会在合并后统一重新计算
    # 这里先计算是为了后续可能使用该字段进行计算
    result = calculate_previous_batch_end_time(result)
    
    # 1. 计算LT(d)
    result["LT(d)"] = result.apply(calculate_sfc_lt, axis=1)
    
    # 2. 计算PT(d)
    result["PT(d)"] = result.apply(calculate_sfc_pt, axis=1)
    
    # 3. 处理OEE默认值
    if "OEE" in result.columns:
        result["OEE"] = result["OEE"].fillna(0.77).replace(0, 0.77)
    
    # 4. 确保EH_machine(s)和EH_labor(s)字段存在
    if "EH_machine(s)" not in result.columns:
        result["EH_machine(s)"] = None
    if "EH_labor(s)" not in result.columns:
        result["EH_labor(s)"] = None
    
    # 5. 计算ST(d)
    result["ST(d)"] = result.apply(calculate_sfc_st, axis=1)
    
    # 6. 计算DueTime和Weekend(d)
    result["DueTime"] = result.apply(calculate_sfc_due_time, axis=1)
    result["Weekend(d)"] = result.apply(calculate_sfc_weekend_days, axis=1)
    
    # 7. 计算CompletionStatus（基于PT和ST比较，不使用DueTime）
    result["CompletionStatus"] = result.apply(calculate_sfc_completion_status, axis=1)
    
    # 8. 计算容差小时数（单独字段）
    result["Tolerance(h)"] = result.apply(calculate_sfc_tolerance_hours, axis=1)
    
    # 9. 计算Machine(#)
    if "machine" in result.columns:
        result["Machine(#)"] = result["machine"].apply(extract_machine_number_sfc)
    else:
        logging.warning("machine字段不存在，无法计算Machine(#)")
        result["Machine(#)"] = None
    
    return result


def save_excel_for_validation(df: pd.DataFrame, parquet_path: str, cfg: Dict[str, Any]) -> None:
    """保存DataFrame为Excel格式用于数据完整性检查"""
    # 检查是否启用Excel输出
    excel_enabled = cfg.get("output", {}).get("excel", {}).get("enabled", True)
    if not excel_enabled:
        return
    
    if df.empty:
        logging.warning("数据为空，不保存Excel文件")
        return
    
    try:
        # 构建Excel文件路径：在publish目录下创建excel子文件夹
        publish_dir = os.path.dirname(parquet_path)
        excel_dir = os.path.join(publish_dir, "excel")
        
        # 从parquet文件名生成excel文件名
        parquet_filename = os.path.basename(parquet_path)
        excel_filename = parquet_filename.replace('.parquet', '.xlsx')
        excel_path = os.path.join(excel_dir, excel_filename)
        
        # 确保excel目录存在
        os.makedirs(excel_dir, exist_ok=True)
        
        # 获取Excel配置
        max_rows = cfg.get("output", {}).get("excel", {}).get("max_rows", 10000)
        include_stats = cfg.get("output", {}).get("excel", {}).get("include_stats", True)
        
        # 准备Excel数据
        excel_df = df.copy()
        
        # 如果数据量过大，只保存前N行
        if len(excel_df) > max_rows:
            logging.info(f"数据量较大({len(excel_df)}行)，Excel文件只保存前{max_rows}行")
            excel_df = excel_df.head(max_rows)
        
        # 创建Excel写入器
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            # 保存主数据
            excel_df.to_excel(writer, sheet_name='数据', index=False)
            
            # 如果启用统计信息，添加统计工作表
            if include_stats:
                stats_data = []
                for col in df.columns:
                    col_stats = {
                        '字段名': col,
                        '数据类型': str(df[col].dtype),
                        '总记录数': len(df),
                        '非空记录数': df[col].notna().sum(),
                        '空值记录数': df[col].isna().sum(),
                        '非空率': f"{df[col].notna().sum()/len(df)*100:.1f}%",
                        '唯一值数量': df[col].nunique()
                    }
                    
                    # 数值字段添加额外统计
                    if df[col].dtype in ['int64', 'float64', 'Int64']:
                        col_stats.update({
                            '最小值': df[col].min(),
                            '最大值': df[col].max(),
                            '平均值': round(df[col].mean(), 2),
                            '中位数': round(df[col].median(), 2)
                        })
                    
                    stats_data.append(col_stats)
                
                stats_df = pd.DataFrame(stats_data)
                stats_df.to_excel(writer, sheet_name='统计信息', index=False)
        
        logging.info(f"已保存Excel验证文件: {excel_path}, 行数: {len(excel_df)}")
        
    except ImportError:
        logging.warning("未安装openpyxl，无法保存Excel文件。请运行: pip install openpyxl")
    except Exception as e:
        logging.warning(f"保存Excel文件失败: {e}")
        # 不抛出异常，因为Excel输出是可选的


def save_to_parquet(df: pd.DataFrame, output_path: str, cfg: Dict[str, Any]) -> None:
    """保存为Parquet格式"""
    if df.empty:
        logging.warning("数据为空，不保存")
        return
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # 定义数字字段列表（需要确保空值为null）
    numeric_fields = [
        "TrackOutQuantity", "ScrapQuantity", 
        "LT(d)", "PT(d)", "ST(d)", "Weekend(d)", "Tolerance(h)",
        "OEE", "Setup Time (h)", "EH_machine(s)", "EH_labor(s)", "Machine(#)"
    ]
    
    # 处理数字字段：将NaN转换为None（保存到Parquet时会是null）
    for col in numeric_fields:
        if col in df.columns:
            # 对于整数类型，使用Int64（支持NaN）
            if col in ["TrackOutQuantity", "ScrapQuantity", "Machine(#)"]:
                # 确保是Int64类型（支持NaN）
                df[col] = df[col].astype("Int64")
            # 对于浮点数类型，NaN已经是None的表示，但确保类型正确
            elif df[col].dtype in ['float64', 'float32']:
                # 保持float64类型，NaN会自动转换为null
                pass
            else:
                # 其他数字类型，确保NaN存在
                df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # 处理datetime字段：将NaT转换为None（确保保存为null而不是NaT）
    # 对于所有datetime类型的列，将NaT转换为None
    datetime_fields = ["Checkin_SFC", "EnterStepTime", "PreviousBatchEndTime", "TrackOutTime", "DueTime"]
    for col in datetime_fields:
        if col in df.columns:
            # 检查是否是datetime类型
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                # 先转换为object类型（允许None值）
                df[col] = df[col].astype(object)
                # 将NaT和NaN都替换为None
                df[col] = df[col].apply(lambda x: None if pd.isna(x) else x)
            elif df[col].dtype == 'object':
                # 如果已经是object类型，检查是否有NaT值（可能是之前转换失败的）
                # 将任何NaT、NaN、'NaT'字符串都替换为None
                df[col] = df[col].apply(lambda x: None if (pd.isna(x) or x == 'NaT' or str(x) == 'NaT') else x)
    
    # 确保字符串列类型正确（但排除datetime字段，因为它们已经转换为object类型以支持None）
    datetime_field_set = set(datetime_fields)
    for col in df.columns:
        if col not in datetime_field_set and df[col].dtype == 'object':
            df[col] = df[col].astype(str).replace('nan', None).replace('None', None)
            # 将空字符串也转换为None
            df[col] = df[col].replace('', None)
    
    compression = cfg.get("output", {}).get("parquet", {}).get("compression", "snappy")
    
    try:
        # 保存为Parquet时，PyArrow会自动将object类型的datetime值转换为timestamp类型
        # None值会被正确保存为null
        df.to_parquet(output_path, index=False, engine='pyarrow', compression=compression)
        logging.info(f"已保存Parquet文件: {output_path}, 行数: {len(df)}")
        
        # 同时保存Excel文件用于数据完整性检查
        save_excel_for_validation(df, output_path, cfg)
        
        # 验证PreviousBatchEndTime字段的空值处理
        if "PreviousBatchEndTime" in df.columns:
            null_count = df["PreviousBatchEndTime"].isna().sum()
            none_count = (df["PreviousBatchEndTime"] == None).sum() if df["PreviousBatchEndTime"].dtype == 'object' else 0
            logging.debug(f"PreviousBatchEndTime字段：null值 {null_count} 个，None值 {none_count} 个")
    except Exception as e:
        logging.error(f"保存Parquet失败: {e}")
        raise


def generate_record_hash(row: pd.Series, key_fields: List[str]) -> str:
    """
    生成记录的唯一hash值（基于业务字段，不含文件名）
    因为相邻日期的文件中有90%的数据行是重复的，应该基于业务字段去重
    """
    values = []
    
    # 提取关键字段的值
    for field in key_fields:
        val = row.get(field)
        if pd.isna(val):
            val = None
        else:
            # 如果是datetime类型，转换为字符串
            if isinstance(val, (pd.Timestamp, datetime)):
                val = val.strftime('%Y-%m-%d %H:%M:%S')
            else:
                val = str(val)
        values.append(val if val is not None else '')
    
    # 生成hash（不含文件名）
    key_str = '|'.join(values)
    return hashlib.md5(key_str.encode('utf-8')).hexdigest()


def _init_incremental_state_tables(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS etl_incremental_files (
            etl_name TEXT NOT NULL,
            file_path TEXT NOT NULL,
            file_mtime REAL,
            file_name TEXT,
            processed_time TEXT,
            record_count INTEGER,
            PRIMARY KEY (etl_name, file_path)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS etl_incremental_hashes (
            etl_name TEXT NOT NULL,
            record_hash TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (etl_name, record_hash)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS etl_incremental_meta (
            etl_name TEXT PRIMARY KEY,
            last_processed_time TEXT,
            last_full_refresh_time TEXT
        )
        """
    )


def _get_last_processed_time(conn) -> Optional[str]:
    row = conn.execute(
        'SELECT last_processed_time FROM etl_incremental_meta WHERE etl_name = ?',
        (ETL_NAME,),
    ).fetchone()
    if row is None:
        return None
    return row[0]


def _set_last_processed_time(conn, ts: str) -> None:
    conn.execute(
        'INSERT INTO etl_incremental_meta (etl_name, last_processed_time) VALUES (?, ?) '
        'ON CONFLICT(etl_name) DO UPDATE SET last_processed_time = excluded.last_processed_time',
        (ETL_NAME, ts),
    )


def _clear_incremental_state(conn) -> None:
    conn.execute('DELETE FROM etl_incremental_files WHERE etl_name = ?', (ETL_NAME,))
    conn.execute('DELETE FROM etl_incremental_hashes WHERE etl_name = ?', (ETL_NAME,))
    conn.execute('DELETE FROM etl_incremental_meta WHERE etl_name = ?', (ETL_NAME,))


def is_file_processed(file_path: str, conn) -> bool:
    """检查文件是否已处理过（基于 file_path + mtime）"""
    try:
        current_mtime = os.path.getmtime(file_path)
    except Exception:
        return False

    row = conn.execute(
        'SELECT file_mtime FROM etl_incremental_files WHERE etl_name = ? AND file_path = ?',
        (ETL_NAME, file_path),
    ).fetchone()
    if row is None:
        return False

    stored_mtime = row[0]
    if stored_mtime is None:
        return False

    return abs(float(stored_mtime) - float(current_mtime)) < 1.0


def filter_incremental_sfc_data(df: pd.DataFrame, file_path: str, cfg: Dict[str, Any], conn) -> pd.DataFrame:
    """
    增量过滤：只返回新数据（未处理过的记录）
    基于业务字段进行hash匹配（不含文件名）
    """
    if df.empty:
        return df
    
    incr_cfg = cfg.get("incremental", {})
    if not incr_cfg.get("enabled", False):
        logging.info("增量处理未启用，返回全部数据")
        return df
    
    # 从SQLite获取已处理hash集合（仅针对本文件需要的hash集合做过滤）
    processed_hashes: Set[str] = set()
    
    # 获取唯一键字段（不含文件名）
    # 默认值应该与配置文件保持一致
    key_fields = incr_cfg.get("unique_key_fields", ["BatchNumber", "Operation", "TrackOutTime"])
    
    # 检查必需字段是否存在
    missing_fields = [f for f in key_fields if f not in df.columns]
    if missing_fields:
        logging.warning(f"增量处理所需字段不存在: {missing_fields}，返回全部数据")
        return df
    
    # 生成每条记录的hash并筛选新数据（不含文件名）
    df['_record_hash'] = df.apply(lambda row: generate_record_hash(row, key_fields), axis=1)

    try:
        conn.execute('CREATE TEMP TABLE IF NOT EXISTS temp_hashes (record_hash TEXT PRIMARY KEY)')
        conn.execute('DELETE FROM temp_hashes')
        hash_rows = [(h,) for h in df['_record_hash'].dropna().astype(str).unique().tolist()]
        conn.executemany('INSERT OR IGNORE INTO temp_hashes (record_hash) VALUES (?)', hash_rows)
        rows = conn.execute(
            'SELECT h.record_hash FROM etl_incremental_hashes h '
            'INNER JOIN temp_hashes t ON h.record_hash = t.record_hash '
            'WHERE h.etl_name = ?',
            (ETL_NAME,),
        ).fetchall()
        processed_hashes = {r[0] for r in rows}
    except Exception as e:
        logging.warning(f"从SQLite读取processed_hashes失败，将跳过hash过滤: {e}")
        processed_hashes = set()

    if processed_hashes:
        new_data = df[~df['_record_hash'].isin(processed_hashes)].copy()
    else:
        new_data = df.copy()
    new_data = new_data.drop(columns=['_record_hash'])
    
    new_count = len(new_data)
    total_count = len(df)
    existing_count = total_count - new_count
    
    file_name = os.path.basename(file_path)
    logging.info(f"文件 {file_name}: 总数据 {total_count} 行，已处理 {existing_count} 行，新数据 {new_count} 行")
    
    return new_data


def update_sfc_etl_state(df: pd.DataFrame, file_path: str, cfg: Dict[str, Any], conn) -> None:
    """更新ETL状态：记录已处理的记录hash和文件信息"""
    if df.empty:
        return
    
    incr_cfg = cfg.get("incremental", {})
    if not incr_cfg.get("enabled", False):
        return
    
    processed_hashes: Set[str] = set()
    
    # 获取唯一键字段（不含文件名）
    # 默认值应该与配置文件保持一致
    key_fields = incr_cfg.get("unique_key_fields", ["BatchNumber", "Operation", "TrackOutTime"])
    
    # 检查必需字段是否存在
    missing_fields = [f for f in key_fields if f not in df.columns]
    if missing_fields:
        logging.warning(f"更新状态所需字段不存在: {missing_fields}")
        return
    
    # 生成所有记录的hash（不含文件名）
    df['_record_hash'] = df.apply(lambda row: generate_record_hash(row, key_fields), axis=1)
    new_hashes = set(df['_record_hash'].dropna().astype(str).unique().tolist())

    file_name = os.path.basename(file_path)
    try:
        file_mtime = os.path.getmtime(file_path)
    except Exception:
        file_mtime = None

    now_ts = datetime.now().isoformat()

    try:
        if new_hashes:
            hash_rows = [(ETL_NAME, h) for h in new_hashes]
            conn.executemany(
                'INSERT OR IGNORE INTO etl_incremental_hashes (etl_name, record_hash) VALUES (?, ?)',
                hash_rows,
            )

        conn.execute(
            'INSERT INTO etl_incremental_files (etl_name, file_path, file_mtime, file_name, processed_time, record_count) '
            'VALUES (?, ?, ?, ?, ?, ?) '
            'ON CONFLICT(etl_name, file_path) DO UPDATE SET '
            'file_mtime = excluded.file_mtime, '
            'file_name = excluded.file_name, '
            'processed_time = excluded.processed_time, '
            'record_count = excluded.record_count',
            (ETL_NAME, file_path, file_mtime, file_name, now_ts, len(df)),
        )

        _set_last_processed_time(conn, now_ts)
    except Exception as e:
        logging.warning(f"写入SQLite增量状态失败: {e}")

    logging.info(f"状态更新完成：文件 {file_name}，新增hash {len(new_hashes)} 条")


def merge_with_history(new_df: pd.DataFrame, history_file: str, cfg: Dict[str, Any]) -> pd.DataFrame:
    """合并新数据和历史数据（用于最终输出）"""
    if new_df.empty:
        logging.warning("新数据为空，尝试加载历史数据")

        try:
            db = get_default_db_manager()
            with db.get_connection() as conn:
                history_df = pd.read_sql_query('SELECT * FROM "sfc_batch_report_latest"', conn)
            return history_df
        except Exception as e:
            logging.warning(f"加载SQLite历史数据失败: {e}")
            return new_df
    
    db = get_default_db_manager()
    try:
        try:
            with db.get_connection() as conn:
                history_df = pd.read_sql_query('SELECT * FROM "sfc_batch_report_latest"', conn)
            logging.info(f"加载SQLite历史SFC数据: {len(history_df)} 行")
        except Exception as e:
            logging.warning(f"加载SQLite历史SFC数据失败，将按空历史继续: {e}")
            history_df = pd.DataFrame()

        # 获取唯一键字段用于去重
        incr_cfg = cfg.get("incremental", {})
        # 默认值应该与配置文件保持一致
        key_fields = incr_cfg.get("unique_key_fields", ["BatchNumber", "Operation", "TrackOutTime"])

        # 确保字段存在（注意：去重时不包含文件名，因为历史数据可能来自多个文件）
        if history_df.empty:
            available_key_fields = [f for f in key_fields if f in new_df.columns]
        else:
            available_key_fields = [f for f in key_fields if f in new_df.columns and f in history_df.columns]

        if available_key_fields:
            # 合并数据
            combined_df = pd.concat([history_df, new_df], ignore_index=True)

            # 按TrackOutTime排序（如果有），最新的在前
            dedupe_cfg = cfg.get("deduplicate", {})
            sort_field = dedupe_cfg.get("sort_field", "TrackOutTime")
            if sort_field in combined_df.columns:
                sort_ascending = dedupe_cfg.get("sort_ascending", False)
                combined_df = combined_df.sort_values(sort_field, na_position='last', ascending=sort_ascending)

            # 去重：保留最新的（不包含文件名，因为历史数据可能来自多个文件）
            combined_df = combined_df.drop_duplicates(subset=available_key_fields, keep='last').reset_index(drop=True)

            logging.info(f"数据合并完成：历史 {len(history_df)} 行 + 新增 {len(new_df)} 行 = 合并后 {len(combined_df)} 行")
            try:
                db.bulk_insert(combined_df, 'sfc_batch_report_latest', if_exists='replace')
            except Exception as e:
                logging.warning(f"写入SQLite历史表失败: {e}")
            return combined_df

        logging.error(f"无法找到有效的唯一键字段进行合并。配置的字段: {key_fields}，新数据字段: {list(new_df.columns)}，历史数据字段: {list(history_df.columns)}")
        # 如果关键字段缺失，仍然尝试合并，但记录警告
        # 这种情况下可能会有重复数据，但至少不会丢失数据
        combined_df = pd.concat([history_df, new_df], ignore_index=True)
        logging.warning("由于字段不匹配，直接拼接数据，可能存在重复记录")
        try:
            db.bulk_insert(combined_df, 'sfc_batch_report_latest', if_exists='replace')
        except Exception as e:
            logging.warning(f"写入SQLite历史表失败: {e}")
        return combined_df

    except Exception as e:
        logging.error(f"合并历史数据失败: {e}，返回新数据")
        return new_df


def prompt_refresh_mode(default_incremental: bool = True, countdown_seconds: int = 10) -> bool:
    """
    提示用户选择刷新模式（增量/全量）
    
    参数:
        default_incremental: 默认是否增量刷新（True=增量，False=全量）
        countdown_seconds: 倒计时秒数，None表示不使用倒计时
    
    返回:
        True: 增量刷新
        False: 全量刷新
    """
    import threading
    import time
    
    print("\n" + "="*60)
    print("请选择数据处理模式：")
    print("="*60)
    print("1. 增量刷新（Incremental）- 只处理新数据，保留历史数据")
    print("2. 全量刷新（Full Refresh）- 清除所有历史数据，重新处理全部数据")
    print("="*60)
    
    if default_incremental:
        default_text = "增量刷新"
        default_num = 1
    else:
        default_text = "全量刷新"
        default_num = 2
    
    print(f"\n默认选择: {default_num} ({default_text})")
    
    user_choice = [None]  # 用于存储用户选择
    
    def get_user_input():
        """获取用户输入的线程函数"""
        try:
            print("请输入选择 (1 或 2)，直接回车使用默认选择:")
            user_input = input().strip()
            
            if user_input == "":
                user_choice[0] = default_incremental
                print(f"✓ 使用默认选择: {default_text}")
            elif user_input == "1":
                user_choice[0] = True
                print("✓ 用户选择: 增量刷新")
            elif user_input == "2":
                user_choice[0] = False
                print("✓ 用户选择: 全量刷新")
            else:
                print("输入无效，请输入 1（增量刷新）或 2（全量刷新），或直接回车使用默认选择:")
                # 递归调用重新获取输入
                user_choice[0] = prompt_refresh_mode(default_incremental, None)
                
        except KeyboardInterrupt:
            print("\n用户中断，使用默认选择")
            user_choice[0] = default_incremental
        except EOFError:
            print("\n输入结束，使用默认选择")
            user_choice[0] = default_incremental
    
    if countdown_seconds is not None:
        # 使用倒计时模式
        print(f"⏰ {countdown_seconds}秒后自动选择默认模式...")
        
        # 创建输入线程
        input_thread = threading.Thread(target=get_user_input)
        input_thread.daemon = True
        input_thread.start()
        
        # 倒计时等待
        for i in range(countdown_seconds, 0, -1):
            if user_choice[0] is not None:
                break
            print(f"\r⏰ 倒计时: {i}秒", end="", flush=True)
            time.sleep(1)
        
        if user_choice[0] is None:
            print(f"\r⏰ 倒计时结束，自动选择默认模式: {default_text}")
            return default_incremental
        else:
            print()  # 换行
            return user_choice[0]
    else:
        # 传统等待模式
        get_user_input()
        return user_choice[0] if user_choice[0] is not None else default_incremental


def should_do_full_refresh(cfg: Dict[str, Any], conn) -> bool:
    """判断是否应该执行全量刷新"""
    incr_cfg = cfg.get("incremental", {})
    threshold_days = incr_cfg.get("full_refresh_threshold_days")
    if not threshold_days or threshold_days <= 0:
        return False
    
    last_processed_time = _get_last_processed_time(conn)
    
    if not last_processed_time:
        logging.info("未找到上次处理时间，执行全量刷新")
        return True
    
    try:
        last_time = pd.to_datetime(last_processed_time)
        days_since = (datetime.now() - last_time).days
        if days_since >= threshold_days:
            logging.info(f"距离上次处理已过去 {days_since} 天（阈值: {threshold_days} 天），执行全量刷新")
            return True
    except Exception as e:
        logging.warning(f"解析上次处理时间失败: {e}，执行全量刷新")
        return True
    
    return False


def process_all_sfc_data(cfg: Dict[str, Any], force_full_refresh: bool = False) -> pd.DataFrame:
    """处理所有SFC数据的主函数（增量处理）"""
    # 读取SFC数据（支持通配符）
    sfc_path = cfg.get("source", {}).get("sfc_path", "")
    if not sfc_path:
        logging.warning("SFC数据路径未配置")
        return pd.DataFrame()
    
    # 增量处理配置
    incr_cfg = cfg.get("incremental", {})

    db = get_default_db_manager()
    with db.get_connection() as conn:
        _init_incremental_state_tables(conn)

        # 如果强制全量刷新，清理SQLite状态 + 历史表
        if force_full_refresh:
            logging.info("执行全量刷新：清除SQLite增量状态和历史表")
            _clear_incremental_state(conn)

            try:
                db.execute_sql('DROP TABLE IF EXISTS sfc_batch_report_latest')
                logging.info("已清除SQLite历史表: sfc_batch_report_latest")
            except Exception as e:
                logging.warning(f"清除SQLite历史表失败: {e}")

        # 检查是否需要全量刷新（自动判断）
        if incr_cfg.get("enabled", False) and not force_full_refresh:
            if should_do_full_refresh(cfg, conn):
                logging.info("执行全量刷新：清除SQLite增量状态")
                _clear_incremental_state(conn)
    
    # 兼容：后续增量状态已迁移到SQLite，不再读写外部JSON状态文件
    
    # 处理通配符
    if "*" in sfc_path or "?" in sfc_path:
        sfc_files = glob.glob(sfc_path)
        if not sfc_files:
            logging.warning(f"未找到匹配的SFC文件: {sfc_path}")
            return pd.DataFrame()
        
        # 按文件修改时间排序，最新的在前
        sfc_files = sorted(sfc_files, key=lambda x: os.path.getmtime(x), reverse=True)
        
        # 测试模式：限制文件数量
        test_cfg = cfg.get("test", {})
        test_enabled = test_cfg.get("enabled", False)
        if test_enabled:
            max_files = test_cfg.get("max_sfc_files", 10)
            if len(sfc_files) > max_files:
                sfc_files = sfc_files[:max_files]
                logging.info(f"测试模式：只处理最近 {max_files} 个SFC文件（共找到 {len(glob.glob(sfc_path))} 个）")
        
        # 增量处理：过滤已处理的文件（基于SQLite状态表）
        if incr_cfg.get("enabled", False):
            db = get_default_db_manager()
            with db.get_connection() as conn:
                _init_incremental_state_tables(conn)
                original_count = len(sfc_files)
                sfc_files = [f for f in sfc_files if not is_file_processed(f, conn)]
                skipped_count = original_count - len(sfc_files)
                if skipped_count > 0:
                    logging.info(f"跳过已处理的文件: {skipped_count} 个，剩余待处理: {len(sfc_files)} 个")
        
        if not sfc_files:
            logging.info("所有文件都已处理过，没有新数据，跳过处理")
            # 直接返回空DataFrame，不重新加载和保存历史数据
            return pd.DataFrame()
        
        logging.info(f"找到 {len(sfc_files)} 个SFC文件（将处理）")
        

        # 初始化历史数据（如果存在）
        if incr_cfg.get("enabled", False):
            try:
                db = get_default_db_manager()
                with db.get_connection() as conn:
                    sfc_df = pd.read_sql_query('SELECT * FROM "sfc_batch_report_latest"', conn)
                logging.info(f"加载初始SQLite历史数据: {len(sfc_df)} 行")
            except Exception as e:
                logging.warning(f"加载SQLite历史数据失败: {e}，从空数据开始")
                sfc_df = pd.DataFrame()
        else:
            sfc_df = pd.DataFrame()
        
        # 逐个文件处理，每处理完一个文件就与历史数据合并
        for file_path in sfc_files:
            try:
                logging.info(f"读取SFC文件: {file_path}")
                df = read_sharepoint_excel(file_path)  # 不限制行数
                
                if df.empty:
                    continue
                
                # 处理SFC数据（字段映射、类型转换等）
                df = process_sfc_data(df, cfg)
                
                # 增量过滤：先做去重分析，只保留新数据
                if incr_cfg.get("enabled", False):
                    df_before_filter = df.copy()
                    db = get_default_db_manager()
                    with db.get_connection() as conn:
                        _init_incremental_state_tables(conn)
                        df = filter_incremental_sfc_data(df, file_path, cfg, conn)

                        # 更新状态（记录已处理的记录hash和文件信息）
                        # 使用过滤前的数据更新状态，确保所有记录都被标记为已处理
                        update_sfc_etl_state(df_before_filter, file_path, cfg, conn)
                    
                    # 只对新数据合并标准时间和计算指标（节约资源）
                    if not df.empty:
                        # 合并标准时间数据（只对新数据）
                        df = merge_standard_time_sfc(df, cfg)
                        
                        # 计算指标（LT, PT, ST, DueTime等）
                        df = calculate_sfc_metrics(df, cfg)
                        
                        # 立即与历史数据合并
                        sfc_df = merge_with_history(df, "", cfg)
                        logging.info(f"文件处理完成并已合并到历史数据: {file_path}")
                    else:
                        logging.info(f"文件无新数据: {file_path}")
                else:
                    # 如果不启用增量处理，直接处理全部数据
                    if not df.empty:
                        # 合并标准时间数据
                        df = merge_standard_time_sfc(df, cfg)
                        
                        # 计算指标（LT, PT, ST, DueTime等）
                        df = calculate_sfc_metrics(df, cfg)
                        
                        # 合并历史数据
                        sfc_df = merge_with_history(df, "", cfg)
                    
            except Exception as e:
                logging.warning(f"读取SFC文件失败 {file_path}: {e}")
        
        if sfc_df.empty:
            logging.info("处理完成后数据为空")
            return pd.DataFrame()
        
        logging.info(f"所有文件处理完成，最终数据行数: {len(sfc_df)}")
        
    else:
        # 单个文件处理
        if not os.path.exists(sfc_path):
            logging.warning(f"SFC数据路径不存在: {sfc_path}")
            return pd.DataFrame()
        
        # 检查文件是否已处理
        if incr_cfg.get("enabled", False):
            try:
                db = get_default_db_manager()
                with db.get_connection() as conn:
                    _init_incremental_state_tables(conn)
                    if is_file_processed(sfc_path, conn):
                        logging.info(f"文件已处理过，跳过: {sfc_path}")
                        try:
                            return pd.read_sql_query('SELECT * FROM "sfc_batch_report_latest"', conn)
                        except Exception as e:
                            logging.warning(f"加载SQLite历史数据失败: {e}")
                            return pd.DataFrame()
            except Exception as e:
                logging.warning(f"检查SQLite增量状态失败，将继续处理该文件: {e}")
        
        logging.info(f"读取SFC数据: {sfc_path}")
        sfc_df = read_sharepoint_excel(sfc_path)
        
        if sfc_df.empty:
            return pd.DataFrame()
        
        # 处理SFC数据
        sfc_df = process_sfc_data(sfc_df, cfg)
        
        # 增量过滤：先做去重分析，只保留新数据
        if incr_cfg.get("enabled", False):
            sfc_df_before_filter = sfc_df.copy()
            db = get_default_db_manager()
            with db.get_connection() as conn:
                _init_incremental_state_tables(conn)
                sfc_df = filter_incremental_sfc_data(sfc_df, sfc_path, cfg, conn)
                # 使用过滤前的数据更新状态，确保所有记录都被标记为已处理
                update_sfc_etl_state(sfc_df_before_filter, sfc_path, cfg, conn)
            
            # 只对新数据合并标准时间和计算指标（节约资源）
            if not sfc_df.empty:
                # 合并标准时间数据（只对新数据）
                sfc_df = merge_standard_time_sfc(sfc_df, cfg)
                
                # 计算指标（LT, PT, ST, DueTime等）
                sfc_df = calculate_sfc_metrics(sfc_df, cfg)
            
            # 合并历史数据
            sfc_df = merge_with_history(sfc_df, "", cfg)
        else:
            # 如果不启用增量处理，处理全部数据
            # 合并标准时间数据
            sfc_df = merge_standard_time_sfc(sfc_df, cfg)
            
            # 计算指标（LT, PT, ST, DueTime等）
            sfc_df = calculate_sfc_metrics(sfc_df, cfg)
            
            # 合并历史数据
            sfc_df = merge_with_history(sfc_df, "", cfg)
    
    return sfc_df


if __name__ == "__main__":
    # 固定使用增量刷新模式（由BAT脚本统一控制）
    # 移除命令行参数，保持ETL脚本纯净
    
    cfg = load_config(CONFIG_PATH)
    setup_logging_with_rotation(cfg, project_root)
    
    # 固定使用增量刷新
    is_incremental = True
    force_full_refresh = False
    
    logging.info("="*60)
    logging.info("SFC批量报告ETL启动")
    logging.info("刷新模式: 增量刷新（固定）")
    logging.info("="*60)
    
    t0 = time.time()
    try:
        # 处理SFC数据
        sfc_result_df = process_all_sfc_data(cfg, force_full_refresh=force_full_refresh)
        
        if sfc_result_df.empty:
            logging.info("没有新数据需要处理，保持现有数据不变")
        else:
            # 在最终保存前，对所有合并后的数据统一重新计算 PreviousBatchEndTime
            # 确保排序准确（方案A：最终统一计算）
            logging.info("对所有合并后的数据统一计算 PreviousBatchEndTime")
            sfc_result_df = calculate_previous_batch_end_time(sfc_result_df)
            
            # 增量处理：状态已写入SQLite
            incr_cfg = cfg.get("incremental", {})
            if incr_cfg.get("enabled", False):
                # 状态已在处理过程中更新，这里只需要确保保存了
                logging.info("增量处理：状态已更新")
            
            if WRITE_PARQUET_OUTPUT:
                # 保存结果到双份位置
                output_dir = cfg.get("output", {}).get("base_dir", "publish")
                output_dir = os.path.join(BASE_DIR, output_dir) if not os.path.isabs(output_dir) else output_dir
                os.makedirs(output_dir, exist_ok=True)
                
                # 本地项目目录
                local_publish_dir = os.path.join(project_root, "data_pipelines", "sources", "sfc", "publish")
                os.makedirs(local_publish_dir, exist_ok=True)
                
                # 保存到双份位置（主输出 + 项目目录）
                sfc_latest_file = os.path.join(output_dir, "SFC_batch_report_latest.parquet")
                local_file = os.path.join(local_publish_dir, "SFC_batch_report_latest.parquet")
                save_to_dual_locations(sfc_result_df, sfc_latest_file, local_file, cfg)
                logging.info(f"SFC数据已保存到双份位置")

            try:
                db = get_default_db_manager()
                db.bulk_insert(sfc_result_df, 'sfc_batch_report_latest', if_exists='replace')
                logging.info(f"已写入SQLite表 sfc_batch_report_latest: {len(sfc_result_df)} 行")
            except Exception as e:
                logging.warning(f"写入SQLite表 sfc_batch_report_latest 失败: {e}")
        
        logging.info(f"处理完成，耗时: {time.time() - t0:.2f}秒")
        
    except Exception as e:
        logging.exception(f"ETL失败: {e}")
        sys.exit(1)

