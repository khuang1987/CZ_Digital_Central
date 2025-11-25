"""
SA指标数据清洗ETL脚本
功能：从SharePoint读取Excel数据，进行数据处理、匹配、分组计算，输出为Parquet格式供Power BI使用
参考：80-数据清洗的处理方法，但增加了完整的数据计算逻辑
"""

import os
import sys
import time
import logging
import glob
import json
import hashlib
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Set
import re
import warnings
warnings.filterwarnings('ignore')

# 导入ETL通用工具
from etl_utils import (
    setup_logging,
    load_config,
    read_sharepoint_excel,
    save_to_parquet,
    prompt_refresh_mode,
    update_etl_state,
    get_base_dir,
    ensure_directory_exists,
    # 多工厂数据处理工具
    read_multi_factory_mes_data,
    validate_multi_factory_data,
    get_factory_summary
)

# Windows平台支持
try:
    import msvcrt
    HAS_MSVCRT = True
except ImportError:
    HAS_MSVCRT = False

import pandas as pd
import numpy as np
import yaml
from openpyxl import load_workbook

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    print("警告：未安装pyarrow，将无法保存Parquet格式")
    pq = None

# 配置基础路径
BASE_DIR = get_base_dir()
CONFIG_PATH = os.path.join(BASE_DIR, "..", "03_配置文件", "config", "config_mes_batch_report.yaml")




def process_mes_data(df: pd.DataFrame, cfg: Dict[str, Any]) -> pd.DataFrame:
    """
    处理MES基础前处理数据
    对应Power Query: e2_批次报工记录_MES_基础前处理.pq
    """
    if df.empty:
        return df
    
    result = df.copy()
    
    # 0. 清理列名的前后空格（避免字段映射失败）
    result.columns = result.columns.str.strip()
    
    # 1. 字段映射（根据实际Excel列名调整）
    column_mapping = cfg.get("mes_mapping", {})
    if column_mapping:
        # 过滤掉注释的映射（空字符串或None）
        column_mapping = {k: v for k, v in column_mapping.items() if v and not str(v).startswith('#')}
        if column_mapping:
            result = result.rename(columns=column_mapping)
            logging.debug(f"字段映射完成: {column_mapping}")
        else:
            logging.warning("字段映射配置为空，请检查config.yaml中的mes_mapping配置")
    else:
        logging.warning("未配置字段映射，将使用Excel原始列名")
    
    logging.debug(f"映射后列名: {list(result.columns)}")
    
    # 2. 过滤批次号包含"-"的记录
    if "BatchNumber" in result.columns:
        result = result[~result["BatchNumber"].astype(str).str.contains("-", na=False)]
    
    # 3. 类型转换
    type_config = cfg.get("mes_types", {})
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
                logging.warning(f"类型转换失败 {col} -> {dtype}: {e}")
    
    # 4. 工序名称标准化
    if "Operation description" in result.columns:
        result["Operation description"] = result["Operation description"].apply(standardize_operation_name)
    
    # 5. 工序号补零为4位，批次号转为文本
    if "Operation" in result.columns:
        def clean_operation(op):
            if pd.isna(op) or op is None:
                return None
            try:
                op_num = float(op)
                return f"{int(op_num):04d}"
            except (ValueError, TypeError):
                return str(op).strip().zfill(4) if str(op).strip() else None
        
        result["Operation"] = result["Operation"].apply(clean_operation)
    if "BatchNumber" in result.columns:
        result["BatchNumber"] = result["BatchNumber"].astype(str).str.strip()
    
    # 6. 添加TrackOutDate（从TrackOutTime提取日期）
    # 注意：TrackOutTime字段已通过字段映射从TrackOutDate创建，现在需要提取日期部分
    if "TrackOutTime" in result.columns:
        result["TrackOutDate"] = pd.to_datetime(result["TrackOutTime"], errors='coerce').dt.date
    
    # 7. 从Group字段提取数字部分
    if "Group" in result.columns:
        result["Group"] = result["Group"].apply(extract_group_number)
        logging.debug(f"已从Group字段提取数字部分")
    
    # 8. 从Resource字段提取资源代码，创建machine字段
    if "Resource" in result.columns:
        # 提取资源代码并创建machine字段
        result["machine"] = result["Resource"].apply(extract_resource_code)
        logging.debug(f"已从Resource字段提取资源代码，创建machine字段")
    else:
        # 如果Resource字段不存在，创建空字段
        result["machine"] = None
        logging.warning("未找到Resource字段，无法提取资源代码")
    
    # 10. 按machine分组，计算TrackInTime和Setup
    result = calculate_trackin_time_and_setup(result)
    
    return result


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
    """
    MES工序名称清洗合并标准化
    根据分析报告的合并方案进行清洗
    
    清洗规则：
    1. 去除工厂前缀（CZM、CKH等）
    2. 去除外协标识（（可外协）、（外协））
    3. 同类工序合并
    """
    if pd.isna(op_name) or op_name is None:
        return ""
    
    op_str = str(op_name).strip()
    
    # 1. 去除工厂前缀（支持CZM、CKH等工厂代码）
    if op_str.startswith("CZM "):
        op_str = op_str[4:]  # 去除"CZM "
    elif op_str.startswith("CKH "):
        op_str = op_str[4:]  # 去除"CKH "
    
    # 2. 去除外协标识
    op_str = op_str.replace("（可外协）", "").replace("（外协）", "")
    op_str = op_str.replace("(可外协)", "").replace("(外协)", "")
    op_str = op_str.strip()  # 去除可能产生的多余空格
    
    # 3. 同类工序合并（按照分析报告的合并方案）
    if op_str.startswith("线切割"):
        return "线切割"
    elif op_str.startswith("数控铣"):
        return "数控铣"
    elif op_str.startswith("纵切车"):
        return "纵切车"
    elif op_str.startswith("数控车"):
        return "数控车"
    elif op_str.startswith("车削"):
        return "车削"
    elif op_str.startswith("锯"):
        return "锯"
    # 以下工序保持独立，只做基础清洗
    elif op_str.startswith("清洗"):
        return "清洗"
    elif op_str.startswith("终检"):
        return "终检"
    elif op_str.startswith("钳工"):
        return "钳工"
    elif op_str.startswith("钝化"):
        return "钝化"
    elif op_str.startswith("点钝化"):
        return "点钝化"
    elif op_str.startswith("喷砂"):
        if "微" in op_str:
            return "微喷砂"
        else:
            return "喷砂"
    elif op_str.startswith("包装"):
        return "包装"
    elif op_str.startswith("电解"):
        if "去氢" in op_str:
            return "电解去氢"
        else:
            return "电解"
    elif op_str.startswith("抛光"):
        return "抛光"
    elif op_str.startswith("激光打标"):
        return "激光打标"
    elif op_str.startswith("真空热处理"):
        return "真空热处理"
    elif op_str.startswith("非真空热处理"):
        return "非真空热处理"
    elif op_str.startswith("研磨"):
        return "研磨"
    elif op_str.startswith("无心磨"):
        return "无心磨"
    elif op_str.startswith("Preparation step"):
        return "Preparation step"
    elif op_str.startswith("五轴磨"):
        return "五轴磨"
    elif op_str.startswith("折弯"):
        return "折弯"
    elif op_str.startswith("氩弧焊"):
        return "氩弧焊"
    elif op_str.startswith("注塑"):
        return "注塑"
    elif op_str.startswith("涂层"):
        return "涂层"
    elif op_str.startswith("涂色"):
        return "涂色"
    elif op_str.startswith("深孔钻"):
        return "深孔钻"
    elif op_str.startswith("激光焊接"):
        return "激光焊接"
    elif op_str.startswith("装配"):
        return "装配"
    elif op_str.startswith("阳极氧化"):
        return "阳极氧化"
    elif op_str.startswith("电火花"):
        return "电火花"
    elif op_str.startswith("镀铬"):
        return "镀铬"
    else:
        # 对于未匹配的工序，也进行基础清洗（去CZM前缀和外协标识）
        return op_str


def calculate_trackin_time_and_setup(df: pd.DataFrame) -> pd.DataFrame:
    """
    按machine分组，计算Setup
    注意：TrackInTime保持源表的原始值，不进行修改
    """
    if df.empty or "machine" not in df.columns:
        if df.empty:
            logging.warning("数据为空，跳过分组计算")
        else:
            logging.warning("machine字段不存在，跳过分组计算")
        return df
    
    # 检查必要字段是否存在
    if "TrackOutTime" not in df.columns:
        logging.warning("TrackOutTime字段不存在，无法进行分组计算，返回原数据")
        return df
    
    if "CFN" not in df.columns:
        logging.warning("CFN字段不存在，无法进行分组计算，返回原数据")
        return df
    
    result_list = []
    
    for resource_code, group in df.groupby("machine"):
        group_sorted = group.sort_values("TrackOutTime", na_position='last').reset_index(drop=True)
        
        # 获取上一行的CFN（用于计算Setup）
        group_sorted['_prev_cfn'] = group_sorted['CFN'].shift(1)
        
        # TrackInTime保持源表的原始值，不进行修改
        # 如果源表中没有TrackInTime字段，尝试使用StartTime
        if "TrackInTime" not in group_sorted.columns:
            if "StartTime" in group_sorted.columns:
                group_sorted['TrackInTime'] = group_sorted['StartTime']
            else:
                group_sorted['TrackInTime'] = None
        
        # 计算Setup：如果上一行的CFN与当前CFN相同，则为"No"，否则为"Yes"
        group_sorted['Setup'] = group_sorted.apply(
            lambda row: "No" if pd.notna(row['_prev_cfn']) and row['_prev_cfn'] == row.get('CFN', None) else "Yes",
            axis=1
        )
        
        # 删除临时列
        group_sorted = group_sorted.drop(columns=['_prev_cfn'])
        
        result_list.append(group_sorted)
    
    return pd.concat(result_list, ignore_index=True)


def merge_sfc_data(mes_df: pd.DataFrame, cfg: Dict[str, Any]) -> pd.DataFrame:
    """
    合并SFC数据获取Checkin_SFC字段
    按BatchNumber、Operation匹配
    对应Power Query: e2_批次报工记录_MES_后处理.pq 中的 MergedSFC 步骤
    """
    if mes_df.empty:
        return mes_df
    
    # 获取SFC latest文件路径
    sfc_latest_file = cfg.get("source", {}).get("sfc_latest_file", "publish/SFC_batch_report_latest.parquet")
    sfc_latest_file = os.path.join(BASE_DIR, sfc_latest_file) if not os.path.isabs(sfc_latest_file) else sfc_latest_file
    
    if not os.path.exists(sfc_latest_file):
        logging.warning(f"SFC数据文件不存在: {sfc_latest_file}，跳过合并Checkin_SFC")
        mes_df["Checkin_SFC"] = None
        return mes_df
    
    try:
        sfc_df = pd.read_parquet(sfc_latest_file)
        logging.info(f"读取SFC数据: {len(sfc_df)} 行")
    except Exception as e:
        logging.warning(f"读取SFC数据失败: {e}，跳过合并Checkin_SFC")
        mes_df["Checkin_SFC"] = None
        return mes_df
    
    # 检查SFC数据是否有必要的字段
    if "BatchNumber" not in sfc_df.columns or "Operation" not in sfc_df.columns:
        logging.warning("SFC数据缺少必要字段（BatchNumber或Operation），跳过合并Checkin_SFC")
        mes_df["Checkin_SFC"] = None
        return mes_df
    
    # 兼容字段：如果SFC数据使用的是 CheckInTime，则统一映射为 Checkin_SFC
    if "Checkin_SFC" not in sfc_df.columns and "CheckInTime" in sfc_df.columns:
        try:
            sfc_df["Checkin_SFC"] = sfc_df["CheckInTime"]
            logging.info("SFC数据使用CheckInTime，已映射为Checkin_SFC用于合并")
        except Exception as e:
            logging.warning(f"映射CheckInTime到Checkin_SFC失败: {e}")
            sfc_df["Checkin_SFC"] = None

    # 合并键：只使用 BatchNumber + Operation
    sfc_join_cols = ["BatchNumber", "Operation"]
    mes_join_cols = ["BatchNumber", "Operation"]
    
    # 检查合并键字段是否存在
    if not all(col in sfc_df.columns for col in sfc_join_cols):
        logging.warning(f"SFC数据缺少合并字段，需要: {sfc_join_cols}，跳过合并Checkin_SFC")
        mes_df["Checkin_SFC"] = None
        return mes_df
    
    if not all(col in mes_df.columns for col in mes_join_cols):
        logging.warning(f"MES数据缺少合并字段，需要: {mes_join_cols}，跳过合并Checkin_SFC")
        mes_df["Checkin_SFC"] = None
        return mes_df
    
    # 检查Checkin_SFC字段是否存在
    if "Checkin_SFC" not in sfc_df.columns:
        logging.warning("SFC数据缺少Checkin_SFC字段，跳过合并")
        mes_df["Checkin_SFC"] = None
        return mes_df
    
    # 合并（左连接，保留所有MES数据）
    # 注意：SFC数据源中字段已经是Checkin_SFC，不会产生重复列名
    result = mes_df.merge(
        sfc_df[sfc_join_cols + ["Checkin_SFC"]],
        on=mes_join_cols,
        how="left"
    )
    
    matched_count = result["Checkin_SFC"].notna().sum()
    logging.info(f"合并SFC数据完成: {len(result)} 行，匹配到Checkin_SFC: {matched_count} 行")
    
    return result


def merge_standard_time(mes_df: pd.DataFrame, cfg: Dict[str, Any]) -> pd.DataFrame:
    """
    合并标准时间表（从合并后的parquet文件读取）
    按CFN、Operation、Group匹配
    """
    if mes_df.empty:
        return mes_df
    
    # 获取合并后的标准时间表路径
    std_time_path = cfg.get("source", {}).get("standard_time_path", "")
    
    # 处理路径：直接使用latest版本
    if not std_time_path:
        # 如果配置为空，使用默认latest文件
        std_time_path = "publish/SAP_Routing_latest.parquet"
    
    # 处理相对路径
    if not os.path.isabs(std_time_path):
        std_time_path = os.path.join(BASE_DIR, std_time_path)
    
    # 检查文件是否存在
    if os.path.exists(std_time_path):
        logging.info(f"使用标准时间表: {os.path.basename(std_time_path)}")
    else:
        logging.warning(f"标准时间表文件不存在: {std_time_path}，跳过合并")
        # 添加空的标准时间列
        for col in ["EH_machine(s)", "EH_labor(s)", "OEE", "Setup Time (h)"]:
            mes_df[col] = None
        return mes_df
    
    try:
        std_time_df = pd.read_parquet(std_time_path)
        logging.info(f"读取合并后的标准时间表: {len(std_time_df)} 行")
    except Exception as e:
        logging.warning(f"读取标准时间表失败: {e}，跳过合并")
        for col in ["EH_machine(s)", "EH_labor(s)", "OEE", "Setup Time (h)"]:
            mes_df[col] = None
        return mes_df
    
    # 检查必要字段（至少需要CFN或Material Number之一）
    required_cols = ["Operation", "Group"]
    missing_cols = [col for col in required_cols if col not in std_time_df.columns]
    if missing_cols:
        logging.warning(f"标准时间表缺少必要字段: {missing_cols}，跳过合并")
        for col in ["EH_machine(s)", "EH_labor(s)", "OEE", "Setup Time (h)"]:
            mes_df[col] = None
        return mes_df
    
    # 检查是否有CFN或Material Number字段
    has_cfn = "CFN" in std_time_df.columns
    has_material_number = "Material Number" in std_time_df.columns
    if not has_cfn and not has_material_number:
        logging.warning("标准时间表缺少CFN和Material Number字段，无法进行匹配")
        for col in ["EH_machine(s)", "EH_labor(s)", "OEE", "Setup Time (h)"]:
            mes_df[col] = None
        return mes_df
    
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
            mes_df[col] = None
        return mes_df
    
    # 第一步：使用MES的CFN匹配标准表的Material Number
    if "CFN" in mes_df.columns and "Material Number" in std_time_df.columns:
        # 准备合并键：MES的CFN对应标准表的Material Number
        merge_cols_mes = ["CFN", "Operation", "Group"]
        merge_cols_std = ["Material Number", "Operation", "Group"]
        
        # 合并前统一数据类型
        for i, mes_col in enumerate(merge_cols_mes):
            std_col = merge_cols_std[i]
            if mes_col in mes_df.columns and std_col in std_time_df.columns:
                # 统一转换为字符串类型
                mes_df[mes_col] = mes_df[mes_col].astype(str)
                std_time_df[std_col] = std_time_df[std_col].astype(str)
                # 处理NaN值（转换为空字符串）
                mes_df[mes_col] = mes_df[mes_col].replace('nan', '').replace('None', '')
                std_time_df[std_col] = std_time_df[std_col].replace('nan', '').replace('None', '')
        
        # 使用left_on和right_on避免列名冲突
        # 第一步：使用MES的CFN匹配标准表的Material Number
        result = mes_df.merge(
            std_time_df[merge_cols_std + value_cols],
            left_on=merge_cols_mes,
            right_on=merge_cols_std,
            how="left"
        )
    else:
        # 如果标准时间表没有Material Number字段，直接使用MES数据，后续用CFN匹配
        result = mes_df.copy()
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
    
    # 第二步：对未匹配的记录，使用MES的CFN匹配标准表的CFN
    # 检查是否有未匹配的记录（EH_machine(s)和EH_labor(s)都为空）
    unmatched_mask = result["EH_machine(s)"].isna() & result["EH_labor(s)"].isna()
    unmatched_count = unmatched_mask.sum()
    
    if unmatched_count > 0 and "CFN" in std_time_df.columns and "CFN" in mes_df.columns:
        logging.info(f"第一步Material Number匹配后，还有 {unmatched_count} 条记录未匹配，尝试使用CFN匹配")
        
        # 获取未匹配的记录
        unmatched_df = result[unmatched_mask].copy()
        
        # 准备CFN匹配的合并键
        cfn_merge_cols = ["CFN", "Operation", "Group"]
        
        # 统一数据类型
        for col in cfn_merge_cols:
            if col in unmatched_df.columns and col in std_time_df.columns:
                unmatched_df[col] = unmatched_df[col].astype(str)
                std_time_df[col] = std_time_df[col].astype(str)
                unmatched_df[col] = unmatched_df[col].replace('nan', '').replace('None', '')
                std_time_df[col] = std_time_df[col].replace('nan', '').replace('None', '')
        
        # 使用CFN匹配
        cfn_value_cols = []
        if "Machine" in std_time_df.columns:
            cfn_value_cols.append("Machine")
        if "Labor" in std_time_df.columns:
            cfn_value_cols.append("Labor")
        if "Quantity" in std_time_df.columns:
            cfn_value_cols.append("Quantity")
        if "OEE" in std_time_df.columns:
            cfn_value_cols.append("OEE")
        if "Setup Time (h)" in std_time_df.columns:
            cfn_value_cols.append("Setup Time (h)")
        
        if cfn_value_cols:
            # 合并
            cfn_matched = unmatched_df.merge(
                std_time_df[cfn_merge_cols + cfn_value_cols],
                on=cfn_merge_cols,
                how="left"
            )
            
            # 计算单件时间
            if "Machine" in cfn_matched.columns and "Quantity" in cfn_matched.columns:
                quantity = cfn_matched["Quantity"].fillna(1).replace(0, 1)
                cfn_matched["EH_machine(s)"] = cfn_matched["Machine"] / quantity
                cfn_matched = cfn_matched.drop(columns=["Machine"])
            else:
                cfn_matched["EH_machine(s)"] = None
            
            if "Labor" in cfn_matched.columns and "Quantity" in cfn_matched.columns:
                quantity = cfn_matched["Quantity"].fillna(1).replace(0, 1)
                cfn_matched["EH_labor(s)"] = cfn_matched["Labor"] / quantity
                cfn_matched = cfn_matched.drop(columns=["Labor"])
            else:
                cfn_matched["EH_labor(s)"] = None
            
            if "Quantity" in cfn_matched.columns:
                cfn_matched = cfn_matched.drop(columns=["Quantity"])
            
            # 更新result中未匹配的记录
            # 只更新那些在cfn_matched中匹配成功的记录
            cfn_matched_mask = cfn_matched["EH_machine(s)"].notna() | cfn_matched["EH_labor(s)"].notna()
            if cfn_matched_mask.any():
                # 获取匹配成功的记录索引
                matched_indices = cfn_matched[cfn_matched_mask].index
                
                # 更新result中对应索引的记录
                for col in ["EH_machine(s)", "EH_labor(s)", "OEE", "Setup Time (h)"]:
                    if col in cfn_matched.columns:
                        result.loc[matched_indices, col] = cfn_matched.loc[matched_indices, col]
                
                cfn_matched_count = len(matched_indices)
                logging.info(f"第二步CFN匹配成功: {cfn_matched_count} 条记录")
    
    # 确保OEE有默认值
    if "OEE" in result.columns:
        result["OEE"] = result["OEE"].fillna(0.77).replace(0, 0.77)
    
    # 统计匹配结果
    total_matched = (result["EH_machine(s)"].notna() | result["EH_labor(s)"].notna()).sum()
    logging.info(f"合并标准时间数据完成: {len(result)} 行，总匹配成功: {total_matched} 行")
    if "EH_machine(s)" in result.columns:
        logging.info(f"匹配到EH_machine(s): {result['EH_machine(s)'].notna().sum()} 行")
    if "EH_labor(s)" in result.columns:
        logging.info(f"匹配到EH_labor(s): {result['EH_labor(s)'].notna().sum()} 行")
    if "OEE" in result.columns:
        logging.info(f"匹配到OEE: {result['OEE'].notna().sum()} 行")
    
    return result


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
        # 确保TrackOutTime是datetime类型
        if result["TrackOutTime"].dtype not in ['datetime64[ns]', 'datetime64[us]', 'datetime64[ms]']:
            result["TrackOutTime"] = pd.to_datetime(result["TrackOutTime"], errors='coerce')
        
        # 使用shift(1)获取上一批的TrackOutTime，保持datetime类型
        result.loc[mask_valid, "PreviousBatchEndTime"] = (
            result[mask_valid].groupby("machine")["TrackOutTime"].shift(1)
        )
        
        # 确保PreviousBatchEndTime是datetime类型
        if "PreviousBatchEndTime" in result.columns:
            result["PreviousBatchEndTime"] = pd.to_datetime(result["PreviousBatchEndTime"], errors='coerce')
        
        # 对于第一批（PreviousBatchEndTime 为空），使用 EnterStepTime 代替
        mask_first_batch = mask_valid & result["PreviousBatchEndTime"].isna()
        if "EnterStepTime" in result.columns and mask_first_batch.any():
            # 确保EnterStepTime是datetime类型
            if result["EnterStepTime"].dtype not in ['datetime64[ns]', 'datetime64[us]', 'datetime64[ms]']:
                result["EnterStepTime"] = pd.to_datetime(result["EnterStepTime"], errors='coerce')
            result.loc[mask_first_batch, "PreviousBatchEndTime"] = result.loc[mask_first_batch, "EnterStepTime"]
    
    # 统计计算结果
    total_count = len(result)
    calculated_count = result["PreviousBatchEndTime"].notna().sum()
    valid_calculated = (mask_valid & result["PreviousBatchEndTime"].notna()).sum() if mask_valid.any() else 0
    logging.info(f"计算 PreviousBatchEndTime 完成：总计 {total_count} 条，成功计算 {calculated_count} 条（其中有效machine记录 {valid_calculated} 条）")
    
    return result


def calculate_metrics(df: pd.DataFrame, cfg: Dict[str, Any]) -> pd.DataFrame:
    """
    计算所有指标字段
    对应Power Query: e2_批次报工记录_MES_后处理.pq
    """
    if df.empty:
        return df
    
    result = df.copy()
    
    # 记录实际存在的列名（用于调试）
    logging.debug(f"数据列名: {list(result.columns)}")
    
    # 加载日历表
    calendar_file = cfg.get("source", {}).get("calendar_file", "")
    calendar_df = load_calendar_table(calendar_file) if calendar_file else pd.DataFrame()
    
    # 获取每日工作时间配置
    daily_working_hours = cfg.get("source", {}).get("daily_working_hours", 8.0)
    
    # 0. 计算 PreviousBatchEndTime（在计算LT/PT之前）
    # 注意：如果数据是增量处理的，这里只对新数据计算，最终会在合并后统一重新计算
    # 这里先计算是为了后续可能使用该字段进行计算
    result = calculate_previous_batch_end_time(result)
    
    # 1. 计算LT(d)
    result["LT(d)"] = result.apply(calculate_lt, axis=1)
    
    # 2. 计算PT(d)
    result["PT(d)"] = result.apply(calculate_pt, axis=1)
    
    # 3. 处理OEE默认值
    if "OEE" in result.columns:
        result["OEE"] = result["OEE"].fillna(0.77).replace(0, 0.77)
    
    # 4. 确保EH_machine(s)和EH_labor(s)字段存在
    if "EH_machine(s)" not in result.columns:
        result["EH_machine(s)"] = None
    if "EH_labor(s)" not in result.columns:
        result["EH_labor(s)"] = None
    
    # 5. 计算ST(d)
    result["ST(d)"] = result.apply(calculate_st, axis=1)
    
    # 6. 计算DueTime和NonWorkday(d)（基于日历表）
    result["DueTime"] = result.apply(lambda row: calculate_due_time(row, calendar_df, daily_working_hours), axis=1)
    result["NonWorkday(d)"] = result.apply(lambda row: calculate_nonworkday_days(row, calendar_df), axis=1)
    
    # 7. 计算CompletionStatus（基于PT和ST比较，考虑容差和换批时间）
    result["CompletionStatus"] = result.apply(lambda row: calculate_completion_status(row, calendar_df), axis=1)
    
    # 8. 计算容差小时数（单独字段，与SFC逻辑一致）
    result["Tolerance(h)"] = result.apply(calculate_tolerance_hours, axis=1)
    
    # 9. 计算Machine(#) - 检查machine字段是否存在
    if "machine" in result.columns:
        result["Machine(#)"] = result["machine"].apply(extract_machine_number)
    else:
        logging.warning("machine字段不存在，无法计算Machine(#)")
        result["Machine(#)"] = None
    
    # 10. 筛选只保留需要的字段
    required_columns = [
        "BatchNumber", "CFN", "ProductionOrder", "Operation", "Operation description",
        "Group", "machine", "StepInQuantity", "TrackOutQuantity",
        "EnterStepTime", "Checkin_SFC", "TrackInTime", "TrackOutTime", "TrackOutDate",
        "PreviousBatchEndTime", "DueTime", "NonWorkday(d)", "CompletionStatus", "Tolerance(h)",
        "LT(d)", "PT(d)", "ST(d)",
        "Setup", "Setup Time (h)", "OEE", "EH_machine(s)", "EH_labor(s)", "Machine(#)",
        "VSM", "ERPCode", "Product_Description",
        # 新增：多工厂数据标识字段
        "factory_source", "factory_name"
    ]
    
    # 只保留实际存在的字段
    existing_columns = [col for col in required_columns if col in result.columns]
    
    # 如果缺少某些必需字段，记录警告
    missing_columns = [col for col in required_columns if col not in result.columns]
    if missing_columns:
        logging.warning(f"MES数据缺少以下字段（将被忽略）: {missing_columns}")
    
    # 筛选字段
    result = result[existing_columns].copy()
    
    logging.info(f"MES数据字段筛选完成：保留 {len(existing_columns)} 个字段")
    
    return result


def calculate_lt(row: pd.Series) -> Optional[float]:
    """计算LT(d) - 实际加工时间，不扣除周末"""
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
        return None
    
    trackout_dt = pd.to_datetime(trackout)
    start_dt = pd.to_datetime(start_time)
    
    # 计算实际时间差（天），不扣除周末
    total_seconds = (trackout_dt - start_dt).total_seconds()
    total_hours = total_seconds / 3600
    
    # 转换为天数（不扣除周末）
    return round(total_hours / 24, 2)


def calculate_pt(row: pd.Series) -> Optional[float]:
    """
    计算PT(d) - 实际加工时间
    
    升级逻辑：避免将设备停产时间计入PT
    - 正常情况：PT = TrackOutTime - PreviousBatchEndTime
    - 特殊情况：如果EnterStepTime > PreviousBatchEndTime，说明中间有停产期
    - 升级后：PT = TrackOutTime - TrackInTime（不包括停产等待时间）
    
    根据业务逻辑：
    1. 如果EnterStepTime <= PreviousBatchEndTime：正常连续生产
       PT(d) = (TrackOutTime - PreviousBatchEndTime) / 24
    2. 如果EnterStepTime > PreviousBatchEndTime：中间有停产期
       PT(d) = (TrackOutTime - TrackInTime) / 24
    3. 如果PreviousBatchEndTime为空，使用TrackInTime
    """
    trackout = row.get("TrackOutTime", None)
    previous_batch_end = row.get("PreviousBatchEndTime", None)
    trackin = row.get("TrackInTime", None)
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
        # 有停产期：使用TrackInTime作为实际加工开始时间
        if pd.notna(trackin):
            start_time = trackin
            logging.debug(f"使用TrackInTime计算PT: {trackin}")
        else:
            # 如果TrackInTime为空，回退到PreviousBatchEndTime
            start_time = previous_batch_end
            logging.debug(f"TrackInTime为空，回退到PreviousBatchEndTime: {previous_batch_end}")
    else:
        # 正常连续生产：使用PreviousBatchEndTime
        if pd.notna(previous_batch_end):
            start_time = previous_batch_end
            logging.debug(f"正常生产，使用PreviousBatchEndTime: {previous_batch_end}")
        elif pd.notna(trackin):
            # 如果PreviousBatchEndTime为空，使用TrackInTime
            start_time = trackin
            logging.debug(f"PreviousBatchEndTime为空，使用TrackInTime: {trackin}")
        else:
            # 如果两者都为空，返回None
            return None
    
    trackout_dt = pd.to_datetime(trackout)
    start_dt = pd.to_datetime(start_time)
    
    # 确保结束时间大于开始时间
    if trackout_dt <= start_dt:
        return None
    
    # 计算实际时间差（秒）
    total_seconds = (trackout_dt - start_dt).total_seconds()
    
    # 转换为小时
    total_hours = total_seconds / 3600.0
    
    # 转换为天数（保留2位小数）
    return round(total_hours / 24.0, 2)


def calculate_st(row: pd.Series) -> Optional[float]:
    """
    计算ST(d) - 理论加工时间，不考虑周末
    
    计算逻辑（与SFC保持一致）：
    ST = (调试时间 + (合格数量 + 报废数量) × EH_machine或EH_labor / OEE + 0.5小时换批时间) / 24
    单位：天
    """
    # 使用TrackOutQuantity + ScrapQuantity（与SFC保持一致）
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




# 全局变量：日历表缓存
_calendar_df: Optional[pd.DataFrame] = None


def load_calendar_table(calendar_file: str) -> pd.DataFrame:
    """
    加载日历工作日表
    返回包含日期、是否工作日的DataFrame
    """
    global _calendar_df
    
    if _calendar_df is not None:
        return _calendar_df
    
    try:
        if not os.path.exists(calendar_file):
            logging.warning(f"日历表文件不存在: {calendar_file}，将使用默认周末逻辑")
            return pd.DataFrame()
        
        df = pd.read_csv(calendar_file, encoding='utf-8-sig')
        
        # 确保日期列为datetime类型
        if '日期' in df.columns:
            df['日期'] = pd.to_datetime(df['日期'])
        else:
            logging.error("日历表缺少'日期'列")
            return pd.DataFrame()
        
        # 确保有'是否工作日'列
        if '是否工作日' not in df.columns:
            logging.error("日历表缺少'是否工作日'列")
            return pd.DataFrame()
        
        # 将 '是否工作日' 列转换为布尔类型
        # CSV中可能是字符串 'True'/'False'，需要转换
        if df['是否工作日'].dtype == 'object':
            df['是否工作日'] = df['是否工作日'].map({'True': True, 'False': False, True: True, False: False})
        
        # 创建日期索引以便快速查找
        df = df.set_index('日期')
        
        _calendar_df = df
        logging.info(f"成功加载日历表: {calendar_file}, 共{len(df)}条记录")
        
        # 输出一些示例数据用于验证
        sample_dates = pd.to_datetime(['2024-10-01', '2024-10-07', '2024-10-08'])
        for date in sample_dates:
            if date in df.index:
                is_work = df.loc[date, '是否工作日']
                logging.debug(f"日历表示例: {date.date()} 是否工作日={is_work}")
        
        return df
        
    except Exception as e:
        logging.error(f"加载日历表失败: {calendar_file}, 错误: {e}")
        return pd.DataFrame()


def is_workday(date: datetime, calendar_df: pd.DataFrame) -> bool:
    """
    判断指定日期是否为工作日（基于日历表）
    如果日历表未加载或日期不在范围内，返回默认判断（周一到周五）
    """
    if calendar_df.empty:
        # 如果日历表未加载，使用默认逻辑（周一到周五为工作日）
        weekday = date.weekday()  # Monday=0, Sunday=6
        logging.debug(f"日历表未加载，使用默认逻辑判断 {date.date()}: 工作日={weekday < 5}")
        return weekday < 5
    
    # 获取日期部分（去除时间）
    date_only = date.date()
    date_key = pd.Timestamp(date_only)
    
    if date_key in calendar_df.index:
        is_work = calendar_df.loc[date_key, '是否工作日']
        # 确保返回布尔值
        is_work = bool(is_work)
        return is_work
    else:
        # 如果日期不在日历表中，使用默认逻辑
        weekday = date.weekday()
        logging.debug(f"日期 {date.date()} 不在日历表中，使用默认逻辑: 工作日={weekday < 5}")
        return weekday < 5


def get_next_workday_8am(date: datetime, calendar_df: pd.DataFrame) -> datetime:
    """
    获取下一个工作日的8:00（基于日历表）
    """
    current_date = date.date()
    current_datetime_8am = date.replace(hour=8, minute=0, second=0, microsecond=0)
    
    # 如果当前时间已经超过当天8:00，从下一天开始查找
    if date.hour >= 8:
        current_date = current_date + timedelta(days=1)
        current_datetime_8am = current_datetime_8am + timedelta(days=1)
    
    # 最多查找30天（防止无限循环）
    for _ in range(30):
        test_datetime = datetime.combine(current_date, datetime.min.time()).replace(hour=8)
        if is_workday(test_datetime, calendar_df):
            return test_datetime
        current_date = current_date + timedelta(days=1)
    
    # 如果30天内找不到工作日，返回30天后的8:00
    logging.warning(f"在30天内未找到工作日，返回默认值: {current_datetime_8am}")
    return current_datetime_8am


def calculate_due_time(row: pd.Series, calendar_df: pd.DataFrame, daily_working_hours: float = 8.0) -> Optional[datetime]:
    """
    计算DueTime - 理论完成时间（用于参考，不作为状态判断）
    
    逻辑：从 PreviousBatchEndTime 开始到理论完成的时间
    - 如果 PreviousBatchEndTime 为空，使用 EnterStepTime
    - 按工作日逐天累加工作时间，跳过非工作日
    - 工作日按24小时连续生产
    """
    # 使用 PreviousBatchEndTime 作为开始时间
    start_time = row.get("PreviousBatchEndTime", None)
    
    # 如果 PreviousBatchEndTime 为空，使用 EnterStepTime
    if pd.isna(start_time):
        start_time = row.get("EnterStepTime", None)
    if pd.isna(start_time):
        return None
    
    start_dt = pd.to_datetime(start_time)
    
    setup_time = 0
    if row.get("Setup") == "Yes" and pd.notna(row.get("Setup Time (h)")):
        setup_time = row.get("Setup Time (h)", 0) or 0
    
    # 获取单件时间（秒）
    # 优先使用Machine，如果Machine为0或空，则使用Labor
    machine_time_s = row.get("EH_machine(s)", None)
    labor_time_s = row.get("EH_labor(s)", None)
    
    # 确定使用哪个时间
    if pd.notna(machine_time_s) and machine_time_s > 0:
        unit_time_s = machine_time_s
    elif pd.notna(labor_time_s) and labor_time_s > 0:
        unit_time_s = labor_time_s
    else:
        return None
    
    qty = row.get("StepInQuantity", 0) or 0
    oee = row.get("OEE", 0.77) or 0.77
    
    # 计算总时间（秒）= 数量 × 单件时间（秒）/ OEE
    total_time_s = qty * unit_time_s / oee
    
    # 转换为小时
    total_time_h = total_time_s / 3600
    
    # 总工时 = 调试时间 + 加工时间 + 0.5小时换批时间
    total_hours = setup_time + total_time_h + 0.5
    
    # 使用新算法：按工作日逐天累加工作时间
    due_final = calculate_due_time_by_workdays(start_dt, total_hours, calendar_df, daily_working_hours)
    
    return due_final


def calculate_due_time_by_workdays(start: datetime, required_hours: float, calendar_df: pd.DataFrame, daily_working_hours: float = 8.0) -> datetime:
    """
    更直观的算法：按工作日逐天累加工作时间，跳过非工作日
    直到累加的工作时间达到理论工时，那一天就是完成时间
    
    Args:
        start: 开始时间
        required_hours: 需要的理论工时（小时）
        calendar_df: 日历表
        daily_working_hours: 每日工作时间（小时），默认8小时
    
    Returns:
        理论完成时间（工作日的8:00）
    """
    if required_hours <= 0:
        return start
    
    # 从开始时间开始
    current_time = start
    remaining_hours = required_hours
    
    # 如果开始时间在当天8:00之后，先计算当天剩余的工作时间
    start_date_8am = current_time.replace(hour=8, minute=0, second=0, microsecond=0)
    
    if current_time >= start_date_8am:
        # 检查当天是否为工作日
        if is_workday(start_date_8am, calendar_df):
            # 当天是工作日，计算当天剩余时间（工作日按24小时连续生产）
            # 计算到第二天8:00的剩余小时数
            next_day_8am = start_date_8am + timedelta(days=1)
            hours_until_next_day = (next_day_8am - current_time).total_seconds() / 3600
            
            if hours_until_next_day > 0:
                if remaining_hours <= hours_until_next_day:
                    # 当天就能完成
                    return current_time + timedelta(hours=remaining_hours)
                else:
                    # 当天完成不了，减去当天剩余时间
                    remaining_hours -= hours_until_next_day
                    # 移动到下一天的8:00
                    current_time = next_day_8am
            else:
                # 移动到下一天的8:00
                current_time = next_day_8am
        else:
            # 当天不是工作日，移动到下一天
            current_time = start_date_8am + timedelta(days=1)
    else:
        # 开始时间在当天8:00之前，从当天8:00开始
        current_time = start_date_8am
    
    # 按天遍历，累加工作日的工作时间
    max_days = 365  # 防止无限循环
    days_checked = 0
    
    while remaining_hours > 0 and days_checked < max_days:
        current_date_8am = current_time.replace(hour=8, minute=0, second=0, microsecond=0)
        
        if is_workday(current_date_8am, calendar_df):
            # 工作日，累加工作时间（24小时连续生产）
            if remaining_hours <= daily_working_hours:
                # 今天就能完成（从当天8:00开始计算）
                return current_date_8am + timedelta(hours=remaining_hours)
            else:
                # 今天完成不了，减去今天的工作时间，继续下一天
                remaining_hours -= daily_working_hours
        
        # 移动到下一天的8:00
        current_time = current_date_8am + timedelta(days=1)
        days_checked += 1
    
    # 如果超过最大天数，返回最后计算的时间
    if days_checked >= max_days:
        logging.warning(f"计算完成时间超过{max_days}天，返回估算值")
    
    return current_time


def calculate_nonworkday_hours(start: datetime, end: datetime, calendar_df: pd.DataFrame) -> float:
    """
    计算从start到end之间的非工作日小时数（基于日历表）
    用于NonWorkday(d)字段的计算
    """
    if end <= start:
        return 0.0
    
    total_hours = 0.0
    
    # 按天遍历时间区间，统计非工作日小时数
    current_date = start.date()
    end_date = end.date()
    current_time = start
    
    while current_date <= end_date:
        # 获取当天的开始时间（8:00）和结束时间（次日8:00）
        day_start_8am = datetime.combine(current_date, datetime.min.time()).replace(hour=8)
        day_end_8am = day_start_8am + timedelta(days=1)
        
        # 判断当天是否为工作日
        if not is_workday(day_start_8am, calendar_df):
            # 非工作日，计算与[current_time, end]重叠的部分
            overlap_start = max(current_time, day_start_8am)
            overlap_end = min(end, day_end_8am)
            
            if overlap_start < overlap_end:
                hours = (overlap_end - overlap_start).total_seconds() / 3600
                total_hours += hours
        
        # 移动到下一天
        current_date = current_date + timedelta(days=1)
        # 更新current_time为下一天的开始时间（8:00），但不要超过end
        if current_date <= end_date:
            current_time = max(current_time, day_end_8am)
        else:
            break
    
    return round(total_hours, 2)


def adjust_weekend(start: datetime, due: datetime, calendar_df: pd.DataFrame) -> datetime:
    """
    调整非工作日，顺延到工作日（基于日历表）
    如果截止时间落在非工作日，顺延到下一个工作日的8:00
    （保留此函数用于兼容，但推荐使用calculate_due_time_by_workdays）
    """
    if due <= start:
        return due
    
    # 检查due是否落在非工作日
    due_date_8am = due.replace(hour=8, minute=0, second=0, microsecond=0)
    
    # 如果due在当天8:00之后，检查当天是否为工作日
    if due.hour >= 8:
        if not is_workday(due_date_8am, calendar_df):
            # 当天是非工作日，顺延到下一个工作日8:00
            return get_next_workday_8am(due, calendar_df)
    else:
        # due在当天8:00之前，检查前一天是否为工作日
        prev_day = due_date_8am - timedelta(days=1)
        if not is_workday(prev_day, calendar_df):
            # 前一天是非工作日，顺延到下一个工作日8:00
            return get_next_workday_8am(due, calendar_df)
    
    # 如果due落在工作日，计算从start到due之间的非工作日小时数并加上
    non_workday_hours = calculate_nonworkday_hours(start, due, calendar_df)
    return due + timedelta(hours=non_workday_hours)


def calculate_nonworkday_days(row: pd.Series, calendar_df: pd.DataFrame) -> Optional[float]:
    """
    计算NonWorkday(d) - 非工作日天数（单位：天）
    
    逻辑：计算LT的周期范围内的非工作日天数
    - 开始时间：和LT计算一致（0010工序用Checkin_SFC，其他工序用EnterStepTime）
    - 结束时间：TrackOutTime
    - 统计非工作日小时数后转换为天数
    """
    # 获取开始时间：和LT计算逻辑一致
    operation = row.get("Operation", "")
    trackout = row.get("TrackOutTime", None)
    checkin_sfc = row.get("Checkin_SFC", None)
    enter_step = row.get("EnterStepTime", None)
    trackin = row.get("TrackInTime", None)
    
    if pd.isna(trackout):
        return None
    
    # 确定开始时间（和calculate_lt逻辑一致）
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
    
    if pd.isna(start_time):
        return None
    
    start_dt = pd.to_datetime(start_time)
    end_dt = pd.to_datetime(trackout)
    
    if end_dt <= start_dt:
        return None
    
    # 计算从开始时间到结束时间之间的非工作日小时数（基于日历表）
    non_workday_hours = calculate_nonworkday_hours(start_dt, end_dt, calendar_df)
    
    # 转换为天数（保留2位小数）
    return round(non_workday_hours / 24, 2)


def calculate_completion_status(row: pd.Series, calendar_df: pd.DataFrame) -> Optional[str]:
    """
    计算CompletionStatus - 完成状态
    
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
    nonworkday_d = row.get("NonWorkday(d)", 0.0)  # 非工作日天数
    
    if pd.isna(pt) or pd.isna(st):
        return None
    
    # PT转换为小时
    pt_hours = pt * 24
    # ST转换为小时
    st_hours = st * 24
    # 非工作日转换为小时
    nonworkday_hours = nonworkday_d * 24
    
    # 检查是否需要使用标准换型时间
    changeover_time = 0.5  # 默认换批时间
    if row.get("Setup") == "Yes" and pd.notna(row.get("Setup Time (h)")):
        changeover_time = row.get("Setup Time (h)", 0.5) or 0.5  # 使用标准换型时间
    
    # 阈值 = ST + 容差 + 换批/换型时间 + 非工作日时间
    threshold = st_hours + tolerance_h + changeover_time + nonworkday_hours
    
    # 比较：PT（小时） > 阈值 → Overdue
    if pt_hours > threshold:
        return "Overdue"
    else:
        return "OnTime"


def calculate_tolerance_hours(row: pd.Series) -> Optional[float]:
    """计算容差小时数（固定8小时，与SFC逻辑一致）"""
    due = row.get("DueTime", None)
    actual = row.get("TrackOutTime", None)
    
    if pd.isna(due) or pd.isna(actual):
        return None
    
    # 固定容差为8小时
    return 8.0


def extract_machine_number(resource_code: Any) -> Optional[int]:
    """从machine字段提取数字"""
    if pd.isna(resource_code):
        return None
    
    code_str = str(resource_code)
    numbers = re.findall(r'\d+', code_str)
    if numbers:
        try:
            return int(numbers[0])
        except:
            return None
    return None


def remove_duplicates(df: pd.DataFrame, cfg: Dict[str, Any]) -> pd.DataFrame:
    """
    基于关键字去重，保留最新的数据
    配置项：
    - deduplicate.key_fields: 用于判断重复的关键字段列表
    - deduplicate.sort_field: 用于判断"最新"的排序字段
    - deduplicate.sort_ascending: 排序方向（False表示降序，保留最大值/最新值）
    """
    if df.empty:
        return df
    
    dedup_cfg = cfg.get("deduplicate", {})
    
    # 如果未配置去重，直接返回
    if not dedup_cfg or not dedup_cfg.get("enabled", False):
        return df
    
    key_fields = dedup_cfg.get("key_fields", [])
    sort_field = dedup_cfg.get("sort_field", "TrackOutTime")
    sort_ascending = dedup_cfg.get("sort_ascending", False)  # False表示降序，保留最新的
    
    # 检查关键字段是否存在
    missing_fields = [f for f in key_fields if f not in df.columns]
    if missing_fields:
        logging.warning(f"去重关键字段不存在，跳过去重: {missing_fields}")
        return df
    
    if sort_field not in df.columns:
        logging.warning(f"排序字段不存在，跳过去重: {sort_field}")
        return df
    
    # 记录去重前的行数
    rows_before = len(df)
    
    # 先按排序字段排序（确保"最新"的定义正确）
    # 如果有文件修改时间或创建时间字段，也可以使用
    df_sorted = df.sort_values(by=sort_field, ascending=sort_ascending, na_position='last')
    
    # 基于关键字段去重，保留第一条（即最新的）
    df_deduped = df_sorted.drop_duplicates(subset=key_fields, keep='first').reset_index(drop=True)
    
    # 记录去重后的行数
    rows_after = len(df_deduped)
    rows_removed = rows_before - rows_after
    
    if rows_removed > 0:
        logging.info(f"去重完成：移除 {rows_removed} 条重复记录（基于字段: {key_fields}，排序字段: {sort_field}）")
        logging.info(f"去重前: {rows_before} 行，去重后: {rows_after} 行")
    
    return df_deduped


def process_all_data(cfg: Dict[str, Any], force_full_refresh: bool = False) -> pd.DataFrame:
    """处理所有数据的主函数"""
    # 检查测试模式配置
    test_cfg = cfg.get("test", {})
    test_enabled = test_cfg.get("enabled", False)
    max_rows = test_cfg.get("max_rows", None) if test_enabled else None
    
    if test_enabled:
        logging.info(f"测试模式已启用，将仅读取前 {max_rows} 行数据")
    
    # 1. 读取MES数据（支持多工厂配置）
    source_cfg = cfg.get("source", {})
    mes_sources = source_cfg.get("mes_sources", [])
    mes_path = source_cfg.get("mes_path", "")
    
    # 检查配置：优先使用新的多工厂配置，向后兼容单文件配置
    if mes_sources:
        logging.info(f"使用多工厂数据源配置，共 {len(mes_sources)} 个工厂")
        # 多工厂模式
        logging.info("开始读取多工厂数据...")
        mes_df = read_multi_factory_mes_data(cfg)
        
        if mes_df.empty:
            logging.error("未读取到任何MES数据，ETL终止")
            return pd.DataFrame()
        
        # 验证多工厂数据完整性
        if not validate_multi_factory_data(mes_df):
            logging.error("多工厂数据验证失败，ETL终止")
            return pd.DataFrame()
        
        # 输出多工厂数据摘要
        factory_summary = get_factory_summary(mes_df)
        logging.info(f"多工厂数据摘要: 总计 {factory_summary['total_records']} 条记录，包含 {factory_summary['factory_count']} 个工厂")
        for factory_id, details in factory_summary['factory_details'].items():
            logging.info(f"  {factory_id} ({details['factory_name']}): {details['record_count']} 条记录")
        
        # 注意：去重将在字段映射后执行，以确保使用正确的字段名
        
    elif mes_path:
        logging.info(f"使用单文件数据源配置: {mes_path}")
        # 向后兼容：原有的单文件处理逻辑
        if "*" in mes_path or "?" in mes_path:
            mes_files = glob.glob(mes_path)
            if not mes_files:
                logging.error(f"未找到匹配的MES文件: {mes_path}")
                return pd.DataFrame()
            logging.info(f"找到 {len(mes_files)} 个MES文件")
            # 读取所有文件并合并
            mes_dfs = []
            for file_path in mes_files:
                try:
                    logging.info(f"读取MES文件: {file_path}")
                    df = read_sharepoint_excel(file_path, max_rows=max_rows)
                    mes_dfs.append(df)
                except Exception as e:
                    logging.warning(f"读取文件失败 {file_path}: {e}")
            if not mes_dfs:
                logging.error("所有MES文件读取失败")
                return pd.DataFrame()
            mes_df = pd.concat(mes_dfs, ignore_index=True)
            logging.info(f"合并后MES数据行数: {len(mes_df)}")
            
            # 在合并后立即去重（基于原始数据）
            mes_df = remove_duplicates(mes_df, cfg)
        else:
            if not os.path.exists(mes_path):
                logging.error(f"MES数据路径不存在: {mes_path}")
                return pd.DataFrame()
            logging.info(f"读取MES数据: {mes_path}")
            mes_df = read_sharepoint_excel(mes_path, max_rows=max_rows)
    else:
        logging.error("未配置MES数据源（需要 mes_sources 或 mes_path）")
        return pd.DataFrame()
    
    # 先做基础处理（字段映射和类型转换），以便增量过滤能识别标准字段名
    mes_df = process_mes_data(mes_df, cfg)
    
    # 执行去重（在字段映射之后，确保使用正确的字段名）
    mes_df = remove_duplicates(mes_df, cfg)
    
    # 增量处理：在字段映射之后进行增量过滤
    incr_cfg = cfg.get("incremental", {})
    state_file = incr_cfg.get("state_file", "publish/etl_mes_state.json")
    state_file = os.path.join(BASE_DIR, state_file) if not os.path.isabs(state_file) else state_file
    
    # 如果强制全量刷新，删除状态文件和历史文件
    if force_full_refresh:
        logging.info("执行全量刷新：清除状态文件和历史数据文件")
        if os.path.exists(state_file):
            os.remove(state_file)
            logging.info(f"已删除状态文件: {state_file}")
        
        history_file = incr_cfg.get("history_file", "publish/MES_batch_report_latest.parquet")
        history_file = os.path.join(BASE_DIR, history_file) if not os.path.isabs(history_file) else history_file
        if os.path.exists(history_file):
            os.remove(history_file)
            logging.info(f"已删除历史数据文件: {history_file}")
    
    if incr_cfg.get("enabled", False) and not force_full_refresh:
        if should_do_full_refresh(cfg, state_file):
            logging.info("执行全量刷新：清除状态文件")
            if os.path.exists(state_file):
                os.remove(state_file)
        else:
            logging.info("执行增量处理：过滤新数据")
            mes_df = filter_incremental_data(mes_df, cfg, state_file)
            if mes_df.empty:
                logging.info("MES数据没有新数据，跳过后续处理")
                return pd.DataFrame()
    
    # 3. 处理MES数据
    result = mes_df.copy()
    
    # 合并SFC数据获取Checkin_SFC（在合并标准时间之前）
    result = merge_sfc_data(result, cfg)
    
    # 合并标准时间数据（从合并后的parquet文件读取）
    result = merge_standard_time(result, cfg)
    
    # 计算指标
    result = calculate_metrics(result, cfg)
    
    # 4. 增量处理：合并历史数据（如果启用）
    if incr_cfg.get("enabled", False):
        history_file = incr_cfg.get("history_file", "publish/MES_batch_report_latest.parquet")
        history_file = os.path.join(BASE_DIR, history_file) if not os.path.isabs(history_file) else history_file
        result = merge_with_history(result, history_file, cfg)
    
    # 5. 在最终保存前，对所有合并后的数据统一重新计算 PreviousBatchEndTime
    # 确保基于完整、排序后的数据集进行准确计算
    if not result.empty:
        logging.info("对所有合并后的数据统一计算 PreviousBatchEndTime")
        result = calculate_previous_batch_end_time(result)
    
    # 返回MES结果
    return result


def generate_record_hash(row: pd.Series, key_fields: List[str]) -> str:
    """生成记录的唯一hash值"""
    # 提取关键字段的值
    values = []
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
    
    # 生成hash
    key_str = '|'.join(values)
    return hashlib.md5(key_str.encode('utf-8')).hexdigest()


def load_etl_state(state_file: str) -> Dict[str, Any]:
    """加载ETL状态文件"""
    if os.path.exists(state_file):
        try:
            with open(state_file, 'r', encoding='utf-8') as f:
                state = json.load(f)
                # 确保processed_hashes是set类型
                if "processed_hashes" in state and isinstance(state["processed_hashes"], list):
                    state["processed_hashes"] = set(state["processed_hashes"])
                return state
        except Exception as e:
            logging.warning(f"加载状态文件失败: {e}，将使用默认状态")
    return {
        "last_processed_time": None,
        "processed_hashes": set(),
        "last_full_refresh_time": None
    }


def save_etl_state(state_file: str, state: Dict[str, Any]) -> None:
    """保存ETL状态文件"""
    os.makedirs(os.path.dirname(state_file), exist_ok=True)
    
    # 转换set为list以便JSON序列化
    state_copy = state.copy()
    if "processed_hashes" in state_copy and isinstance(state_copy["processed_hashes"], set):
        state_copy["processed_hashes"] = list(state_copy["processed_hashes"])
    
    try:
        with open(state_file, 'w', encoding='utf-8') as f:
            json.dump(state_copy, f, indent=2, ensure_ascii=False, default=str)
    except Exception as e:
        logging.error(f"保存状态文件失败: {e}")


def filter_incremental_data(df: pd.DataFrame, cfg: Dict[str, Any], state_file: str) -> pd.DataFrame:
    """
    增量过滤：只返回新数据（未处理过的记录）
    
    策略：
    1. 使用复合唯一键（BatchNumber + Operation + machine + TrackOutTime）生成hash
    2. 检查hash是否已处理过
    3. 可选：使用时间窗口筛选
    """
    if df.empty:
        return df
    
    incr_cfg = cfg.get("incremental", {})
    if not incr_cfg.get("enabled", False):
        logging.info("增量处理未启用，返回全部数据")
        return df
    
    # 加载状态
    state = load_etl_state(state_file)
    processed_hashes = set(state.get("processed_hashes", []))
    last_processed_time = state.get("last_processed_time")
    
    # 获取唯一键字段
    key_fields = incr_cfg.get("unique_key_fields", ["BatchNumber", "Operation", "machine", "TrackOutTime"])
    
    # 检查必需字段是否存在
    missing_fields = [f for f in key_fields if f not in df.columns]
    if missing_fields:
        logging.warning(f"增量处理所需字段不存在: {missing_fields}，返回全部数据")
        return df
    
    # 时间窗口筛选（可选）
    time_window_days = incr_cfg.get("time_window_days")
    if time_window_days and time_window_days > 0:
        cutoff_date = datetime.now() - timedelta(days=time_window_days)
        if "TrackOutTime" in df.columns:
            df_time_filtered = df[df["TrackOutTime"] >= cutoff_date].copy()
            logging.info(f"时间窗口筛选：保留最近{time_window_days}天的数据，从{len(df)}行筛选到{len(df_time_filtered)}行")
            df = df_time_filtered
    
    # 生成每条记录的hash并筛选新数据
    df['_record_hash'] = df.apply(lambda row: generate_record_hash(row, key_fields), axis=1)
    
    # 筛选新数据（hash不在已处理集合中）
    new_data = df[~df['_record_hash'].isin(processed_hashes)].copy()
    new_data = new_data.drop(columns=['_record_hash'])
    
    new_count = len(new_data)
    total_count = len(df)
    existing_count = total_count - new_count
    
    logging.info(f"增量过滤：总数据 {total_count} 行，已处理 {existing_count} 行，新数据 {new_count} 行")
    
    return new_data


def merge_with_history(new_df: pd.DataFrame, history_file: str, cfg: Dict[str, Any]) -> pd.DataFrame:
    """合并新数据和历史数据"""
    if new_df.empty:
        logging.warning("新数据为空，尝试加载历史数据")
        if os.path.exists(history_file):
            try:
                return pd.read_parquet(history_file)
            except Exception as e:
                logging.warning(f"加载历史数据失败: {e}")
        return new_df
    
    if not os.path.exists(history_file):
        logging.info("历史数据文件不存在，直接使用新数据")
        return new_df
    
    try:
        history_df = pd.read_parquet(history_file)
        logging.info(f"加载历史数据: {len(history_df)} 行")
        
        # 获取唯一键字段用于去重
        incr_cfg = cfg.get("incremental", {})
        key_fields = incr_cfg.get("unique_key_fields", ["BatchNumber", "Operation", "machine", "TrackOutTime"])
        
        # 确保字段存在
        available_key_fields = [f for f in key_fields if f in new_df.columns and f in history_df.columns]
        
        if available_key_fields:
            # 合并数据
            combined_df = pd.concat([history_df, new_df], ignore_index=True)
            
            # 去重：基于唯一键，保留最新的（如果有TrackOutTime）
            if "TrackOutTime" in combined_df.columns:
                combined_df = combined_df.sort_values("TrackOutTime", na_position='last')
            
            combined_df = combined_df.drop_duplicates(subset=available_key_fields, keep='last').reset_index(drop=True)
            
            logging.info(f"数据合并完成：历史 {len(history_df)} 行 + 新增 {len(new_df)} 行 = 合并后 {len(combined_df)} 行")
            return combined_df
        else:
            logging.warning("无法找到有效的唯一键字段进行合并，直接拼接数据")
            return pd.concat([history_df, new_df], ignore_index=True)
            
    except Exception as e:
        logging.error(f"合并历史数据失败: {e}，返回新数据")

def should_do_full_refresh(cfg: Dict[str, Any], state_file: str) -> bool:
    """判断是否应该执行全量刷新"""
    incr_cfg = cfg.get("incremental", {})
    if not incr_cfg.get("enabled", False):
        return False
    
    threshold_days = incr_cfg.get("full_refresh_threshold_days")
    if not threshold_days or threshold_days <= 0:
        return False
    
    state = load_etl_state(state_file)
    last_processed_time = state.get("last_processed_time")
    
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




if __name__ == "__main__":
    cfg = load_config(CONFIG_PATH)
    setup_logging(cfg)
    
    # 提示用户选择刷新模式
    is_incremental = prompt_refresh_mode(default_incremental=True)
    force_full_refresh = not is_incremental
    
    if force_full_refresh:
        logging.info("="*60)
        logging.info("用户选择: 全量刷新")
        logging.info("="*60)
    else:
        logging.info("="*60)
        logging.info("用户选择: 增量刷新")
        logging.info("="*60)
    
    t0 = time.time()
    try:
        # 处理MES数据
        mes_result_df = process_all_data(cfg, force_full_refresh=force_full_refresh)
        
        if mes_result_df.empty:
            logging.warning("MES处理后的数据为空，跳过保存")
        else:
            # 保存结果
            output_dir = cfg.get("output", {}).get("base_dir", "publish")
            output_dir = os.path.join(BASE_DIR, output_dir) if not os.path.isabs(output_dir) else output_dir
            os.makedirs(output_dir, exist_ok=True)
            
            # 增量处理：保存到latest文件
            incr_cfg = cfg.get("incremental", {})
            if incr_cfg.get("enabled", False):
                # 保存到latest文件（用于增量合并）
                history_file = incr_cfg.get("history_file", "publish/MES_batch_report_latest.parquet")
                history_file = os.path.join(BASE_DIR, history_file) if not os.path.isabs(history_file) else history_file
                save_to_parquet(mes_result_df, history_file, cfg)
                
                # 更新状态文件
                state_file = incr_cfg.get("state_file", "publish/etl_mes_state.json")
                state_file = os.path.join(BASE_DIR, state_file) if not os.path.isabs(state_file) else state_file
                update_etl_state(mes_result_df, state_file, cfg)
                
                logging.info("增量处理：已保存到latest文件并更新状态")
            else:
                # 如果未启用增量处理，也保存到latest文件
                latest_file = os.path.join(output_dir, "MES_batch_report_latest.parquet")
                save_to_parquet(mes_result_df, latest_file, cfg)
                logging.info(f"MES数据已保存: {latest_file}")
        
            logging.info(f"处理完成，耗时: {time.time() - t0:.2f}秒")
            
    except Exception as e:
        logging.exception(f"ETL失败: {e}")
        sys.exit(1)
