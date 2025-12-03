"""
SFC班组合格率数据清洗ETL脚本
处理每日班组合格率数据，生成质量分析报告
"""

import os
import sys
import pandas as pd
import numpy as np
import glob
import json
from datetime import datetime, timedelta
import logging
import argparse
from pathlib import Path
import re

# 添加ETL工具函数
sys.path.append(os.path.dirname(__file__))
from etl_utils import load_config, setup_logging, save_to_parquet, read_sharepoint_excel

def extract_report_date_from_filename(filename: str) -> datetime:
    """
    从文件名提取报表日期
    IGPR-20251202080001 -> 2025-12-02 08:00:01
    """
    try:
        # 使用正则表达式匹配 IGPR-YYYYMMDDHHMMSS 格式
        pattern = r'IGPR-(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})'
        match = re.search(pattern, filename)
        
        if match:
            year, month, day, hour, minute, second = match.groups()
            return datetime(int(year), int(month), int(day), int(hour), int(minute), int(second))
        else:
            # 如果无法解析，返回文件修改时间作为备选
            logging.warning(f"无法从文件名提取日期: {filename}，使用文件修改时间")
            return datetime.now()  # 或者返回 None
            
    except Exception as e:
        logging.warning(f"解析文件名日期失败 {filename}: {e}")
        return datetime.now()  # 或者返回 None

def read_team_passrate_data(config: dict) -> pd.DataFrame:
    """
    读取班组合格率数据
    支持多文件读取和合并
    """
    source_cfg = config.get("source", {})
    data_path = source_cfg.get("team_passrate_path", "")
    
    if not data_path:
        raise ValueError("未配置班组合格率数据路径")
    
    logging.info(f"读取班组合格率数据: {data_path}")
    
    # 处理通配符路径
    if "*" in data_path or "?" in data_path:
        data_files = glob.glob(data_path)
        if not data_files:
            raise FileNotFoundError(f"未找到匹配的数据文件: {data_path}")
        
        # 按文件修改时间排序（最新文件优先）
        data_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
        logging.info(f"找到 {len(data_files)} 个班组合格率数据文件，按修改时间排序")
        
        # 测试模式：限制文件数量
        test_cfg = config.get("test", {})
        test_enabled = test_cfg.get("enabled", False)
        max_files = test_cfg.get("max_files", 20) if test_enabled else len(data_files)
        
        if test_enabled and len(data_files) > max_files:
            data_files = data_files[:max_files]
            logging.info(f"🧪 测试模式：仅读取前 {max_files} 个最新文件")
        
        # 读取所有文件并合并
        dfs = []
        for i, file_path in enumerate(data_files, start=1):
            try:
                mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                logging.info(f"读取文件 {i}/{len(data_files)}: {os.path.basename(file_path)} (修改时间: {mod_time})")
                df = read_sharepoint_excel(file_path)
                if not df.empty:
                    df['source_file'] = os.path.basename(file_path)
                    df['file_mod_time'] = datetime.fromtimestamp(os.path.getmtime(file_path))
                    
                    # 从文件名提取报表日期
                    filename = os.path.basename(file_path)
                    report_date = extract_report_date_from_filename(filename)
                    df['报表日期'] = report_date
                    
                    dfs.append(df)
            except Exception as e:
                logging.warning(f"读取文件失败 {i}/{len(data_files)}: {file_path}: {e}")
        
        if not dfs:
            raise ValueError("所有班组合格率数据文件读取失败")
        
        combined_df = pd.concat(dfs, ignore_index=True)
        logging.info(f"合并后班组合格率数据行数: {len(combined_df)}")
        return combined_df
    
    else:
        # 单文件处理
        if not os.path.exists(data_path):
            raise FileNotFoundError(f"班组合格率数据文件不存在: {data_path}")
        
        df = read_sharepoint_excel(data_path)
        if not df.empty:
            # 从文件名提取报表日期
            filename = os.path.basename(data_path)
            report_date = extract_report_date_from_filename(filename)
            df['报表日期'] = report_date
            
        return df

def validate_team_passrate_data(df: pd.DataFrame, config: dict) -> bool:
    """
    验证班组合格率数据完整性
    """
    logging.info("开始验证班组合格率数据完整性...")
    
    # 检查必要字段
    required_fields = config.get("processing", {}).get("required_fields", 
        ["批次号", "产品序号", "检验结果", "班组"])
    
    missing_fields = [field for field in required_fields if field not in df.columns]
    if missing_fields:
        logging.error(f"缺少必要字段: {missing_fields}")
        return False
    
    # 检查数据行数
    if df.empty:
        logging.error("班组合格率数据为空")
        return False
    
    # 检查关键字段缺失率
    for field in required_fields:
        missing_count = df[field].isnull().sum()
        missing_rate = missing_count / len(df) * 100
        if missing_rate > 5:  # 缺失率超过5%报警
            logging.warning(f"字段 {field} 缺失率较高: {missing_rate:.1f}% ({missing_count}/{len(df)})")
    
    logging.info(f"班组合格率数据验证通过: {len(df)} 行记录")
    return True

def clean_team_passrate_data(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    清洗班组合格率数据
    """
    logging.info("开始清洗班组合格率数据...")
    
    result = df.copy()
    original_count = len(result)
    
    # 1. 标准化字段名（去除空格，统一命名）
    result.columns = result.columns.str.strip()
    
    # 2. 标准化批次号：移除IPQCK前缀
    if '批次号' in result.columns:
        result['批次号'] = result['批次号'].astype(str).str.replace(r'^IPQCK', '', regex=True)
        logging.info("已标准化批次号：移除IPQCK前缀")
    
    # 2. 处理机床设备缺失值
    if '机床设备' in result.columns:
        missing_equipment = result['机床设备'].isnull()
        if missing_equipment.any():
            # 使用工序编号填充缺失的机床设备，显式转换为字符串避免dtype警告
            result['机床设备'] = result['机床设备'].astype('object')
            result.loc[missing_equipment, '机床设备'] = result.loc[missing_equipment, '工序编号'].astype(str)
            logging.info(f"已用工序编号填充 {missing_equipment.sum()} 个缺失的机床设备")
    
    # 3. 处理产品序号缺失值
    if '产品序号' in result.columns:
        missing_serial = result['产品序号'].isnull()
        if missing_serial.any():
            # 使用行号填充缺失的产品序号
            result.loc[missing_serial, '产品序号'] = result.loc[missing_serial].groupby('批次号').cumcount() + 1
            logging.info(f"已填充 {missing_serial.sum()} 个缺失的产品序号")
    
    # 4. 标准化检验结果
    if '检验结果' in result.columns:
        # 统一检验结果格式
        result['检验结果'] = result['检验结果'].str.strip().str.upper()
        valid_results = ['合格', '不合格', 'PASS', 'FAIL']
        invalid_results = ~result['检验结果'].isin(valid_results)
        if invalid_results.any():
            logging.warning(f"发现 {invalid_results.sum()} 个异常检验结果")
    
    # 5. 数据类型转换
    type_conversions = {
        '机床设备': 'str',
        '产品序号': 'int64',
        '合格数': 'int64',
        '不合格数': 'int64'
    }
    
    for col, dtype in type_conversions.items():
        if col in result.columns:
            try:
                result[col] = result[col].astype(dtype)
            except Exception as e:
                logging.warning(f"字段 {col} 类型转换失败: {e}")
    
    # 6. 业务键去重（保留最新数据）
    before_dedup = len(result)
    
    # 检查业务键是否存在
    business_keys = ['批次号', '产品序号', '工序编号', '工序名称']
    missing_keys = [key for key in business_keys if key not in result.columns]
    
    if missing_keys:
        logging.warning(f"缺少业务键字段 {missing_keys}，使用批次+工序+班组去重")
        # 如果缺少产品序号，使用批次+工序+班组作为备选业务键
        backup_keys = ['批次号', '工序编号', '工序名称', '班组']
        backup_missing = [key for key in backup_keys if key not in result.columns]
        if backup_missing:
            logging.warning(f"缺少备选业务键字段 {backup_missing}，使用全行去重")
            result = result.drop_duplicates()
        else:
            result = result.drop_duplicates(subset=backup_keys, keep='first')
            logging.info(f"备选业务键去重: 基于批次+工序+班组移除重复记录")
    else:
        # 使用完整的业务键去重，保留最新文件的记录（keep='first'因为已按文件时间排序）
        before_business_dedup = len(result)
        result = result.drop_duplicates(subset=business_keys, keep='first')
        
        business_dedup_count = before_business_dedup - len(result)
        logging.info(f"业务键去重: 基于批次号+产品序号+工序编号+工序名称移除 {business_dedup_count} 条重复记录")
        
        # 如果仍有完全重复的行，再进行一次全行去重
        final_before = len(result)
        result = result.drop_duplicates()
        final_dedup_count = final_before - len(result)
        
        if final_dedup_count > 0:
            logging.info(f"全行去重: 移除 {final_dedup_count} 条完全重复记录")
    
    removed_duplicates = before_dedup - len(result)
    logging.info(f"总去重统计: 移除 {removed_duplicates} 条重复记录")
    
    logging.info(f"班组合格率数据清洗完成: {original_count} → {len(result)} 行")
    return result

def aggregate_team_passrate_data_batch_level(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    按批次计算真实合格率：以批次为单位，使用最终工序合格数/（最终工序合格数+所有工序不合格数总和）
    这样能反映产品在整个生产过程中的真实质量
    """
    if df.empty:
        return df
    
    result = df.copy()
    logging.info("开始按批次计算真实合格率...")
    
    # 检查必要的字段
    required_fields = ['批次号', '产品号', '工序编号', '工序名称', '班组']
    missing_fields = [field for field in required_fields if field not in result.columns]
    
    if missing_fields:
        logging.error(f"缺少必要字段: {missing_fields}")
        return pd.DataFrame()
    
    # 处理两种检验结果格式
    if '检验结果' in result.columns and ('合格数' not in result.columns or '不合格数' not in result.columns):
        # 格式1: 个人产品级检验结果，需要聚合计算
        logging.info("检测到个人产品级检验结果，开始聚合计算...")
        
        # 标准化检验结果
        result['检验结果'] = result['检验结果'].str.strip().str.upper()
        result['是否合格'] = result['检验结果'].isin(['合格', 'PASS'])
        
        # 按批次+产品号+工序编号+班组聚合
        operation_agg = result.groupby(['批次号', '产品号', '工序编号', '工序名称', '班组']).agg(
            合格数=('是否合格', 'sum'),
            不合格数=('是否合格', lambda x: len(x) - x.sum()),
            产品序号数量=('产品序号', 'count'),
            file_mod_time=('file_mod_time', 'max'),
            source_file=('source_file', lambda x: ', '.join(set(x.dropna().astype(str).str.strip())))
        ).reset_index()
        
    elif '合格数' in result.columns and '不合格数' in result.columns:
        # 格式2: 已有批次级统计数据，直接聚合
        logging.info("检测到批次级统计数据，直接聚合...")
        
        # 确保数值类型
        result['合格数'] = pd.to_numeric(result['合格数'], errors='coerce').fillna(0)
        result['不合格数'] = pd.to_numeric(result['不合格数'], errors='coerce').fillna(0)
        
        # 按批次+产品号+工序编号+班组聚合，保留产品序号信息
        operation_agg = result.groupby(['批次号', '产品号', '工序编号', '工序名称', '班组']).agg(
            合格数=('合格数', 'sum'),
            不合格数=('不合格数', 'sum'),
            产品序号数量=('产品序号', 'count'),
            产品序号=('产品序号', 'first'),  # 保留第一个产品序号
            file_mod_time=('file_mod_time', 'max'),
            source_file=('source_file', lambda x: ', '.join(set(x.dropna().astype(str).str.strip())))
        ).reset_index()
        
    else:
        logging.error("无法识别检验结果格式")
        return pd.DataFrame()
    
    logging.info(f"工序级聚合完成: {len(result)} → {len(operation_agg)} 条记录")
    
    # 转换工序编号为数字以便比较最大值，处理无效值
    operation_agg['工序编号_数字'] = pd.to_numeric(operation_agg['工序编号'], errors='coerce')
    
    # 过滤掉工序编号为NaN的记录（这些可能是N/A等非数字值）
    valid_operations = operation_agg.dropna(subset=['工序编号_数字']).copy()
    
    if len(valid_operations) == 0:
        logging.error("没有找到有效的数字工序编号，无法计算批次级合格率")
        return pd.DataFrame()
    
    if len(valid_operations) < len(operation_agg):
        logging.warning(f"过滤掉 {len(operation_agg) - len(valid_operations)} 条无效工序编号记录")
    
    # 按批次分组，计算批次级合格率
    batch_results = []
    skipped_batches = 0
    
    for (batch_no, product_no), batch_group in valid_operations.groupby(['批次号', '产品号']):
        try:
            # 找到最终工序（工序编号最大的工序）
            max_idx = batch_group['工序编号_数字'].idxmax()
            final_operations = batch_group.loc[max_idx]
            
            # 计算批次总不合格数（所有工序的不合格数总和）
            total_unqualified = batch_group['不合格数'].sum()
            
            # 最终工序的合格数（单个班组的合格数）
            final_qualified = final_operations['合格数']
            
            # 计算批次真实合格率 = 最终工序合格数 / (最终工序合格数 + 所有工序不合格数总和)
            total_defects = final_qualified + total_unqualified
            batch_pass_rate = (final_qualified / total_defects * 100).round(2) if total_defects > 0 else 0
            
            # 转换为百分比格式（缩小100倍）
            batch_pass_rate_percentage = round(batch_pass_rate / 100, 4) if batch_pass_rate > 0 else 0
            
            # 为批次中的每个班组创建记录（只取最终工序的班组）
            final_op_records = batch_group[batch_group['工序编号_数字'] == final_operations['工序编号_数字']]
            for _, final_op in final_op_records.iterrows():
                batch_record = final_op.copy()
                batch_record['批次合格率'] = batch_pass_rate_percentage
                batch_record['批次总不合格数'] = total_unqualified
                batch_record['最终工序合格数'] = final_qualified
                batch_record['计算方式'] = '批次级真实合格率'
                batch_results.append(batch_record)
                
        except Exception as e:
            logging.warning(f"跳过批次 {batch_no}-{product_no}，计算失败: {e}")
            skipped_batches += 1
            continue
    
    if skipped_batches > 0:
        logging.warning(f"跳过了 {skipped_batches} 个批次，因为数据质量问题")
    
    # 合并所有批次结果
    batch_df = pd.DataFrame(batch_results)
    
    if batch_df.empty:
        logging.error("批次级计算结果为空")
        return pd.DataFrame()
    
    # 重命名合格率为工序合格率（保持原有字段）
    batch_df = batch_df.rename(columns={'合格率': '工序合格率'})
    
    # 移除不需要的列
    columns_to_remove = ['班组排名', '数据质量', '产品序号数量', '工序编号_数字']
    batch_df = batch_df.drop(columns=columns_to_remove, errors='ignore')
    
    logging.info(f"批次级合格率计算完成: {len(operation_agg)} → {len(batch_df)} 条记录")
    logging.info(f"平均批次合格率: {batch_df['批次合格率'].mean():.4f} (百分比格式)")
    
    return batch_df

def aggregate_team_passrate_data(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    按批次、产品号、工序编号、工序名称、班组分组聚合数据
    统一处理两种检验结果格式，计算合格数量和合格率
    """
    if df.empty:
        return df
    
    result = df.copy()
    logging.info("开始按业务维度分组聚合数据...")
    
    # 检查必要的分组字段
    group_fields = ['批次号', '产品号', '工序编号', '工序名称', '班组']
    missing_fields = [field for field in group_fields if field not in result.columns]
    
    if missing_fields:
        logging.error(f"缺少分组字段: {missing_fields}")
        return pd.DataFrame()
    
    # 处理两种检验结果格式
    if '检验结果' in result.columns and ('合格数' not in result.columns or '不合格数' not in result.columns):
        # 格式1: 个人产品级检验结果，需要聚合计算
        logging.info("检测到个人产品级检验结果，开始聚合计算...")
        
        # 标准化检验结果
        result['检验结果'] = result['检验结果'].str.strip().str.upper()
        result['是否合格'] = result['检验结果'].isin(['合格', 'PASS'])
        
        # 按分组字段聚合
        aggregated = result.groupby(group_fields).agg(
            合格数=('是否合格', 'sum'),
            不合格数=('是否合格', lambda x: len(x) - x.sum()),
            总检验数=('是否合格', 'count'),
            产品序号数量=('产品序号', 'count'),
            file_mod_time=('file_mod_time', 'max'),
            source_file=('source_file', lambda x: ', '.join(set(x.dropna().astype(str).str.strip())))
        ).reset_index()
        
    elif '合格数' in result.columns and '不合格数' in result.columns:
        # 格式2: 已有批次级统计数据，直接聚合
        logging.info("检测到批次级统计数据，直接聚合...")
        
        # 确保数值类型
        result['合格数'] = pd.to_numeric(result['合格数'], errors='coerce').fillna(0)
        result['不合格数'] = pd.to_numeric(result['不合格数'], errors='coerce').fillna(0)
        
        # 按分组字段聚合
        aggregated = result.groupby(group_fields).agg(
            合格数=('合格数', 'sum'),
            不合格数=('不合格数', 'sum'),
            产品序号数量=('产品序号', 'count'),
            file_mod_time=('file_mod_time', 'max'),
            source_file=('source_file', lambda x: ', '.join(set(x.dropna().astype(str).str.strip())))
        ).reset_index()
        
    else:
        logging.error("无法识别检验结果格式")
        return pd.DataFrame()
    
    # 计算合格率（百分比格式）
    aggregated['总检验数'] = aggregated['合格数'] + aggregated['不合格数']
    aggregated['合格率'] = (aggregated['合格数'] / aggregated['总检验数'] * 100).round(2)
    
    # 移除无效数据（总检验数为0的记录）
    valid_data = aggregated[aggregated['总检验数'] > 0].copy()
    
    # 添加数据质量标记
    valid_data['数据质量'] = '正常'
    
    # 计算班组排名（按合格率）
    valid_data = valid_data.sort_values('合格率', ascending=False)
    valid_data['班组排名'] = valid_data.groupby(['批次号', '产品号', '工序编号', '工序名称']).cumcount() + 1
    
    logging.info(f"聚合完成: {len(result)} → {len(valid_data)} 条记录")
    logging.info(f"平均合格率: {valid_data['合格率'].mean():.2f}%")
    
    return valid_data

def normalize_batch_and_operation_for_matching(df: pd.DataFrame) -> pd.DataFrame:
    """
    标准化批次号和工序编号以匹配SFC批次报工表
    """
    result = df.copy()
    
    # 标准化批次号：移除IPQCK前缀
    if '批次号' in result.columns:
        result['批次号_标准化'] = result['批次号'].astype(str).str.replace(r'^IPQCK', '', regex=True)
    
    # 标准化工序编号：数字补零到4位
    if '工序编号' in result.columns:
        result['工序编号_标准化'] = result['工序编号'].astype(str).str.strip()
        # 只对纯数字进行补零到4位
        mask = result['工序编号_标准化'].str.match(r'^\d+$')
        result.loc[mask, '工序编号_标准化'] = result.loc[mask, '工序编号_标准化'].str.zfill(4)
        logging.info("已标准化工序编号：数字补零到4位（与SFC批次报工表保持一致）")
    
    return result

def enrich_with_processing_time(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    从SFC批次报工表获取工序加工时间，增强班组合格率数据
    """
    if df.empty:
        return df
    
    logging.info("开始从SFC批次报工表获取工序加工时间...")
    
    # 读取SFC批次报工表
    output_dir = config.get("output", {}).get("base_dir", "publish")
    if not os.path.isabs(output_dir):
        output_dir = os.path.join(os.path.dirname(__file__), output_dir)
    
    sfc_batch_file = os.path.join(output_dir, "SFC_batch_report_latest.parquet")
    
    if not os.path.exists(sfc_batch_file):
        logging.warning(f"SFC批次报工表不存在: {sfc_batch_file}")
        return df
    
    try:
        # 读取SFC批次报工表
        sfc_batch_df = pd.read_parquet(sfc_batch_file)
        logging.info(f"成功读取SFC批次报工表: {len(sfc_batch_df)} 条记录")
        
        # 检查必要字段
        required_fields = ['BatchNumber', 'Operation', 'Operation description', 'TrackOutTime']
        missing_fields = [field for field in required_fields if field not in sfc_batch_df.columns]
        
        if missing_fields:
            logging.error(f"SFC批次报工表缺少字段: {missing_fields}")
            return df
        
        # 提取需要的字段并去重（一个批次+工序可能有多个记录，取最新的TrackOutTime）
        time_data = sfc_batch_df[required_fields].copy()
        
        # 按批次+工序分组，取最新的TrackOutTime
        time_data_sorted = time_data.sort_values('TrackOutTime', ascending=False)
        time_data_unique = time_data_sorted.drop_duplicates(
            subset=['BatchNumber', 'Operation'], 
            keep='first'
        )
        
        logging.info(f"SFC批次报工表去重后: {len(time_data_unique)} 条唯一记录")
        
        # 执行数据增强 - 左连接
        result = df.copy()
        
        # 标准化数据格式以提升匹配率
        result_normalized = normalize_batch_and_operation_for_matching(result)
        
        # 调试信息：检查标准化效果
        logging.info(f"数据标准化效果:")
        if '批次号' in result.columns:
            original_batches = result['批次号'].dropna().unique()[:5]
            normalized_batches = result_normalized['批次号_标准化'].dropna().unique()[:5]
            logging.info(f"  批次号标准化: {list(original_batches)} -> {list(normalized_batches)}")
        
        if '工序编号' in result.columns:
            original_ops = result['工序编号'].dropna().unique()[:5]
            normalized_ops = result_normalized['工序编号_标准化'].dropna().unique()[:5]
            logging.info(f"  工序编号标准化: {list(original_ops)} -> {list(normalized_ops)}")
        
        # 调试信息：检查输入数据状态
        logging.info(f"班组合格率聚合数据调试信息:")
        logging.info(f"  数据形状: {result_normalized.shape}")
        logging.info(f"  索引类型: {type(result_normalized.index)}")
        logging.info(f"  关键字段存在: 批次号_标准化={('批次号_标准化' in result_normalized.columns)}, 工序编号_标准化={('工序编号_标准化' in result_normalized.columns)}")
        logging.info(f"  关键字段样本: 批次号_标准化={result_normalized['批次号_标准化'].iloc[0] if len(result_normalized) > 0 else 'N/A'}, 工序编号_标准化={result_normalized['工序编号_标准化'].iloc[0] if len(result_normalized) > 0 else 'N/A'}")
        
        logging.info(f"SFC批次报工表调试信息:")
        logging.info(f"  数据形状: {time_data_unique.shape}")
        logging.info(f"  索引类型: {type(time_data_unique.index)}")
        logging.info(f"  关键字段样本: BatchNumber={time_data_unique['BatchNumber'].iloc[0] if len(time_data_unique) > 0 else 'N/A'}, Operation={time_data_unique['Operation'].iloc[0] if len(time_data_unique) > 0 else 'N/A'}")
        
        # 执行连接 - 使用标准化后的字段
        try:
            logging.info("开始执行合并操作（使用标准化字段）...")
            merged = result_normalized.merge(
                time_data_unique[['BatchNumber', 'Operation', 'TrackOutTime']],
                left_on=['批次号_标准化', '工序编号_标准化'],
                right_on=['BatchNumber', 'Operation'],
                how='left'
            )
            logging.info("合并操作成功完成")
        except Exception as merge_error:
            logging.error(f"合并操作失败: {merge_error}")
            logging.error(f"合并错误类型: {type(merge_error)}")
            # 尝试更简单的合并方式
            try:
                logging.info("尝试简化合并方式...")
                merged = result_normalized.merge(
                    time_data_unique[['BatchNumber', 'Operation', 'TrackOutTime']].reset_index(drop=True),
                    left_on=['批次号_标准化', '工序编号_标准化'],
                    right_on=['BatchNumber', 'Operation'],
                    how='left'
                )
                logging.info("简化合并成功")
            except Exception as simple_error:
                logging.error(f"简化合并也失败: {simple_error}")
                raise merge_error
        
        # 重命名TrackOutTime为工序加工时间
        merged = merged.rename(columns={'TrackOutTime': '工序加工时间'})
        
        # 移除重复的连接字段
        merged = merged.drop(columns=['BatchNumber', 'Operation'], errors='ignore')
        
        # 更新工序编号为标准化格式（保持与SFC批次报工表一致）
        if '工序编号_标准化' in merged.columns:
            merged['工序编号'] = merged['工序编号_标准化']
            merged = merged.drop(columns=['工序编号_标准化'], errors='ignore')
            logging.info("已更新工序编号为4位标准化格式")
        
        # 统计匹配结果
        matched_count = merged['工序加工时间'].notna().sum()
        total_count = len(merged)
        match_rate = matched_count / total_count * 100 if total_count > 0 else 0
        
        logging.info(f"数据增强完成: {matched_count}/{total_count} 条记录匹配到加工时间 ({match_rate:.1f}%)")
        
        # 如果匹配率较低，给出警告
        if match_rate < 80:
            logging.warning(f"加工时间匹配率较低 ({match_rate:.1f}%)，可能存在数据不一致")
        
        return merged
        
    except Exception as e:
        logging.error(f"数据增强失败: {e}")
        return df

def calculate_team_passrate_metrics(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    补充聚合数据中缺失的指标字段
    注意：大部分指标已在aggregate_team_passrate_data中计算
    """
    logging.info("开始补充聚合数据的指标字段...")
    
    result = df.copy()
    
    # 添加处理时间戳（聚合数据中缺失）
    result['处理时间'] = datetime.now()
    
    # 确保数据质量标识（如果聚合函数未设置）
    if '数据质量' not in result.columns:
        result['数据质量'] = '正常'
    
    # 标记异常数据（基于合格率范围）
    if '合格率' in result.columns:
        result.loc[result['合格率'] < 0, '数据质量'] = '异常'
        result.loc[result['合格率'] > 100, '数据质量'] = '异常'
    
    logging.info("聚合数据指标补充完成")
    return result

def process_team_passrate_data(config: dict, force_full_refresh: bool = False) -> pd.DataFrame:
    """
    处理班组合格率数据的主函数
    """
    logging.info("=" * 60)
    logging.info("开始处理SFC班组合格率数据")
    logging.info("=" * 60)
    
    try:
        # 1. 读取数据
        raw_df = read_team_passrate_data(config)
        
        # 2. 验证数据
        if not validate_team_passrate_data(raw_df, config):
            raise ValueError("班组合格率数据验证失败")
        
        # 3. 清洗数据
        cleaned_df = clean_team_passrate_data(raw_df, config)
        
        # 4. 按批次计算真实合格率（考虑累积缺陷）
        aggregated_df = aggregate_team_passrate_data_batch_level(cleaned_df, config)
        
        # 5. 数据增强：从SFC批次报工表获取工序加工时间
        enriched_df = enrich_with_processing_time(aggregated_df, config)
        
        # 6. 计算指标（聚合后的数据已包含合格率，此步骤可简化）
        result_df = calculate_team_passrate_metrics(enriched_df, config)
        
        # 7. 最终去重：处理批次级聚合后的重复数据
        logging.info("开始最终去重处理...")
        before_final_dedup = len(result_df)
        
        # 按完整业务键去重，保留最新文件时间戳的记录
        final_business_keys = ['批次号', '产品序号', '工序编号', '工序名称']
        missing_final_keys = [key for key in final_business_keys if key not in result_df.columns]
        
        if missing_final_keys:
            logging.warning(f"最终去重缺少业务键字段 {missing_final_keys}")
            # 使用可用字段去重
            available_keys = [key for key in final_business_keys if key in result_df.columns]
            if available_keys:
                result_df = result_df.drop_duplicates(subset=available_keys, keep='first')
                logging.info(f"最终去重: 基于 {available_keys} 移除重复记录")
        else:
            # 按文件修改时间排序，保留最新的记录
            result_df = result_df.sort_values('file_mod_time', ascending=False)
            result_df = result_df.drop_duplicates(subset=final_business_keys, keep='first')
            
        final_dedup_count = before_final_dedup - len(result_df)
        logging.info(f"最终去重完成: 移除 {final_dedup_count} 条重复记录")
        
        # 8. 数据质量检查
        logging.info(f"最终数据统计:")
        logging.info(f"  总记录数: {len(result_df)}")
        logging.info(f"  班组数: {result_df['班组'].nunique()}")
        logging.info(f"  批次数: {result_df['批次号'].nunique()}")
        
        if '产品合格率' in result_df.columns:
            avg_pass_rate = result_df['产品合格率'].mean()
            logging.info(f"  平均合格率: {avg_pass_rate:.2f}%")
        
        return result_df
        
    except Exception as e:
        logging.error(f"班组合格率数据处理失败: {e}")
        raise

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='SFC班组合格率数据清洗ETL脚本')
    parser.add_argument('--config', type=str, 
                       default="../03_配置文件/config/config_sfc_team_passrate.yaml",
                       help='配置文件路径')
    parser.add_argument('--mode', choices=['incremental', 'full'], 
                       default='full', help='刷新模式')
    parser.add_argument('--unattended', action='store_true', 
                       help='无人值守模式，不进行交互式提示')
    
    args = parser.parse_args()
    
    # 加载配置
    config_path = os.path.join(os.path.dirname(__file__), args.config)
    config = load_config(config_path)
    
    # 设置日志
    setup_logging(config, "sfc_team_passrate")
    
    # 处理数据
    try:
        result_df = process_team_passrate_data(config, force_full_refresh=(args.mode == 'full'))
        
        if result_df.empty:
            logging.warning("班组合格率处理后的数据为空，跳过保存")
        else:
            # 保存结果
            output_dir = config.get("output", {}).get("base_dir", "publish")
            output_dir = os.path.join(os.path.dirname(__file__), output_dir) if not os.path.isabs(output_dir) else output_dir
            os.makedirs(output_dir, exist_ok=True)
            
            # 保存到latest文件
            latest_file = os.path.join(output_dir, "SFC_Team_PassRate_latest.parquet")
            save_to_parquet(result_df, latest_file, config)
            logging.info(f"班组合格率数据已保存: {latest_file}")
            
            # 保存Excel验证文件（前1000行）
            excel_dir = os.path.join(output_dir, "excel")
            os.makedirs(excel_dir, exist_ok=True)
            excel_file = os.path.join(excel_dir, "SFC_Team_PassRate_latest.xlsx")
            result_df.head(1000).to_excel(excel_file, index=False)
            logging.info(f"班组合格率Excel验证文件已保存: {excel_file}")
        
        logging.info("SFC班组合格率数据处理完成")
        
    except Exception as e:
        logging.exception(f"ETL处理失败: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
