"""
SA指标数据清洗结果验证脚本
用于确认ETL清洗结果的正确性，包括关键计算结果的抽样检查
命名规范: etl_validate_{功能模块}_{数据类型}.py
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from typing import Dict, Any, List, Tuple

# 导入ETL工具函数
from etl_utils import load_config, setup_logging

# 加载配置文件
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "..", "03_配置文件", "config", "config_validate_sa.yaml")
cfg = load_config(CONFIG_PATH)


def load_latest_data(data_type: str) -> pd.DataFrame:
    """
    加载最新的处理后数据
    Args:
        data_type: 数据类型 (sfc, mes, sap_routing)
    Returns:
        处理后的DataFrame
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))
    publish_dir = cfg.get("data_paths", {}).get("publish_dir", r"C:\Users\huangk14\OneDrive - Medtronic PLC\CZ Production - 文档\General\POWER BI 数据源 V2\30-MES导出数据\publish")
    file_mapping = cfg.get("data_paths", {}).get("file_mapping", {
        'sfc': 'SFC_batch_report_latest.parquet',
        'mes': 'MES_batch_report_latest.parquet',
        'sap_routing': 'SAP_Routing_latest.parquet'
    })
    
    if data_type not in file_mapping:
        raise ValueError(f"不支持的数据类型: {data_type}")
    
    file_path = os.path.join(publish_dir, file_mapping[data_type])
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"数据文件不存在: {file_path}")
    
    return pd.read_parquet(file_path)


def validate_sfc_calculations(sfc_df: pd.DataFrame, sample_size: int = None) -> Dict[str, Any]:
    """
    验证SFC数据的关键计算结果
    Args:
        sfc_df: SFC处理后的数据
        sample_size: 抽样数量
    Returns:
        验证结果字典
    """
    results = {'status': 'success', 'details': [], 'errors': []}
    
    # 从配置文件获取参数
    if sample_size is None:
        sample_size = cfg.get("validation", {}).get("sample_size", 10)
    time_tolerance = cfg.get("validation", {}).get("time_tolerance", 0.1)
    sfc_config = cfg.get("sfc_validation", {})
    
    try:
        # 1. 验证时间计算 (LT, PT, ST)
        time_fields = sfc_config.get("time_fields", ['TrackOutTime', 'CheckInTime', 'EnterStepTime', 'LT', 'PT', 'ST'])
        if all(col in sfc_df.columns for col in time_fields[:3]):  # 只检查前3个时间字段
            valid_data = sfc_df.dropna(subset=time_fields[:3])
            if len(valid_data) > 0:
                sample_df = valid_data.sample(min(sample_size, len(valid_data)))
                
                for idx, row in sample_df.iterrows():
                    track_out = pd.to_datetime(row['TrackOutTime'])
                    check_in = pd.to_datetime(row['CheckInTime'])
                    enter_step = pd.to_datetime(row['EnterStepTime'])
                    
                    # 手动计算时间差
                    manual_lt = (track_out - check_in).total_seconds() / 3600  # 小时
                    manual_pt = (track_out - enter_step).total_seconds() / 3600  # 小时
                    manual_st = (check_in - enter_step).total_seconds() / 3600  # 小时
                    
                    # 获取系统计算的值
                    system_lt = row.get('LT', 0) if pd.notna(row.get('LT', 0)) else 0
                    system_pt = row.get('PT', 0) if pd.notna(row.get('PT', 0)) else 0
                    system_st = row.get('ST', 0) if pd.notna(row.get('ST', 0)) else 0
                    
                    # 对比验证（使用配置的误差容忍度）
                    lt_diff = abs(manual_lt - system_lt)
                    pt_diff = abs(manual_pt - system_pt)
                    st_diff = abs(manual_st - system_st)
                    
                    if lt_diff > time_tolerance or pt_diff > time_tolerance or st_diff > time_tolerance:
                        results['errors'].append({
                            'type': '时间计算差异',
                            'index': idx,
                            'BatchNumber': row.get('BatchNumber', ''),
                            'Operation': row.get('Operation', ''),
                            'LT差异': round(lt_diff, 3),
                            'PT差异': round(pt_diff, 3),
                            'ST差异': round(st_diff, 3)
                        })
                    else:
                        results['details'].append({
                            'type': '时间计算验证',
                            'index': idx,
                            'BatchNumber': row.get('BatchNumber', ''),
                            'Operation': row.get('Operation', ''),
                            'status': '通过'
                        })
        
        # 2. 验证标准时间合并
        std_time_fields = sfc_config.get("standard_time_fields", ['EH_machine(s)'])
        for field in std_time_fields:
            if field in sfc_df.columns:
                # 检查是否有合理的标准时间值
                valid_eh = sfc_df[sfc_df[field].notna() & (sfc_df[field] > 0)]
                if len(valid_eh) > 0:
                    eh_stats = {
                        '最小值': round(valid_eh[field].min(), 2),
                        '最大值': round(valid_eh[field].max(), 2),
                        '平均值': round(valid_eh[field].mean(), 2),
                        '有效记录数': len(valid_eh),
                        '匹配率': f"{len(valid_eh)/len(sfc_df)*100:.1f}%"
                    }
                    results['details'].append({
                        'type': '标准时间统计',
                        'data': eh_stats
                    })
                break
        
        # 3. 验证数据完整性
        total_records = len(sfc_df)
        required_fields = sfc_config.get("required_fields", ['BatchNumber', 'Operation', 'TrackOutTime', 'machine'])
        non_null_checks = {}
        
        for field in required_fields:
            if field in sfc_df.columns:
                non_null_checks[field] = sfc_df[field].notna().sum()
        
        completeness = {k: f"{v}/{total_records} ({v/total_records*100:.1f}%)" for k, v in non_null_checks.items()}
        results['details'].append({
            'type': '数据完整性',
            'data': completeness
        })
        
    except Exception as e:
        results['status'] = 'error'
        results['errors'].append({'type': '验证过程错误', 'message': str(e)})
    
    return results


def validate_mes_calculations(mes_df: pd.DataFrame, sample_size: int = 10) -> Dict[str, Any]:
    """
    验证MES数据的关键计算结果
    Args:
        mes_df: MES处理后的数据
        sample_size: 抽样数量
    Returns:
        验证结果字典
    """
    results = {'status': 'success', 'details': [], 'errors': []}
    
    try:
        # 1. 验证SFC数据合并
        if 'Checkin_SFC' in mes_df.columns:
            merged_count = mes_df['Checkin_SFC'].notna().sum()
            total_count = len(mes_df)
            merge_rate = merged_count / total_count * 100
            
            results['details'].append({
                'type': 'SFC数据合并统计',
                '总记录数': total_count,
                '成功合并数': merged_count,
                '合并率': f"{merge_rate:.1f}%"
            })
        
        # 2. 验证标准时间匹配
        std_time_cols = ['EH_machine(s)', 'EH_labor(s)', 'OEE']
        matched_stats = {}
        
        for col in std_time_cols:
            if col in mes_df.columns:
                matched = mes_df[col].notna().sum()
                matched_stats[col] = f"{matched}/{len(mes_df)} ({matched/len(mes_df)*100:.1f}%)"
        
        if matched_stats:
            results['details'].append({
                'type': '标准时间匹配统计',
                'data': matched_stats
            })
        
        # 3. 验证DueTime计算
        if all(col in mes_df.columns for col in ['TrackOutTime', 'DueTime']):
            valid_data = mes_df.dropna(subset=['TrackOutTime', 'DueTime'])
            if len(valid_data) > 0:
                sample_df = valid_data.sample(min(sample_size, len(valid_data)))
                
                for idx, row in sample_df.iterrows():
                    track_out = pd.to_datetime(row['TrackOutTime'])
                    due_time = pd.to_datetime(row['DueTime'])
                    
                    # DueTime应该大于TrackOutTime
                    if due_time <= track_out:
                        results['errors'].append({
                            'type': 'DueTime计算错误',
                            'index': idx,
                            'BatchNumber': row.get('BatchNumber', ''),
                            'TrackOutTime': str(track_out),
                            'DueTime': str(due_time)
                        })
        
        # 4. 验证PreviousBatchEndTime计算
        if 'PreviousBatchEndTime' in mes_df.columns:
            # 检查时间序列的合理性
            valid_time_data = mes_df.dropna(subset=['TrackOutTime', 'PreviousBatchEndTime'])
            if len(valid_time_data) > 0:
                # 按machine和TrackOutTime排序
                sorted_data = valid_time_data.sort_values(['machine', 'TrackOutTime'])
                
                # 抽样检查连续批次的时间关系
                sample_machines = sorted_data['machine'].unique()[:min(5, len(sorted_data['machine'].unique()))]
                
                for machine in sample_machines:
                    machine_data = sorted_data[sorted_data['machine'] == machine]
                    if len(machine_data) > 1:
                        # 检查前几个批次的时间关系
                        for i in range(1, min(4, len(machine_data))):
                            prev_end = pd.to_datetime(machine_data.iloc[i-1]['PreviousBatchEndTime'])
                            curr_start = pd.to_datetime(machine_data.iloc[i]['TrackOutTime'])
                            
                            # PreviousBatchEndTime应该小于当前批次的TrackOutTime
                            if prev_end >= curr_start:
                                results['errors'].append({
                                    'type': 'PreviousBatchEndTime计算错误',
                                    'machine': machine,
                                    '当前批次': str(machine_data.iloc[i]['BatchNumber']),
                                    'PreviousBatchEndTime': str(prev_end),
                                    'TrackOutTime': str(curr_start)
                                })
        
    except Exception as e:
        results['status'] = 'error'
        results['errors'].append({'type': '验证过程错误', 'message': str(e)})
    
    return results


def validate_sap_routing_data(routing_df: pd.DataFrame) -> Dict[str, Any]:
    """
    验证SAP Routing数据的完整性
    Args:
        routing_df: SAP Routing处理后的数据
    Returns:
        验证结果字典
    """
    results = {'status': 'success', 'details': [], 'errors': []}
    
    try:
        # 1. 验证关键字段完整性
        required_fields = ['Material Number', 'Operation', 'Group']
        completeness = {}
        
        for field in required_fields:
            if field in routing_df.columns:
                valid_count = routing_df[field].notna().sum()
                completeness[field] = f"{valid_count}/{len(routing_df)} ({valid_count/len(routing_df)*100:.1f}%)"
            else:
                completeness[field] = "字段不存在"
                results['errors'].append({'type': '缺少关键字段', 'field': field})
        
        results['details'].append({
            'type': 'SAP Routing数据完整性',
            'data': completeness
        })
        
        # 2. 验证标准时间数据合理性
        time_fields = ['EH_machine(s)', 'EH_labor(s)', 'OEE']
        time_stats = {}
        
        for field in time_fields:
            if field in routing_df.columns:
                valid_data = routing_df[routing_df[field].notna() & (routing_df[field] > 0)]
                if len(valid_data) > 0:
                    time_stats[field] = {
                        '有效记录数': len(valid_data),
                        '最小值': round(valid_data[field].min(), 2),
                        '最大值': round(valid_data[field].max(), 2),
                        '平均值': round(valid_data[field].mean(), 2)
                    }
        
        if time_stats:
            results['details'].append({
                'type': '标准时间数据统计',
                'data': time_stats
            })
        
        # 3. 验证数据唯一性
        if 'Material Number' in routing_df.columns and 'Operation' in routing_df.columns:
            duplicate_check = routing_df.duplicated(subset=['Material Number', 'Operation'])
            duplicate_count = duplicate_check.sum()
            
            results['details'].append({
                'type': '数据唯一性检查',
                '总记录数': len(routing_df),
                '重复记录数': duplicate_count,
                '唯一记录数': len(routing_df) - duplicate_count
            })
        
    except Exception as e:
        results['status'] = 'error'
        results['errors'].append({'type': '验证过程错误', 'message': str(e)})
    
    return results


def generate_validation_report() -> str:
    """
    生成完整的验证报告
    Returns:
        报告文本
    """
    report_lines = []
    report_lines.append("=" * 80)
    report_lines.append("SA指标ETL数据清洗结果验证报告")
    report_lines.append(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report_lines.append("=" * 80)
    report_lines.append("")
    
    try:
        # 验证SFC数据
        report_lines.append("1. SFC批次报工数据验证")
        report_lines.append("-" * 40)
        sfc_df = load_latest_data('sfc')
        sfc_results = validate_sfc_calculations(sfc_df)
        
        if sfc_results['status'] == 'success' and len(sfc_results['errors']) == 0:
            report_lines.append(f"✅ SFC数据验证通过")
        else:
            report_lines.append(f"❌ SFC数据验证发现问题")
        
        # 显示关键统计
        for detail in sfc_results['details']:
            if detail['type'] == '数据完整性':
                report_lines.append(f"   📊 {detail['type']}:")
                for field, completeness in detail['data'].items():
                    report_lines.append(f"      - {field}: {completeness}")
            elif detail['type'] == '标准时间统计':
                report_lines.append(f"   📊 {detail['type']}:")
                for key, value in detail['data'].items():
                    report_lines.append(f"      - {key}: {value}")
        
        if sfc_results['errors']:
            report_lines.append(f"   ⚠️ 发现 {len(sfc_results['errors'])} 个错误:")
            for error in sfc_results['errors'][:3]:  # 只显示前3个错误
                if error['type'] == '时间计算差异':
                    report_lines.append(f"      * {error['type']}: 批次{error['BatchNumber']}, 工序{error['Operation']}")
                    report_lines.append(f"        LT差异: {error['LT差异']}h, PT差异: {error['PT差异']}h, ST差异: {error['ST差异']}h")
                else:
                    report_lines.append(f"      * {error['type']}: {error.get('message', '')}")
        
        report_lines.append("")
        
        # 验证MES数据
        report_lines.append("2. MES批次报工数据验证")
        report_lines.append("-" * 40)
        mes_df = load_latest_data('mes')
        mes_results = validate_mes_calculations(mes_df)
        
        if mes_results['status'] == 'success' and len(mes_results['errors']) == 0:
            report_lines.append(f"✅ MES数据验证通过")
        else:
            report_lines.append(f"❌ MES数据验证发现问题")
        
        # 显示关键统计
        for detail in mes_results['details']:
            if detail['type'] in ['SFC数据合并统计', '标准时间匹配统计']:
                report_lines.append(f"   📊 {detail['type']}:")
                for key, value in detail.items():
                    if key != 'type':
                        report_lines.append(f"      - {key}: {value}")
        
        if mes_results['errors']:
            report_lines.append(f"   ⚠️ 发现 {len(mes_results['errors'])} 个错误:")
            for error in mes_results['errors'][:3]:  # 只显示前3个错误
                report_lines.append(f"      * {error['type']}: {error.get('BatchNumber', error.get('machine', ''))}")
        
        report_lines.append("")
        
        # 验证SAP Routing数据
        report_lines.append("3. SAP Routing标准时间数据验证")
        report_lines.append("-" * 40)
        routing_df = load_latest_data('sap_routing')
        routing_results = validate_sap_routing_data(routing_df)
        
        if routing_results['status'] == 'success' and len(routing_results['errors']) == 0:
            report_lines.append(f"✅ SAP Routing数据验证通过")
        else:
            report_lines.append(f"❌ SAP Routing数据验证发现问题")
        
        # 显示关键统计
        for detail in routing_results['details']:
            if detail['type'] in ['SAP Routing数据完整性', '标准时间数据统计', '数据唯一性检查']:
                report_lines.append(f"   📊 {detail['type']}:")
                for key, value in detail.items():
                    if key != 'type':
                        if isinstance(value, dict):
                            for sub_key, sub_value in value.items():
                                report_lines.append(f"      - {sub_key}: {sub_value}")
                        else:
                            report_lines.append(f"      - {key}: {value}")
        
        if routing_results['errors']:
            report_lines.append(f"   ⚠️ 发现 {len(routing_results['errors'])} 个错误:")
            for error in routing_results['errors'][:3]:  # 只显示前3个错误
                report_lines.append(f"      * {error['type']}: {error.get('message', '')}")
        
        report_lines.append("")
        
        # 总结
        total_errors = len(sfc_results['errors']) + len(mes_results['errors']) + len(routing_results['errors'])
        report_lines.append("=" * 80)
        report_lines.append("验证总结")
        report_lines.append("=" * 80)
        
        if total_errors == 0:
            report_lines.append("🎉 所有数据验证通过！清洗结果正确。")
        else:
            report_lines.append(f"⚠️ 发现 {total_errors} 个问题，建议详细检查。")
        
        report_lines.append("")
        report_lines.append(f"📊 数据统计:")
        report_lines.append(f"   - SFC数据记录数: {len(sfc_df):,}")
        report_lines.append(f"   - MES数据记录数: {len(mes_df):,}")
        report_lines.append(f"   - SAP Routing数据记录数: {len(routing_df):,}")
        
        # 数据质量评分
        quality_score = max(0, 100 - total_errors * 2)  # 每个错误扣2分
        report_lines.append(f"   - 数据质量评分: {quality_score}/100")
        
    except Exception as e:
        report_lines.append(f"❌ 验证过程发生错误: {str(e)}")
    
    return "\n".join(report_lines)


def main():
    """主函数"""
    # 设置日志
    log_config = cfg.get("logging", {
        'level': 'INFO',
        'file': 'logs/etl_validate_sa.log'
    })
    setup_logging(log_config)
    
    logging.info("开始SA指标数据清洗结果验证")
    
    try:
        # 生成验证报告
        report = generate_validation_report()
        
        # 输出报告到控制台
        print(report)
        
        # 保存报告到文件
        report_config = cfg.get("report", {})
        output_dir = report_config.get("output_dir", "logs")
        filename_format = report_config.get("filename_format", "validation_report_sa_{timestamp}.txt")
        timestamp_format = report_config.get("timestamp_format", "%Y%m%d_%H%M%S")
        
        timestamp = datetime.now().strftime(timestamp_format)
        report_file = os.path.join(output_dir, filename_format.format(timestamp=timestamp))
        
        os.makedirs(output_dir, exist_ok=True)
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report)
        
        logging.info(f"验证报告已保存到: {report_file}")
        print(f"\n📄 验证报告已保存到: {report_file}")
        
    except Exception as e:
        logging.error(f"验证失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
