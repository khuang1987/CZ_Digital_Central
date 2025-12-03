"""
ETL通用工具函数
提供可复用的数据处理、配置管理、日志等功能
"""

import os
import sys
import time
import logging
import yaml
import pandas as pd
from typing import Dict, List, Any, Optional
from zipfile import BadZipFile


def setup_logging(cfg: Dict[str, Any], base_dir: str = None) -> None:
    """配置日志"""
    if base_dir is None:
        base_dir = os.path.dirname(os.path.abspath(__file__))
    
    log_level = cfg.get("logging", {}).get("level", "INFO")
    log_file = cfg.get("logging", {}).get("file", "logs/etl.log")
    
    # 修复：正确处理绝对路径和相对路径
    if os.path.isabs(log_file):
        # 如果是绝对路径，直接使用
        log_path = log_file
    else:
        # 如果是相对路径，相对于base_dir
        log_path = os.path.join(base_dir, log_file)
    
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_path, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )


def load_config(config_path: str) -> Dict[str, Any]:
    """加载配置文件"""
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def read_sharepoint_excel(file_path: str, max_rows: Optional[int] = None, max_retries: int = 3) -> pd.DataFrame:
    """读取SharePoint同步的Excel文件，增强错误处理和重试机制"""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Excel文件不存在: {file_path}")
    
    # 检查文件大小（损坏文件通常大小异常）
    file_size = os.path.getsize(file_path)
    if file_size == 0:
        raise ValueError(f"Excel文件为空: {file_path}")
    if file_size < 1024:  # 小于1KB可能是损坏文件
        logging.warning(f"Excel文件大小异常 ({file_size} bytes): {file_path}")
    
    # 重试机制
    for attempt in range(max_retries):
        try:
            # 检查文件是否被锁定（SharePoint同步时常见）
            if attempt > 0:
                logging.info(f"重试读取Excel文件 (第{attempt + 1}次): {file_path}")
                time.sleep(2 * attempt)  # 递增延迟
            
            # 使用read_only模式避免锁定问题
            df = pd.read_excel(file_path, engine='openpyxl', sheet_name=0, nrows=max_rows)
            
            if max_rows and len(df) > 0:
                logging.info(f"测试模式：仅读取前 {len(df)} 行数据")
            return df
            
        except BadZipFile as e:
            if attempt < max_retries - 1:
                logging.warning(f"Excel文件可能正在同步中，等待后重试: {file_path}")
                continue
            else:
                # 提供具体的恢复建议
                logging.error(f"Excel文件损坏或同步未完成: {file_path}")
                logging.error("建议解决方案:")
                logging.error("1. 等待SharePoint同步完成（通常需要几分钟）")
                logging.error("2. 检查文件是否有.tmp或.lock文件")
                logging.error("3. 尝试手动打开文件确认是否正常")
                logging.error("4. 如果持续失败，联系IT检查SharePoint同步状态")
                raise ValueError(f"Excel文件损坏，请检查SharePoint同步状态: {file_path}") from e
                
        except PermissionError as e:
            if attempt < max_retries - 1:
                logging.warning(f"Excel文件被占用，等待后重试: {file_path}")
                time.sleep(3 * attempt)
                continue
            else:
                logging.error(f"Excel文件被其他程序占用: {file_path}")
                logging.error("请关闭正在使用该文件的程序（如Excel）")
                raise PermissionError(f"文件被占用，请关闭Excel或其他程序: {file_path}") from e
                
        except Exception as e:
            logging.error(f"读取Excel文件失败: {file_path}, 错误: {e}")
            if attempt < max_retries - 1:
                logging.info(f"未知错误，重试中...")
                continue
            else:
                raise


def save_to_parquet(df: pd.DataFrame, output_path: str, cfg: Dict[str, Any] = None) -> None:
    """保存DataFrame为Parquet格式"""
    if cfg is None:
        cfg = {}
    
    # 确保输出目录存在
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    if df.empty:
        logging.warning("数据为空，不保存")
        return
    
    # 获取压缩配置
    compression = cfg.get("output", {}).get("parquet", {}).get("compression", "snappy")
    
    # 处理日期字段：将datetime.date转换为datetime.datetime
    date_fields = ["TrackOutDate", "Date created"]
    for col in date_fields:
        if col in df.columns:
            # 将date类型转换为datetime类型
            df[col] = pd.to_datetime(df[col], errors='coerce')
            logging.info(f"已转换日期列 {col} 为datetime类型")
    
    # 处理datetime字段：确保NaT转换为None，并保持datetime类型
    datetime_fields = [
        "TrackOutTime", "EnterStepTime", "TrackInTime", "StartTime",
        "PreviousBatchEndTime", "DueTime", "Checkin_SFC"
    ]
    for col in datetime_fields:
        if col in df.columns:
            # 确保是datetime类型
            if df[col].dtype not in ['datetime64[ns]', 'datetime64[us]', 'datetime64[ms]']:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            # 将NaT转换为None（保存到Parquet时会是null）
            df[col] = df[col].where(pd.notna(df[col]), None)
            # 如果全部是None，转换为object类型以支持null值
            if df[col].isna().all():
                df[col] = df[col].astype('object')
    
    # 确保字符串列类型正确
    for col in df.columns:
        if col not in datetime_fields + date_fields and df[col].dtype == 'object':
            df[col] = df[col].astype(str).replace('nan', None).replace('None', None)
            # 将空字符串也转换为None
            df[col] = df[col].replace('', None)
    
    try:
        df.to_parquet(output_path, index=False, engine='pyarrow', compression=compression)
        logging.info(f"已保存Parquet文件: {output_path}, 行数: {len(df)}")
        
        # 同时保存Excel文件用于数据完整性检查
        save_to_excel_for_validation(df, output_path, cfg)
        
    except Exception as e:
        logging.error(f"保存Parquet失败: {e}")
        raise


def save_to_excel_for_validation(df: pd.DataFrame, parquet_path: str, cfg: Dict[str, Any] = None) -> None:
    """保存DataFrame为Excel格式用于数据完整性检查"""
    if cfg is None:
        cfg = {}
    
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
                    if df[col].dtype in ['int64', 'float64']:
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


def prompt_refresh_mode(default_incremental: bool = True, countdown_seconds: int = 10) -> bool:
    """提示用户选择刷新模式"""
    import msvcrt
    import threading
    import time
    
    print("="*60)
    print("请选择数据处理模式：")
    print("="*60)
    print("1. 增量刷新（Incremental）- 只处理新数据，保留历史数据")
    print("2. 全量刷新（Full Refresh）- 清除所有历史数据，重新处理全部数据")
    print("="*60)
    print()
    
    default_choice = "1" if default_incremental else "2"
    default_text = "增量刷新" if default_incremental else "全量刷新"
    print(f"默认选择: {default_choice} ({default_text})")
    
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
                # 递归调用重新获取输入（不使用倒计时）
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
        print()
        
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
            print()
            return default_incremental
        else:
            print()  # 换行
            return user_choice[0]
    else:
        # 传统等待模式
        get_user_input()
        return user_choice[0] if user_choice[0] is not None else default_incremental


def update_etl_state(df: pd.DataFrame, state_file: str, cfg: Dict[str, Any]) -> None:
    """更新ETL状态文件"""
    import json
    from datetime import datetime
    
    if df.empty:
        logging.warning("数据为空，跳过状态更新")
        return
    
    # 获取唯一键字段配置
    unique_key_fields = cfg.get("incremental", {}).get("unique_key_fields", [])
    if not unique_key_fields:
        logging.warning("未配置唯一键字段，无法更新ETL状态")
        return
    
    # 检查必要字段是否存在
    missing_fields = [field for field in unique_key_fields if field not in df.columns]
    if missing_fields:
        logging.warning(f"缺少必要字段用于状态更新: {missing_fields}")
        return
    
    # 生成记录标识
    try:
        # 将关键字段组合为字符串标识
        df_records = df[unique_key_fields].astype(str)
        record_ids = df_records.apply(lambda row: '|'.join(row.values), axis=1)
        
        # 读取现有状态
        existing_ids = set()
        if os.path.exists(state_file):
            try:
                with open(state_file, 'r', encoding='utf-8') as f:
                    state_data = json.load(f)
                    existing_ids = set(state_data.get('processed_records', []))
            except Exception as e:
                logging.warning(f"读取状态文件失败，将创建新状态: {e}")
        
        # 更新状态
        new_ids = set(record_ids) - existing_ids
        all_ids = existing_ids.union(set(record_ids))
        
        # 保存状态
        state_data = {
            'last_update': datetime.now().isoformat(),
            'total_records': len(all_ids),
            'processed_records': list(all_ids),
            'new_records_count': len(new_ids)
        }
        
        os.makedirs(os.path.dirname(state_file), exist_ok=True)
        with open(state_file, 'w', encoding='utf-8') as f:
            json.dump(state_data, f, ensure_ascii=False, indent=2)
        
        logging.info(f"ETL状态已更新: 总记录 {len(all_ids)}, 新增 {len(new_ids)} 条")
        
    except Exception as e:
        logging.warning(f"更新ETL状态失败: {e}")


def get_base_dir() -> str:
    """获取脚本基础目录"""
    return os.path.dirname(os.path.abspath(__file__))


def ensure_directory_exists(directory: str) -> None:
    """确保目录存在"""
    os.makedirs(directory, exist_ok=True)


# ==================== 多工厂数据处理工具 ====================

def read_multi_factory_mes_data(cfg: Dict[str, Any]) -> pd.DataFrame:
    """
    读取多工厂数据并合并
    
    Args:
        cfg: 配置字典
        
    Returns:
        合并后的DataFrame，包含factory_source和factory_name列
    """
    all_dataframes = []
    
    # 获取多工厂数据源配置
    mes_sources = cfg.get("source", {}).get("mes_sources", [])
    
    if not mes_sources:
        logging.error("未找到mes_sources配置，请检查配置文件")
        return pd.DataFrame()
    
    logging.info(f"开始读取 {len(mes_sources)} 个工厂数据...")
    
    for source in mes_sources:
        if not source.get("enabled", True):
            logging.info(f"跳过已禁用的工厂: {source.get('factory_name', 'Unknown')}")
            continue
            
        factory_id = source["factory_id"]
        factory_name = source["factory_name"]
        file_path = source["path"]
        
        logging.info(f"正在读取 {factory_name} 数据...")
        logging.debug(f"文件路径: {file_path}")
        
        try:
            # 读取Excel数据
            df = read_sharepoint_excel(file_path)
            
            if not df.empty:
                # 添加工厂标识列
                df["factory_source"] = factory_id
                df["factory_name"] = factory_name
                
                # 数据类型标准化处理
                df = standardize_data_types(df)
                
                all_dataframes.append(df)
                logging.info(f"[OK] {factory_name}: 成功读取 {len(df)} 行数据")
            else:
                logging.warning(f"[WARNING] {factory_name}: 数据为空")
                
        except Exception as e:
            logging.error(f"[ERROR] {factory_name}: 读取失败 - {e}")
            continue
    
    if all_dataframes:
        # 合并所有工厂数据
        try:
            combined_df = pd.concat(all_dataframes, ignore_index=True)
            logging.info(f"[OK] 多工厂数据合并完成: 总计 {len(combined_df)} 行数据")
            
            # 记录各工厂数据统计
            factory_stats = combined_df['factory_source'].value_counts().to_dict()
            logging.info(f"各工厂数据量: {factory_stats}")
            
            return combined_df
            
        except Exception as e:
            logging.error(f"[ERROR] 数据合并失败: {e}")
            return pd.DataFrame()
    else:
        logging.error("[ERROR] 没有读取到有效数据")
        return pd.DataFrame()


def standardize_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    标准化数据类型，处理多工厂间的类型差异
    
    Args:
        df: 原始DataFrame
        
    Returns:
        标准化后的DataFrame
    """
    result = df.copy()
    
    # 需要标准化的数值字段（基于验证结果）
    numeric_fields = [
        'ProductionOrder',
        'Last_TrackIn_SecondaryQuantity',
        'First_TrackIn_PrimaryQuantity', 
        'Step_Duration_Minute',
        'Last_TrackIn_PrimaryQuantity'
    ]
    
    for field in numeric_fields:
        if field in result.columns:
            try:
                # 转换为float64，处理空值和类型不一致
                result[field] = pd.to_numeric(result[field], errors='coerce').astype('float64')
                logging.debug(f"字段 {field} 已标准化为 float64")
            except Exception as e:
                logging.warning(f"字段 {field} 标准化失败: {e}")
    
    # 标准化其他数值字段
    other_numeric_fields = [
        'StepInQuantity',
        'TrackOutQuantity',
        'LT(d)',
        'PT(d)', 
        'ST(d)',
        'Setup Time (h)',
        'OEE',
        'EH_machine(s)',
        'EH_labor(s)',
        'Tolerance(h)',
        'NonWorkday(d)'
    ]
    
    for field in other_numeric_fields:
        if field in result.columns:
            try:
                result[field] = pd.to_numeric(result[field], errors='coerce')
            except Exception as e:
                logging.debug(f"字段 {field} 数值转换失败: {e}")
    
    return result


def validate_multi_factory_data(df: pd.DataFrame) -> bool:
    """
    验证多工厂数据的完整性
    
    Args:
        df: 合并后的DataFrame
        
    Returns:
        验证是否通过
    """
    if df.empty:
        logging.error("数据为空，验证失败")
        return False
    
    # 检查必需的工厂标识列
    required_columns = ['factory_source', 'factory_name']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        logging.error(f"缺少必需的工厂标识列: {missing_columns}")
        return False
    
    # 检查工厂数量
    factory_count = df['factory_source'].nunique()
    logging.info(f"数据包含 {factory_count} 个工厂")
    
    # 检查数据完整性
    total_records = len(df)
    null_factory_records = df['factory_source'].isnull().sum()
    
    if null_factory_records > 0:
        logging.warning(f"发现 {null_factory_records} 条记录缺少工厂标识")
    
    logging.info(f"多工厂数据验证通过: {total_records} 条记录")
    return True


def get_factory_summary(df: pd.DataFrame) -> Dict[str, Any]:
    """
    获取多工厂数据摘要信息
    
    Args:
        df: 合并后的DataFrame
        
    Returns:
        包含各工厂统计信息的字典
    """
    if df.empty:
        return {}
    
    summary = {
        'total_records': len(df),
        'factory_count': df['factory_source'].nunique(),
        'factory_details': {}
    }
    
    # 各工厂详细统计
    for factory_id in df['factory_source'].unique():
        factory_data = df[df['factory_source'] == factory_id]
        factory_name = factory_data['factory_name'].iloc[0] if not factory_data.empty else factory_id
        
        summary['factory_details'][factory_id] = {
            'factory_name': factory_name,
            'record_count': len(factory_data),
            'date_range': {
                'start': factory_data['TrackOutDate'].min() if 'TrackOutDate' in factory_data.columns else None,
                'end': factory_data['TrackOutDate'].max() if 'TrackOutDate' in factory_data.columns else None
            }
        }
    
    return summary
