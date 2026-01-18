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
        # 将关键字段组合为字符串标识（使用向量化操作，避免apply）
        df_records = df[unique_key_fields].fillna('').astype(str)
        # 使用向量化字符串拼接，比apply快很多
        record_ids = df_records.iloc[:, 0]
        for col in df_records.columns[1:]:
            record_ids = record_ids + '|' + df_records[col]
        
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
    
    支持两种配置方式：
    1. path: 单个文件路径
    2. pattern: 通配符模式，匹配多个文件（如 CMES_Product_Output_CZM_*.xlsx）
    
    Args:
        cfg: 配置字典
        
    Returns:
        合并后的DataFrame，包含factory_source和factory_name列
    """
    import glob
    
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
        
        # 支持两种配置方式：pattern（通配符）或 path（单文件）
        file_pattern = source.get("pattern")
        file_path = source.get("path")
        
        # 确定要读取的文件列表
        files_to_read = []
        if file_pattern:
            # 使用通配符匹配多个文件
            matched_files = glob.glob(file_pattern)
            if matched_files:
                files_to_read = sorted(matched_files)  # 按文件名排序
                logging.info(f"{factory_name}: 通配符匹配到 {len(files_to_read)} 个文件")
                for f in files_to_read:
                    logging.debug(f"  - {os.path.basename(f)}")
            else:
                logging.warning(f"[WARNING] {factory_name}: 通配符 '{file_pattern}' 未匹配到任何文件")
                continue
        elif file_path:
            # 单文件模式
            files_to_read = [file_path]
        else:
            logging.error(f"[ERROR] {factory_name}: 未配置 path 或 pattern")
            continue
        
        # 读取所有匹配的文件
        factory_dataframes = []
        for file_path in files_to_read:
            logging.info(f"正在读取 {factory_name} - {os.path.basename(file_path)}...")
            
            try:
                # 读取Excel数据
                df = read_sharepoint_excel(file_path)
                
                if not df.empty:
                    # 添加工厂标识列
                    df["factory_source"] = factory_id
                    df["factory_name"] = factory_name
                    # 添加源文件名（便于追溯）
                    df["source_file"] = os.path.basename(file_path)
                    
                    # 数据类型标准化处理
                    df = standardize_data_types(df)
                    
                    factory_dataframes.append(df)
                    logging.info(f"  [OK] 成功读取 {len(df)} 行数据")
                else:
                    logging.warning(f"  [WARNING] 数据为空: {os.path.basename(file_path)}")
                    
            except Exception as e:
                logging.error(f"  [ERROR] 读取失败 - {e}")
                continue
        
        # 合并该工厂的所有文件数据
        if factory_dataframes:
            factory_combined = pd.concat(factory_dataframes, ignore_index=True)
            all_dataframes.append(factory_combined)
            logging.info(f"[OK] {factory_name}: 总计 {len(factory_combined)} 行数据（来自 {len(factory_dataframes)} 个文件）")
    
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


# ==================== 统一的两层增量处理工具 ====================

import json
import hashlib
from datetime import datetime

class IncrementalProcessor:
    """
    统一的两层增量处理器
    
    第1层：文件级去重 - 基于文件修改时间快速跳过未变化的文件
    第2层：记录级去重 - 基于业务唯一键过滤已处理的记录
    """
    
    def __init__(self, state_file: str, unique_key_fields: List[str]):
        """
        初始化增量处理器
        
        Args:
            state_file: 状态文件路径
            unique_key_fields: 用于生成记录唯一标识的字段列表
        """
        self.state_file = state_file
        self.unique_key_fields = unique_key_fields
        self.state = self._load_state()
    
    def _load_state(self) -> Dict[str, Any]:
        """加载状态文件"""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    state = json.load(f)
                    # 确保必要的字段存在
                    if "processed_files" not in state:
                        state["processed_files"] = {}
                    if "processed_records" not in state:
                        state["processed_records"] = []
                    return state
            except Exception as e:
                logging.warning(f"加载状态文件失败: {e}，将使用默认状态")
        
        return {
            "last_update": None,
            "processed_files": {},  # {文件路径: {mtime: 修改时间, size: 文件大小}}
            "processed_records": [],  # 已处理记录的唯一标识列表
            "total_records": 0
        }
    
    def _save_state(self) -> None:
        """保存状态文件"""
        try:
            os.makedirs(os.path.dirname(self.state_file), exist_ok=True)
            self.state["last_update"] = datetime.now().isoformat()
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(self.state, f, ensure_ascii=False, indent=2)
            logging.info(f"状态文件已保存: {self.state_file}")
        except Exception as e:
            logging.error(f"保存状态文件失败: {e}")
    
    def is_file_changed(self, file_path: str) -> bool:
        """
        第1层：检查文件是否有变化
        
        Args:
            file_path: 文件路径
            
        Returns:
            True: 文件有变化或是新文件，需要处理
            False: 文件未变化，可以跳过
        """
        if not os.path.exists(file_path):
            return False
        
        current_mtime = os.path.getmtime(file_path)
        current_size = os.path.getsize(file_path)
        
        processed_files = self.state.get("processed_files", {})
        if file_path not in processed_files:
            return True  # 新文件
        
        file_info = processed_files[file_path]
        stored_mtime = file_info.get("mtime")
        stored_size = file_info.get("size")
        
        # 如果修改时间或大小变化，认为文件有变化
        if stored_mtime != current_mtime or stored_size != current_size:
            return True
        
        return False
    
    def filter_changed_files(self, file_paths: List[str]) -> List[str]:
        """
        第1层：过滤出有变化的文件
        
        Args:
            file_paths: 文件路径列表
            
        Returns:
            有变化的文件路径列表
        """
        changed_files = []
        skipped_count = 0
        
        for file_path in file_paths:
            if self.is_file_changed(file_path):
                changed_files.append(file_path)
            else:
                skipped_count += 1
        
        if skipped_count > 0:
            logging.info(f"文件级去重：跳过 {skipped_count} 个未变化的文件，剩余 {len(changed_files)} 个待处理")
        
        return changed_files
    
    def mark_file_processed(self, file_path: str) -> None:
        """标记文件为已处理"""
        if os.path.exists(file_path):
            self.state["processed_files"][file_path] = {
                "mtime": os.path.getmtime(file_path),
                "size": os.path.getsize(file_path),
                "processed_time": datetime.now().isoformat()
            }
    
    def _generate_record_id(self, row: pd.Series) -> str:
        """生成记录的唯一标识"""
        values = []
        for field in self.unique_key_fields:
            val = row.get(field)
            if pd.isna(val):
                val = ''
            else:
                val = str(val)
            values.append(val)
        return '|'.join(values)
    
    def filter_new_records(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        第2层：过滤出新记录
        
        Args:
            df: 数据DataFrame
            
        Returns:
            只包含新记录的DataFrame
        """
        if df.empty:
            return df
        
        # 检查必需字段是否存在
        missing_fields = [f for f in self.unique_key_fields if f not in df.columns]
        if missing_fields:
            logging.warning(f"记录级去重所需字段不存在: {missing_fields}，返回全部数据")
            return df
        
        # 获取已处理的记录标识
        processed_records = set(self.state.get("processed_records", []))
        
        # 生成每条记录的唯一标识
        df['_record_id'] = df.apply(lambda row: self._generate_record_id(row), axis=1)
        
        # 筛选新记录
        new_df = df[~df['_record_id'].isin(processed_records)].copy()
        new_df = new_df.drop(columns=['_record_id'])
        
        total_count = len(df)
        new_count = len(new_df)
        existing_count = total_count - new_count
        
        logging.info(f"记录级去重：总数据 {total_count} 行，已处理 {existing_count} 行，新数据 {new_count} 行")
        
        return new_df
    
    def update_processed_records(self, df: pd.DataFrame) -> None:
        """更新已处理的记录标识"""
        if df.empty:
            return
        
        # 检查必需字段是否存在
        missing_fields = [f for f in self.unique_key_fields if f not in df.columns]
        if missing_fields:
            logging.warning(f"更新状态所需字段不存在: {missing_fields}")
            return
        
        # 生成新记录的标识
        new_ids = df.apply(lambda row: self._generate_record_id(row), axis=1).tolist()
        
        # 合并到已处理集合
        existing_ids = set(self.state.get("processed_records", []))
        all_ids = existing_ids.union(set(new_ids))
        
        self.state["processed_records"] = list(all_ids)
        self.state["total_records"] = len(all_ids)
        
        logging.info(f"状态已更新：总记录 {len(all_ids)}，新增 {len(new_ids)} 条")
    
    def save(self) -> None:
        """保存状态"""
        self._save_state()
    
    def clear_state(self) -> None:
        """清除状态（用于全量刷新）"""
        self.state = {
            "last_update": None,
            "processed_files": {},
            "processed_records": [],
            "total_records": 0
        }
        if os.path.exists(self.state_file):
            os.remove(self.state_file)
            logging.info(f"状态文件已清除: {self.state_file}")


# ==================== 日志轮转配置 ====================

from logging.handlers import RotatingFileHandler

def setup_logging_with_rotation(cfg: Dict[str, Any], base_dir: str = None, 
                                 max_bytes: int = 10*1024*1024, backup_count: int = 5) -> None:
    """
    配置带轮转的日志
    
    Args:
        cfg: 配置字典
        base_dir: 基础目录
        max_bytes: 单个日志文件最大大小（默认10MB）
        backup_count: 保留的备份文件数量（默认5个）
    """
    if base_dir is None:
        base_dir = os.path.dirname(os.path.abspath(__file__))
    
    log_level = cfg.get("logging", {}).get("level", "INFO")
    log_file = cfg.get("logging", {}).get("file", "logs/etl.log")
    
    if os.path.isabs(log_file):
        log_path = log_file
    else:
        log_path = os.path.join(base_dir, log_file)
    
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    
    # 清除现有的handlers
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # 创建带轮转的文件handler
    file_handler = RotatingFileHandler(
        log_path, 
        maxBytes=max_bytes, 
        backupCount=backup_count,
        encoding='utf-8'
    )
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    
    # 创建控制台handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    
    # 配置root logger
    root_logger.setLevel(getattr(logging, log_level))
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)


# ==================== 双份输出工具 ====================

import shutil

def save_to_dual_locations(df: pd.DataFrame, primary_path: str, secondary_path: str, 
                           cfg: Dict[str, Any] = None) -> bool:
    """
    保存数据到两个位置
    
    Args:
        df: 数据DataFrame
        primary_path: 主输出路径（如SharePoint同步目录）
        secondary_path: 备份路径（如项目目录）
        cfg: 配置字典
        
    Returns:
        是否成功
    """
    success = True
    
    # 保存到主位置
    try:
        os.makedirs(os.path.dirname(primary_path), exist_ok=True)
        if save_to_parquet(df, primary_path, cfg):
            logging.info(f"主输出已保存: {primary_path}")
        else:
            success = False
    except Exception as e:
        logging.error(f"保存到主位置失败: {e}")
        success = False
    
    # 保存到备份位置
    try:
        os.makedirs(os.path.dirname(secondary_path), exist_ok=True)
        if save_to_parquet(df, secondary_path, cfg):
            logging.info(f"备份输出已保存: {secondary_path}")
        else:
            success = False
    except Exception as e:
        logging.error(f"保存到备份位置失败: {e}")
        success = False
    
    return success


# ==================== BaseETL基类 ====================

from abc import ABC, abstractmethod

class BaseETL(ABC):
    """
    ETL基类，提供统一的ETL框架
    
    子类需要实现:
    - extract(): 数据提取
    - transform(df): 数据转换
    - get_output_filename(): 输出文件名
    """
    
    def __init__(self, source_name: str, config_path: str, unique_key_fields: List[str] = None):
        """
        初始化ETL
        
        Args:
            source_name: 数据源名称（如 'sap', 'mes', 'sfc'）
            config_path: 配置文件路径
            unique_key_fields: 用于记录级去重的唯一键字段
        """
        self.source_name = source_name
        self.config_path = config_path
        self.config = load_config(config_path)
        self.unique_key_fields = unique_key_fields or []
        
        # 获取项目根目录
        self.project_root = self._get_project_root()
        
        # 设置状态文件路径（统一放到项目目录）
        self.state_dir = os.path.join(
            self.project_root, 
            "data_pipelines", "sources", source_name, "state"
        )
        os.makedirs(self.state_dir, exist_ok=True)
        
        # 设置输出目录（项目目录下的publish）
        self.local_publish_dir = os.path.join(
            self.project_root,
            "data_pipelines", "sources", source_name, "publish"
        )
        os.makedirs(self.local_publish_dir, exist_ok=True)
        
        # 初始化增量处理器
        state_file = os.path.join(self.state_dir, f"etl_{source_name}_state.json")
        self.incr_processor = IncrementalProcessor(
            state_file=state_file,
            unique_key_fields=self.unique_key_fields
        )
        
        # 设置日志
        setup_logging_with_rotation(self.config, self.project_root)
        
        # 运行统计
        self.start_time = None
        self.stats = {
            "source_name": source_name,
            "input_records": 0,
            "output_records": 0,
            "skipped_files": 0,
            "new_files": 0
        }
    
    def _get_project_root(self) -> str:
        """获取项目根目录"""
        # 从配置文件路径向上查找项目根目录
        current = os.path.dirname(os.path.abspath(self.config_path))
        while current != os.path.dirname(current):  # 不是根目录
            if os.path.exists(os.path.join(current, "data_pipelines")):
                return current
            current = os.path.dirname(current)
        # 如果找不到，使用默认路径
        return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    def get_primary_output_dir(self) -> str:
        """获取主输出目录（从配置文件读取）"""
        return self.config.get("output", {}).get("base_dir", self.local_publish_dir)
    
    def get_local_output_dir(self) -> str:
        """获取本地输出目录（项目目录）"""
        return self.local_publish_dir
    
    @abstractmethod
    def extract(self) -> pd.DataFrame:
        """数据提取 - 子类必须实现"""
        pass
    
    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """数据转换 - 子类必须实现"""
        pass
    
    @abstractmethod
    def get_output_filename(self) -> str:
        """获取输出文件名 - 子类必须实现"""
        pass
    
    def has_file_changes(self, file_paths: List[str]) -> bool:
        """检查是否有文件变化"""
        changed_files = self.incr_processor.filter_changed_files(file_paths)
        self.stats["skipped_files"] = len(file_paths) - len(changed_files)
        self.stats["new_files"] = len(changed_files)
        return len(changed_files) > 0
    
    def load(self, df: pd.DataFrame) -> bool:
        """
        数据加载 - 保存到双份位置
        
        Args:
            df: 处理后的数据
            
        Returns:
            是否成功
        """
        if df.empty:
            logging.info("数据为空，跳过保存")
            return True
        
        filename = self.get_output_filename()
        primary_path = os.path.join(self.get_primary_output_dir(), filename)
        local_path = os.path.join(self.get_local_output_dir(), filename)
        
        self.stats["output_records"] = len(df)
        
        return save_to_dual_locations(df, primary_path, local_path, self.config)
    
    def run(self, force_full_refresh: bool = False) -> bool:
        """
        运行ETL流程
        
        Args:
            force_full_refresh: 是否强制全量刷新
            
        Returns:
            是否成功
        """
        self.start_time = time.time()
        
        logging.info("=" * 60)
        logging.info(f"{self.source_name.upper()} ETL启动")
        logging.info(f"刷新模式: {'全量刷新' if force_full_refresh else '增量刷新'}")
        logging.info("=" * 60)
        
        try:
            # 如果强制全量刷新，清除状态
            if force_full_refresh:
                self.incr_processor.clear_state()
            
            # 1. 提取数据
            logging.info("开始提取数据...")
            df = self.extract()
            
            if df is None or df.empty:
                logging.info("没有新数据需要处理")
                self._log_summary()
                return True
            
            self.stats["input_records"] = len(df)
            
            # 2. 转换数据
            logging.info("开始转换数据...")
            df = self.transform(df)
            
            # 3. 加载数据
            logging.info("开始保存数据...")
            success = self.load(df)
            
            # 4. 保存状态
            if success:
                self.incr_processor.save()
            
            self._log_summary()
            return success
            
        except Exception as e:
            logging.exception(f"ETL执行失败: {e}")
            return False
    
    def _log_summary(self) -> None:
        """输出执行摘要"""
        elapsed = time.time() - self.start_time if self.start_time else 0
        
        logging.info("=" * 60)
        logging.info(f"{self.source_name.upper()} ETL执行摘要")
        logging.info("-" * 60)
        logging.info(f"  数据源: {self.source_name}")
        logging.info(f"  输入记录: {self.stats['input_records']}")
        logging.info(f"  输出记录: {self.stats['output_records']}")
        logging.info(f"  跳过文件: {self.stats['skipped_files']}")
        logging.info(f"  新文件: {self.stats['new_files']}")
        logging.info(f"  耗时: {elapsed:.2f}秒")
        logging.info("=" * 60)
