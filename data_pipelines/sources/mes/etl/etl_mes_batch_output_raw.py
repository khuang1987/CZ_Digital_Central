"""
MES 原始数据 ETL 脚本 (V2 架构)
功能：从 Excel 读取 MES 数据，简单清洗后存入数据库原始表
不做复杂计算，计算逻辑由数据库视图完成
"""

import os
import sys
import time
import logging
import glob
import re
from datetime import datetime
from typing import Dict, List, Any, Optional
from pathlib import Path

import pandas as pd

# Ensure project root is on sys.path so `shared_infrastructure` can be imported
PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from shared_infrastructure.utils.etl_utils import (
    setup_logging,
    load_config,
    read_sharepoint_excel,
)
from shared_infrastructure.utils.db_sqlserver_only import SQLServerOnlyManager

# 配置
current_dir = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = Path(__file__).resolve().parents[4]
CONFIG_PATH = os.path.join(current_dir, "..", "config", "config_mes_batch_report.yaml")
LOG_DIR = os.path.join(PROJECT_ROOT, "shared_infrastructure", "logs", "mes")
SCHEMA_PATH = os.path.join(PROJECT_ROOT, "data_pipelines", "database", "schema", "init_schema_v2.sql")

# 设置日志
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, f"etl_mes_raw_{datetime.now().strftime('%Y%m%d')}.log"), encoding='utf-8'),
        logging.StreamHandler()
    ]
)


def get_db_manager() -> SQLServerOnlyManager:
    """获取 SQL Server 数据库管理器（只写 SQL Server）"""
    return SQLServerOnlyManager(
        sql_server=r"localhost\SQLEXPRESS",
        sql_db="mddap_v2",
        driver="ODBC Driver 17 for SQL Server"
    )


def init_database():
    """初始化 SQL Server 数据库"""
    db = get_db_manager()
    if os.path.exists(SCHEMA_PATH):
        db.init_database(SCHEMA_PATH)
        logging.info(f"SQL Server 数据库架构已初始化: {SCHEMA_PATH}")
    else:
        logging.warning(f"未找到架构文件: {SCHEMA_PATH}")
    
    logging.info("SQL Server 数据库管理器初始化成功")
    return True


def _any_regex_match(patterns: Any, text: str) -> bool:
    if patterns is None:
        return False
    if isinstance(patterns, str):
        patterns = [patterns]
    if not isinstance(patterns, list):
        return False
    for pat in patterns:
        if not pat:
            continue
        try:
            if re.search(str(pat), text, flags=re.IGNORECASE):
                return True
        except re.error:
            continue
    return False


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


def get_mes_file_tasks(
    cfg: Dict[str, Any],
    test_mode: bool = False,
    max_files: int = 3,
    force: bool = False,
    only_year: Optional[str] = None,
    only_factory: Optional[str] = None,
    only_file: Optional[str] = None,
) -> List[Dict[str, str]]:
    """获取待处理的 MES 文件清单（逐文件处理，避免一次性加载全部文件）"""
    mes_sources = cfg.get("source", {}).get("mes_sources", [])
    if not mes_sources:
        logging.error("未找到 mes_sources 配置")
        return []
    
    db = get_db_manager()
    tasks: List[Dict[str, str]] = []
    
    for source in mes_sources:
        if not source.get("enabled", True):
            continue
        
        factory_id = source["factory_id"]
        factory_name = source["factory_name"]

        if only_factory and str(factory_id).upper() != str(only_factory).upper():
            continue
        
        # 获取文件列表（支持通配符）
        file_paths: List[str] = []
        if "patterns" in source and isinstance(source.get("patterns"), list):
            for pat in source.get("patterns"):
                if not pat:
                    continue
                file_paths.extend(glob.glob(str(pat), recursive=True))
        elif "pattern" in source:
            pattern = source["pattern"]
            file_paths = glob.glob(pattern, recursive=True)
        elif "path" in source:
            file_paths = [source["path"]]
        else:
            continue

        file_paths = sorted({str(p) for p in file_paths if p})

        # 文件名正则过滤（用于适配新命名并排除旧文件）
        include_re = source.get("filename_include_regex")
        exclude_re = source.get("filename_exclude_regex")
        if include_re or exclude_re:
            before_cnt = len(file_paths)
            filtered: List[str] = []
            for p in file_paths:
                base = os.path.basename(p)
                if exclude_re and _any_regex_match(exclude_re, base):
                    continue
                if include_re and not _any_regex_match(include_re, base):
                    continue
                filtered.append(p)
            file_paths = filtered
            logging.info(f"{factory_name}: 正则过滤后文件数: {len(file_paths)} (原 {before_cnt})")
        
        # 测试模式：只取最新的 max_files 个文件
        if test_mode:
            file_paths = sorted(file_paths, reverse=True)[:max_files]
            logging.info(f"测试模式：{factory_name} 只处理最新 {len(file_paths)} 个文件")

        if only_file:
            only_file_str = str(only_file).strip()
            file_paths = [
                p for p in file_paths
                if only_file_str.lower() in os.path.basename(p).lower()
            ]
            logging.info(f"{factory_name}: 文件名过滤({only_file_str})后文件数: {len(file_paths)}")

        # 仅处理指定年份文件（基于文件名中 _YYYY 或 _YYYYMM）
        if only_year:
            year = str(only_year).strip()
            # match _YYYY, _YYYYMM, or _YYYYQn (n=1..4), and make sure it's not followed by more digits
            year_pat = re.compile(rf"_{re.escape(year)}(\d{{2}}|Q[1-4])?(?!\d)")
            file_paths = [p for p in file_paths if year_pat.search(os.path.basename(p))]
            logging.info(f"{factory_name}: 年份过滤({year})后文件数: {len(file_paths)}")
        
        # 过滤未变化的文件（force/only_file 模式跳过过滤）
        # Normalize paths to absolute and standard format
        file_paths = [os.path.normpath(os.path.abspath(p)) for p in file_paths]
        
        changed_files = file_paths
        if not (force or only_file):
            changed_files = db.filter_changed_files(f"mes_raw_{factory_id}", file_paths)
        
        if not changed_files:
            logging.info(f"{factory_name}: 所有文件未变化，跳过")
            continue
        
        logging.info(f"{factory_name}: {len(changed_files)} 个文件待处理")

        for file_path in changed_files:
            tasks.append(
                {
                    "factory_id": factory_id,
                    "factory_name": factory_name,
                    "file_path": file_path,
                }
            )

    return tasks


def _quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def bulk_insert_ignore(df: pd.DataFrame, table_name: str = "raw_mes") -> int:
    """批量插入到 SQL Server（使用 bulk_insert）"""
    if df is None or df.empty:
        return 0

    db = get_db_manager()
    try:
        return db.bulk_insert(df, table_name, if_exists="append")
    except Exception as e:
        logging.error(f"Bulk insert failed: {e}")
        raise


def delete_existing_for_source_file(file_path: str, table_name: str = "raw_mes") -> int:
    """Refresh mode helper: delete existing rows for a specific source_file."""
    db = get_db_manager()
    with db.get_connection() as conn:
        cur = conn.cursor()
        cur.execute(f"DELETE FROM dbo.{table_name} WHERE source_file = ?", (file_path,))
        deleted = int(cur.rowcount or 0)
        conn.commit()
        return deleted


def clean_mes_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    简单清洗 MES 数据（不做复杂计算）
    
    Args:
        df: 原始 DataFrame
        
    Returns:
        清洗后的 DataFrame
    """
    if df.empty:
        return df
    
    result = df.copy()
    
    # 列名标准化映射（统一命名规范）
    column_mapping = {
        # 新版 MES 导出列名
        'Material_Name': 'BatchNumber',
        'ERPOperation': 'Operation',
        'Resource': 'Machine',
        'Resource Description': 'MachineDesc',
        'DateEnteredStep': 'EnterStepTime',
        'Last_TrackIn_Date': 'TrackInTime',
        'TrackOutDate': 'TrackOutTime',
        # ERPCode 映射为 Plant，Product_Description 缩写为 Product_Desc
        'ERPCode': 'Plant',  # 工厂代码
        'Product_Description': 'Product_Desc',
        'Product_Name': 'ProductNumber',  # 物料号（M463501B834格式）
        'DrawingProductNumber_Value': 'CFN',  # 与SAP匹配的CFN（36136000-02格式）
        'Step_Name': 'OperationDesc',
        'LogicalFlowPath': 'LogicalFlowPath',  # Group 字段保留原名
        'ERPProdSupervisor': 'VSM',
        'VSM': 'VSM',
        'Step_In_PrimaryQuantity': 'StepInQuantity',
        'TrackOut_PrimaryQuantity': 'TrackOutQuantity',
        'TrackOut_User': 'TrackOutOperator',  # 报工人
        'Step_Duration_Minute': 'StepDuration',
        'ProductionOrder': 'ProductionOrder',
        # 旧版列名兼容
        'Batch Number': 'BatchNumber',
        'batch_number': 'BatchNumber',
        'operation': 'Operation',
        'machine': 'Machine',
        'Enter Step Time': 'EnterStepTime',
        'Track In Time': 'TrackInTime',
        'Track Out Time': 'TrackOutTime',
        'Product Code': 'Plant',
        'ProductCode': 'Plant',
        'Product Name': 'Product_Desc',
        'Product Group': 'CFN',
        'Production Order': 'ProductionOrder',
        'Operation description': 'OperationDesc',
        'OperationDescription': 'OperationDesc',
        'Step In Quantity': 'StepInQuantity',
        'Track Out Quantity': 'TrackOutQuantity',
        'Setup Time (h)': 'SetupTime',
        'EH_machine(s)': 'EH_machine',
        'EH_labor(s)': 'EH_labor',
    }
    
    # 重命名存在的列
    rename_dict = {k: v for k, v in column_mapping.items() if k in result.columns}
    if rename_dict:
        result = result.rename(columns=rename_dict)

    if 'BatchNumber' in result.columns:
        bn = result['BatchNumber'].astype('string').str.strip()
        keep_mask = ~bn.str.contains(r"-0\d+$", na=False, regex=True)
        result = result.loc[keep_mask].copy()
        result['BatchNumber'] = bn.loc[keep_mask]
    
    # 时间字段转换
    time_columns = ['EnterStepTime', 'TrackInTime', 'TrackOutTime']
    for col in time_columns:
        if col in result.columns:
            result[col] = pd.to_datetime(result[col], errors='coerce')
    
    # Operation 字段标准化：转为可空整数（兼容 '0010' / 10.0 / '10.0' 等）
    if 'Operation' in result.columns:
        op_num = pd.to_numeric(result['Operation'], errors='coerce')
        op_num = op_num.where(op_num.isna() | (op_num == op_num.round()))
        result['Operation'] = op_num.round().astype('Int64')

        op_s = result['Operation'].astype('string')
        op_s = op_s.where(op_s.isna(), op_s.str.replace(r"\.0$", "", regex=True))
        result['Operation'] = op_s

    # Plant 字段标准化：转为可空整数（兼容 '4026' / 4026.0 / '4026.0' 等）
    if 'Plant' in result.columns:
        plant_num = pd.to_numeric(result['Plant'], errors='coerce')
        plant_num = plant_num.where(plant_num.isna() | (plant_num == plant_num.round()))
        result['Plant'] = plant_num.round().astype('Int64')

        plant_s = result['Plant'].astype('string')
        plant_s = plant_s.where(plant_s.isna(), plant_s.str.replace(r"\.0$", "", regex=True))
        result['Plant'] = plant_s

    # ProductionOrder 字段标准化：可空整数
    if 'ProductionOrder' in result.columns:
        po = result['ProductionOrder']
        po = po.apply(lambda x: None if pd.isna(x) else str(x).strip())
        po = po.replace({'': None})
        po_num = pd.to_numeric(po, errors='coerce')
        # only keep values that are mathematically integers (e.g. 12345, 12345.0, 1.2345E4)
        po_num = po_num.where(po_num.isna() | (po_num == po_num.round()))
        result['ProductionOrder'] = po_num.round().astype('Int64')
    
    # 数量字段标准化：支持数值字符串，空值/非法值转 NaN（后续写入 SQL Server 会变成 NULL）
    float_columns = ['StepInQuantity', 'TrackOutQuantity']
    for col in float_columns:
        if col in result.columns:
            result[col] = pd.to_numeric(result[col], errors='coerce')
    
    # OperationDesc 字段标准化（清洗工序名称）
    if 'OperationDesc' in result.columns:
        result['OperationDesc'] = result['OperationDesc'].apply(standardize_operation_name)

    # 从 LogicalFlowPath 提取 Group 编号（用于与 SAP 匹配）
    # 例如: "CZM 50258250/0010 CZM 纵切车/CZM 纵切车" -> "50258250"
    if 'LogicalFlowPath' in result.columns:
        import re
        def extract_group_number(group_str):
            if pd.isna(group_str) or group_str is None:
                return None
            group_str = str(group_str).strip()
            if not group_str:
                return None
            # 提取所有数字序列，返回最长的（通常是 Group 编号）
            numbers = re.findall(r'\d+', group_str)
            if not numbers:
                return None
            return max(numbers, key=len) if len(numbers) > 1 else numbers[0]
        
        result['Group'] = result['LogicalFlowPath'].apply(extract_group_number)

    if 'Group' in result.columns:
        group_num = pd.to_numeric(result['Group'], errors='coerce')
        group_num = group_num.where(group_num.isna() | (group_num == group_num.round()))
        result['Group'] = group_num.round().astype('Int64')
    
    # 生成 record_hash
    key_fields = ['BatchNumber', 'Operation', 'Machine', 'TrackOutTime']
    available_keys = [k for k in key_fields if k in result.columns]
    
    if available_keys:
        hash_df = result[available_keys].copy()
        for c in hash_df.columns:
            hash_df[c] = hash_df[c].astype('string').fillna('')
        record_hash = hash_df.iloc[:, 0]
        for col in hash_df.columns[1:]:
            record_hash = record_hash + '|' + hash_df[col]
        result['record_hash'] = record_hash
    
    # 添加时间戳字段
    from datetime import datetime
    result['created_at'] = datetime.now()
    result['updated_at'] = datetime.now()
    
    # 选择需要的列（统一命名）
    target_columns = [
        'BatchNumber', 'Operation', 'Machine',
        'EnterStepTime', 'TrackInTime', 'TrackOutTime',
        'Plant', 'Product_Desc', 'ProductNumber', 'CFN', 'ProductionOrder',
        'OperationDesc', 'Group',
        'StepInQuantity', 'TrackOutQuantity',
        'TrackOutOperator',
        'VSM',
        'factory_name',
        'source_file', 'record_hash',
        'created_at', 'updated_at'
    ]
    
    # 只保留存在的列
    existing_columns = [c for c in target_columns if c in result.columns]
    result = result[existing_columns]
    
    logging.info(f"清洗完成: {len(result)} 行, {len(existing_columns)} 列")
    
    return result


def save_to_database(df: pd.DataFrame, table_name: str = "raw_mes") -> Dict[str, int]:
    """
    保存数据到数据库
    
    Args:
        df: 清洗后的 DataFrame
        table_name: 目标表名
        
    Returns:
        统计信息
    """
    if df.empty:
        return {"inserted": 0, "updated": 0, "skipped": 0}
    
    db = get_db_manager()
    
    # 1. 先对 DataFrame 内部去重
    original_count = len(df)
    df = df.drop_duplicates(subset=['record_hash'], keep='first')
    internal_dups = original_count - len(df)
    if internal_dups > 0:
        logging.info(f"内部去重: 移除 {internal_dups} 条重复记录")

    # 2. 数据库层面去重：只插入 record_hash 不存在的记录
    # NOTE: shared_infrastructure.utils.db_sqlserver_only.merge_insert_by_hash
    # has been optimized to use INSERT...WHERE NOT EXISTS (no MERGE) for SQL Server.
    inserted = db.merge_insert_by_hash(df, table_name, hash_column="record_hash")
    db_dups = len(df) - inserted
    if db_dups > 0:
        logging.info(f"数据库去重: 跳过 {db_dups} 条已存在记录")

    skipped = internal_dups + db_dups
    return {"inserted": inserted, "updated": 0, "skipped": skipped}


def main(
    test_mode: bool = False,
    max_files: int = 3,
    max_rows: int = 1000,
    max_rows_per_file: int = 0,
    force: bool = False,
    only_year: Optional[str] = None,
    only_factory: Optional[str] = None,
    only_file: Optional[str] = None,
    refresh: bool = False,
):
    """主函数"""
    logging.info("=" * 60)
    logging.info("MES 批次产出原始数据 ETL (V2) 启动")
    if test_mode:
        logging.info(f"*** 测试模式：每工厂最多 {max_files} 个文件，每文件最多 {max_rows} 行 ***")
    logging.info("=" * 60)
    
    t0 = time.time()
    
    try:
        # 1. 初始化数据库
        init_database()
        
        # 2. 加载配置
        cfg = load_config(CONFIG_PATH)
        if not cfg:
            logging.error("配置加载失败")
            return

        factory_name_from_plant = bool(cfg.get("runtime", {}).get("factory_name_from_plant", True))
        
        # 3. 获取待处理文件清单（逐文件处理）
        tasks = get_mes_file_tasks(
            cfg,
            test_mode=test_mode,
            max_files=max_files,
            force=force,
            only_year=only_year,
            only_factory=only_factory,
            only_file=only_file,
        )

        if not tasks:
            logging.info("没有新数据需要处理")
            return

        # 用于跨文件去重（仅本次运行内）
        seen_hashes = set()

        total_read = 0
        total_inserted = 0
        total_skipped = 0

        for t in tasks:
            factory_id = t["factory_id"]
            factory_name = t["factory_name"]
            file_path = t["file_path"]

            if refresh:
                deleted = delete_existing_for_source_file(file_path, "raw_mes")
                logging.info(f"刷新模式：已删除 raw_mes 中 source_file={os.path.basename(file_path)} 的 {deleted} 行")

            logging.info(f"读取: {os.path.basename(file_path)}")
            rows_limit = max_rows if test_mode else (max_rows_per_file if max_rows_per_file and max_rows_per_file > 0 else None)
            df = read_sharepoint_excel(file_path, max_rows=rows_limit)

            if df.empty:
                logging.warning(f"文件无数据，跳过: {file_path}")
                continue

            df["factory_source"] = factory_id
            df["factory_name"] = factory_name
            df["source_file"] = file_path

            total_read += len(df)
            logging.info(f"  成功读取 {len(df)} 行")

            df_clean = clean_mes_data(df)
            if df_clean.empty:
                logging.warning(f"清洗后无有效数据，跳过: {file_path}")
                continue

            # factory_name: replace the prefix (e.g. 工厂1) with per-row Plant value
            if factory_name_from_plant and "Plant" in df_clean.columns:
                plant_s = df_clean["Plant"].where(df_clean["Plant"].notna(), None)
                plant_s = plant_s.apply(lambda x: None if x is None else str(x).strip())
                plant_s = plant_s.apply(lambda x: None if (x is None or x == '' or x.lower() == 'nan') else x)
                if "factory_name" in df_clean.columns:
                    df_clean.loc[plant_s.notna(), "factory_name"] = plant_s[plant_s.notna()] + f"-{factory_id}"
                else:
                    df_clean["factory_name"] = plant_s.where(plant_s.notna(), None)
                    df_clean.loc[plant_s.notna(), "factory_name"] = df_clean.loc[plant_s.notna(), "factory_name"] + f"-{factory_id}"

            if "record_hash" in df_clean.columns:
                before = len(df_clean)
                df_clean = df_clean[~df_clean["record_hash"].isin(seen_hashes)]
                for h in df_clean["record_hash"].dropna().astype(str).tolist():
                    seen_hashes.add(h)
                removed = before - len(df_clean)
                if removed > 0:
                    logging.info(f"  本次运行跨文件去重: 移除 {removed} 条")

            stats = save_to_database(df_clean, "raw_mes")
            total_inserted += stats["inserted"]
            total_skipped += stats["skipped"]

            # 处理成功后标记文件已处理（用于增量）
            db = get_db_manager()
            db.mark_file_processed(f"mes_raw_{factory_id}", file_path)

        logging.info(f"写入完成: 读取 {total_read} 行, 插入 {total_inserted} 条, 跳过 {total_skipped} 条")
        
        # 查询数据库当前记录数
        db = get_db_manager()
        with db.get_connection() as conn:
            cursor = conn.cursor()
            total_count = cursor.execute("SELECT COUNT(*) FROM dbo.raw_mes").fetchone()[0]
            logging.info(f"数据库 raw_mes 表当前记录数: {total_count}")
        
        logging.info(f"处理完成，耗时: {time.time() - t0:.2f} 秒")
        
    except Exception as e:
        logging.error(f"ETL 失败: {e}")
        raise


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='MES 批次产出原始数据 ETL')
    parser.add_argument('--test', action='store_true', help='测试模式，只处理部分文件')
    parser.add_argument('--max-files', type=int, default=3, help='测试模式下每工厂最大文件数')
    parser.add_argument('--max-rows', type=int, default=1000, help='测试模式下每文件最大行数')
    parser.add_argument('--max-rows-per-file', type=int, default=0, help='非测试模式：每个文件最多读取行数（0=不限制，用于快速验证）')
    parser.add_argument('--force', action='store_true', help='强制处理文件（忽略增量状态）')
    parser.add_argument('--only-year', type=str, default=None, help='仅处理指定年份文件（例如 2025）')
    parser.add_argument('--only-factory', type=str, default=None, help='仅处理指定工厂（CZM/CKH）')
    parser.add_argument('--only-file', type=str, default=None, help='仅处理文件名包含该字符串的文件（例如 CMES_Product_Output_CZM_202601）')
    parser.add_argument('--refresh', action='store_true', help='刷新模式：导入前先删除该文件对应的历史数据（按 source_file 精确匹配）')
    args = parser.parse_args()
    
    main(
        test_mode=args.test,
        max_files=args.max_files,
        max_rows=args.max_rows,
        max_rows_per_file=args.max_rows_per_file,
        force=args.force,
        only_year=args.only_year,
        only_factory=args.only_factory,
        only_file=args.only_file,
        refresh=args.refresh,
    )
