"""
导入工序名称映射表
用于规范化 Routing 和 SFC 中的工序名称
"""
import pandas as pd
from pathlib import Path
import logging
import sys
import re

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from shared_infrastructure.utils.db_sqlserver_only import SQLServerOnlyManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SOURCE_FILE = Path(r'C:\Users\huangk14\OneDrive - Medtronic PLC\CZ Production - 文档\General\POWER BI 数据源 V2\10-基础数据\工序标准分类.xlsx')
# Cleaning rules now in dimension/config directory
CLEANING_RULES_CSV = Path(__file__).parent.parent / 'config' / 'operation_cleaning_rules.csv'


def get_db_manager() -> SQLServerOnlyManager:
    return SQLServerOnlyManager(
        sql_server=r"localhost\SQLEXPRESS",
        sql_db="mddap_v2",
        driver="ODBC Driver 17 for SQL Server",
    )


def ensure_tables(db: SQLServerOnlyManager):
    """确保工序映射相关表在 SQL Server 中存在"""
    with db.get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            IF OBJECT_ID('dbo.dim_operation_mapping', 'U') IS NULL
            BEGIN
                CREATE TABLE dbo.dim_operation_mapping (
                    id int IDENTITY(1,1) PRIMARY KEY,
                    operation_name nvarchar(255) NOT NULL,
                    display_name nvarchar(255) NULL,
                    standard_routing nvarchar(255) NULL,
                    area nvarchar(255) NULL,
                    lead_time float NULL,
                    erp_code nvarchar(64) NULL,
                    created_at datetime NOT NULL DEFAULT GETDATE()
                );
            END
            """
        )
        conn.commit()


def import_cleaning_rules(conn):
    """导入工序名称清洗规则"""
    if not CLEANING_RULES_CSV.exists():
        logger.warning(f'清洗规则文件不存在: {CLEANING_RULES_CSV}')
        return 0
        
    df = pd.read_csv(CLEANING_RULES_CSV)
    
    # 清洗数据
    df = df[df['Step_Name'].notna() & df['Cleaned_Operation'].notna()]
    df['Step_Name'] = df['Step_Name'].astype(str).str.strip()
    df['Cleaned_Operation'] = df['Cleaned_Operation'].astype(str).str.strip()
    
    cursor = conn.cursor()
    for _, row in df.iterrows():
        cursor.execute('''
            INSERT INTO dbo.dim_operation_cleaning_rule (original_operation, cleaned_operation)
            VALUES (?, ?)
        ''', (row['Step_Name'], row['Cleaned_Operation']))
    
    conn.commit()
    logger.info(f'导入工序清洗规则: {len(df)} 条')
    return len(df)


def import_operation_mapping(conn):
    """导入工序名称映射"""
    df = pd.read_excel(SOURCE_FILE, sheet_name='工序名称')
    
    # 重命名列为英文
    df.columns = ['no', 'operation_name', 'standard_routing', 'area', 'lead_time', 'erp_code']
    
    # --- 加载清洗规则 ---
    cleaning_map = {}
    if CLEANING_RULES_CSV.exists():
        try:
            rules_df = pd.read_csv(CLEANING_RULES_CSV)
            # Filter valid rules
            rules_df = rules_df[rules_df['Step_Name'].notna() & rules_df['Cleaned_Operation'].notna()]
            # Create dict: {original: cleaned}
            cleaning_map = dict(zip(rules_df['Step_Name'].astype(str).str.strip(), 
                                  rules_df['Cleaned_Operation'].astype(str).str.strip()))
            logger.info(f"Loaded {len(cleaning_map)} cleaning rules.")
        except Exception as e:
            logger.error(f"Failed to load cleaning rules: {e}")

    
    # 清洗数据
    df = df[df['operation_name'].notna()]
    df['operation_name'] = df['operation_name'].astype(str).str.strip()
    df['standard_routing'] = df['standard_routing'].astype(str).str.strip()
    df['area'] = df['area'].astype(str).str.strip()
    df['erp_code'] = df['erp_code'].astype(str).str.strip()
    
    
    cursor = conn.cursor()
    for _, row in df.iterrows():
        op_name = row['operation_name']
        
        # 1. Priority: Dictionary Lookup (Manually defined rules)
        # If the name exists in the CSV map, use that result; otherwise use original
        display_name = cleaning_map.get(op_name, op_name)

        # 2. Universal Polish: Auto-Regex Cleaning (Pattern based)
        # Apply this to the RESULT of the lookup to ensure even dictionary entries are stripped of unwanted tags.
        # Regex explanation:
        # [\(（] : Match opening bracket (English or Chinese)
        # (?: ... ) : Non-capturing group for OR logic
        # 外协|OEM|可外协 : Keywords to identify outsourced ops
        # .*? : Match anything lazy until closing bracket
        # [\)）] : Match closing bracket
        display_name = re.sub(r'[\(（](?:外协|OEM|可外协)[^\)）]*[\)）]', '', display_name).strip()
             
        # Also clean common copy-paste artifacts if needed, e.g. "WI-..." suffix if strictly requested,
        # but strictly speaking user only asked for Outsourced merging.
        # Keeping it safe: Only strip the Outsourced tags.
        
        cursor.execute('''
            INSERT INTO dbo.dim_operation_mapping (operation_name, display_name, standard_routing, area, lead_time, erp_code)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (op_name, display_name, row['standard_routing'], row['area'], row['lead_time'], row['erp_code']))
    
    conn.commit()
    logger.info(f'导入工序名称映射: {len(df)} 条')
    return len(df)


def import_workcenter_mapping(conn):
    """导入工作中心映射"""
    df = pd.read_excel(SOURCE_FILE, sheet_name='工作中心对应')
    
    # 重命名列为英文
    df.columns = ['work_center', 'cost_center', 'area']
    
    # 清洗数据
    df = df[df['work_center'].notna()]
    df['work_center'] = df['work_center'].astype(str).str.strip()
    df['cost_center'] = df['cost_center'].astype(str).str.strip()
    df['area'] = df['area'].astype(str).str.strip()
    
    cursor = conn.cursor()
    for _, row in df.iterrows():
        cursor.execute('''
            INSERT INTO dbo.dim_workcenter_mapping (work_center, cost_center, area)
            VALUES (?, ?, ?)
        ''', (row['work_center'], row['cost_center'], row['area']))
    
    conn.commit()
    logger.info(f'导入工作中心映射: {len(df)} 条')
    return len(df)


def import_area_mapping(conn):
    """导入区域映射"""
    df = pd.read_excel(SOURCE_FILE, sheet_name='区域')
    
    # 重命名列为英文
    df.columns = ['area', 'factory']
    
    # 清洗数据
    df = df[df['area'].notna()]
    df['area'] = df['area'].astype(str).str.strip()
    df['factory'] = df['factory'].astype(str).str.strip()
    
    cursor = conn.cursor()
    for _, row in df.iterrows():
        cursor.execute('''
            INSERT INTO dbo.dim_area_mapping (area, factory)
            VALUES (?, ?)
        ''', (row['area'], row['factory']))
    
    conn.commit()
    logger.info(f'导入区域映射: {len(df)} 条')
    return len(df)


def main():
    """主函数"""
    import argparse
    parser = argparse.ArgumentParser(description='Import Operation Mapping')
    parser.add_argument('--force', action='store_true', help='Force refresh even if file is unchanged')
    args = parser.parse_args()

    logger.info('开始导入工序分类数据...')

    db = get_db_manager()
    
    # --- 优化：检测文件是否有变化 ---
    if not args.force:
        changed = db.filter_changed_files("dim_operation_mapping", [SOURCE_FILE])
        if not changed:
            logger.info("工序标准分类文件未变化，跳过导入。")
            return
    else:
        logger.info("强制刷新模式")

    # FIX: Drop table to ensure schema update (adding display_name)
    with db.get_connection() as conn:
        try:
            conn.cursor().execute("DROP TABLE IF EXISTS dbo.dim_operation_mapping")
            conn.commit()
            logger.info("Dropped dim_operation_mapping for schema update.")
        except Exception as e:
            logger.warning(f"Drop table failed: {e}")

    ensure_tables(db)

    with db.get_connection() as conn:
        cur = conn.cursor()
        for t in [
            "dbo.dim_operation_mapping",
        ]:
            try:
                cur.execute(f"TRUNCATE TABLE {t}")
            except Exception:
                cur.execute(f"DELETE FROM {t}")
        conn.commit()
    
    import_count = import_operation_mapping(conn)
    op_count = import_count # Update counter
    
    # Remove the duplicate inline logic
    # df_op = pd.read_excel(SOURCE_FILE, sheet_name='工序名称') ... db.bulk_insert(...)
    # The previous code lines 184-193 were doing bulk insert.
    # My Chunk 3 modified 'import_operation_mapping'. 
    # So I will replace lines 184-193 with just calling that function.

    
    # 记录处理状态
    db.mark_file_processed("dim_operation_mapping", SOURCE_FILE)
    
    print('\n导入结果:')
    print(f'  dim_operation_mapping: {op_count} 条')
    logger.info('工序分类数据导入完成!')


if __name__ == '__main__':
    main()
