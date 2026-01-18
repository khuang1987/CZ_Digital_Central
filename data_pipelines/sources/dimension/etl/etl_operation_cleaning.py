"""
工序名称清洗规则生成与导入
- 从MES和SAP工时表中提取所有工序名称
- 应用清洗规则生成标准化工序名称
- 保留外协分类标识
"""
import pandas as pd
from pathlib import Path
import logging
import re

from shared_infrastructure.utils.db_sqlserver_only import SQLServerOnlyManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 路径配置
CONFIG_DIR = Path(__file__).parent.parent / 'config'
CLEANING_RULES_CSV = CONFIG_DIR / 'operation_cleaning_rules.csv'


def get_db_manager() -> SQLServerOnlyManager:
    return SQLServerOnlyManager(
        sql_server=r"localhost\SQLEXPRESS",
        sql_db="mddap_v2",
        driver="ODBC Driver 17 for SQL Server",
    )


def ensure_sqlserver_tables(db: SQLServerOnlyManager) -> None:
    with db.get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            IF OBJECT_ID('dbo.dim_operation_cleaning_rule', 'U') IS NULL
            BEGIN
                CREATE TABLE dbo.dim_operation_cleaning_rule (
                    id int IDENTITY(1,1) PRIMARY KEY,
                    original_operation nvarchar(255) NOT NULL,
                    cleaned_operation nvarchar(255) NOT NULL,
                    created_at datetime NOT NULL DEFAULT GETDATE()
                );
            END
            """
        )
        conn.commit()


def clean_operation_name(name):
    """
    清洗工序名称规则:
    1. 移除工厂前缀 (CZM, CKH)
    2. 移除文档编号 (WI-, PII-, PS-, etc.)
    3. 保留外协标识并归类
    4. 标准化同义词 (e.g., 车(数) -> 数控车)
    5. 保留检验后缀
    """
    if not isinstance(name, str):
        return None, False
        
    original = name
    name = name.strip()
    
    # 检测是否为外协工序
    is_outsourcing = False
    outsourcing_patterns = [
        r'[\(（][^\)）]*(?:外协|OEM|可外协)[^\)）]*[\)）]',  # (外协), (可外协), (OEM)
        r'[-_\s]外协[-_\s]?',  # -外协-, _外协_
        r'^外协[-_]',  # 外协-xxx
        r'[-_]外协$',  # xxx-外协
        r'外协$',  # xxx外协
        r'[\(（]外$',  # 镀铬(外 - incomplete bracket
    ]
    for pattern in outsourcing_patterns:
        if re.search(pattern, name, re.IGNORECASE):
            is_outsourcing = True
            break
    
    # 1. 移除工厂前缀
    name = re.sub(r'^(CZM|CKH)\s+', '', name, flags=re.IGNORECASE)
    
    # 2. 移除文档编号
    name = re.sub(r'\s*(?:WI|PII|PS|DMR|TM|QP|COP)[-:\s]+[A-Za-z0-9\-\(\)\.]+', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\s+(?:WI|PII|PS)[-0-9A-Z]+$', '', name, flags=re.IGNORECASE)

    # 3. 移除外协标识（稍后会添加回来）
    name = re.sub(r'[\(（][^\)）]*(?:外协|OEM|可外协)[^\)）]*[\)）]', '', name)
    name = re.sub(r'^外协[-_]', '', name)
    name = re.sub(r'[-_]?外协$', '', name)
    name = re.sub(r'[-_\s]?外协[-_\s]?(?=检验)', '', name)
    name = re.sub(r'[\(（]外$', '', name)  # 处理不完整的括号
    
    # 4. 标准化同义词
    replacements = [
        (r'车[\(（]数[\)）]', '数控车'),
        (r'车[\(（]中心[\)）]', '车削中心'),
        (r'车[\(（]普[\)）]', '普通车削'),
        (r'车[\(（]纵切[\)）]', '纵切车'),
        (r'铣[\(（]中心[\)）]', '铣削中心'),
        (r'铣[\(（]普[\)）]', '普通铣削'),
        (r'铣[\(（]数[\)）]', '数控铣'),
        (r'钻[\(（]深孔[\)）]', '深孔钻'),
        (r'磨[\(（]五轴[\)）]', '五轴磨'),
        (r'磨[\(（]无心[\)）]', '无心磨'),
        (r'磨[\(（]工具[\)）]', '工具磨'),
        (r'磨[\(（]平面[\)）]', '平面磨'),
        (r'焊接[\(（]氩弧焊[\)）]', '氩弧焊'),
        (r'焊接[\(（]激光焊[\)）]', '激光焊接'),
        (r'热处理[\(（]真空[\)）]', '真空热处理'),
        (r'热处理[\(（]非真空[\)）]', '非真空热处理'),
        (r'^纵切$', '纵切车'),
        (r'^深孔$', '深孔钻'),
        (r'^打标$', '激光打标'),
        (r'^数车$', '数控车'),
        (r'^数铣$', '数控铣'),
        (r'^普车$', '普通车削'),
        (r'^普铣$', '普通铣削'),
        (r'^钳$', '钳工'),
        (r'^车$', '车削'),
        (r'^铣$', '数控铣'),
        (r'^磨$', '磨'),
        (r'氩弧焊接', '氩弧焊'),
        (r'激光焊$', '激光焊接'),
        (r'^热处理$', '非真空热处理'),
        (r'^末道清洗', '清洗'),
        (r'^过程清洗', '清洗'),
        (r'^酒精清洗', '清洗'),
        (r'^清洗[\(（]末道[\)）]', '清洗'),
        (r'^柠檬酸钝化', '钝化'),
        (r'^局部钝化', '钝化'),
        (r'^点钝$', '点钝化'),
        (r'^下料', '锯'),
        (r'^备料[\(（]锯断[\)）]', '锯'),
        (r'^锯床', '锯'),
    ]
    
    for pattern, repl in replacements:
        name = re.sub(pattern, repl, name)

    # 5. 清理空括号和多余空格
    name = re.sub(r'[\(（]\s*[\)）]', '', name)
    name = re.sub(r'\s+', ' ', name)
    name = name.strip()
    name = name.rstrip('-_')
    
    # 6. 如果是外协工序，添加外协标识
    if is_outsourcing and name:
        # 检查是否已经包含外协
        if '外协' not in name:
            # 检查是否是检验工序
            if name.endswith('检验'):
                base_op = name[:-2]  # 移除"检验"
                name = f'{base_op}(外协)检验'
            else:
                name = f'{name}(外协)'
    
    return name if name else None, is_outsourcing


def get_all_operations_from_db():
    """从数据库获取所有工序名称"""
    db = get_db_manager()
    conn = db.get_connection()
    
    operations = set()
    
    # 从MES表获取 (使用Operation列)
    try:
        df_mes = pd.read_sql('SELECT DISTINCT Operation FROM dbo.raw_mes WHERE Operation IS NOT NULL', conn)
        operations.update(df_mes['Operation'].dropna().astype(str).str.strip().tolist())
        logger.info(f'从raw_mes获取 {len(df_mes)} 个工序名称')
    except Exception as e:
        logger.warning(f'无法从raw_mes获取数据: {e}')
    
    # 从SAP工时表获取 (使用OperationDesc列)
    try:
        df_sap = pd.read_sql('SELECT DISTINCT OperationDesc FROM dbo.raw_sap_labor_hours WHERE OperationDesc IS NOT NULL', conn)
        operations.update(df_sap['OperationDesc'].dropna().astype(str).str.strip().tolist())
        logger.info(f'从raw_sap_labor_hours获取 {len(df_sap)} 个工序名称')
    except Exception as e:
        logger.warning(f'无法从raw_sap_labor_hours获取数据: {e}')
    
    conn.close()
    return operations


def generate_cleaning_rules():
    """生成清洗规则"""
    operations = get_all_operations_from_db()
    logger.info(f'共获取 {len(operations)} 个唯一工序名称')
    
    rules = []
    outsourcing_count = 0
    
    for op in sorted(operations):
        if not op or op.strip() == '':
            continue
        cleaned, is_outsourcing = clean_operation_name(op)
        if cleaned:
            rules.append({
                'Step_Name': op,
                'Cleaned_Operation': cleaned
            })
            if is_outsourcing:
                outsourcing_count += 1
    
    df = pd.DataFrame(rules)
    df = df.drop_duplicates(subset=['Step_Name'], keep='first')
    df = df.sort_values('Step_Name')
    
    logger.info(f'生成 {len(df)} 条清洗规则，其中外协工序 {outsourcing_count} 条')
    return df


def save_cleaning_rules(df):
    """保存清洗规则到CSV"""
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(CLEANING_RULES_CSV, index=False, encoding='utf-8-sig')
    logger.info(f'清洗规则已保存到: {CLEANING_RULES_CSV}')


def import_to_database(df):
    """导入清洗规则到数据库"""
    db = get_db_manager()
    ensure_sqlserver_tables(db)

    conn = db.get_connection()
    cursor = conn.cursor()
    
    # 清空现有规则
    cursor.execute('DELETE FROM dbo.dim_operation_cleaning_rule')
    
    # 导入新规则
    for _, row in df.iterrows():
        cursor.execute(
            """
            INSERT INTO dbo.dim_operation_cleaning_rule (original_operation, cleaned_operation)
            VALUES (?, ?)
            """,
            (row['Step_Name'], row['Cleaned_Operation']),
        )
    
    conn.commit()

    conn.close()
    logger.info(f'已导入 {len(df)} 条清洗规则到数据库')


def preview_outsourcing_rules(df):
    """预览外协相关规则"""
    outsourcing_df = df[df['Cleaned_Operation'].str.contains('外协', na=False)]
    print(f'\n外协工序规则预览 (共 {len(outsourcing_df)} 条):')
    print('-' * 80)
    for _, row in outsourcing_df.head(20).iterrows():
        print(f"  {row['Step_Name']:<50} -> {row['Cleaned_Operation']}")
    if len(outsourcing_df) > 20:
        print(f'  ... 还有 {len(outsourcing_df) - 20} 条')


def main():
    """主函数"""
    logger.info('开始生成工序清洗规则...')
    
    # 生成规则
    df = generate_cleaning_rules()
    
    # 保存到CSV
    save_cleaning_rules(df)
    
    # 导入到数据库
    import_to_database(df)
    
    # 预览外协规则
    preview_outsourcing_rules(df)
    
    # 显示示例
    print('\n清洗规则示例:')
    print('-' * 80)
    samples = [
        'CZM 五轴磨（可外协）',
        'CKH 外协-机加工',
        'CKH 外协-热处理检验',
        '真空热处理（可外协）',
        '镀铬(外协)',
        '线切割-慢丝（可外协）',
        'CKH 冲',
        'CKH 冲检验',
    ]
    for sample in samples:
        if sample in df['Step_Name'].values:
            cleaned = df[df['Step_Name'] == sample]['Cleaned_Operation'].values[0]
            print(f"  {sample:<40} -> {cleaned}")
        else:
            cleaned, _ = clean_operation_name(sample)
            print(f"  {sample:<40} -> {cleaned} (计算值)")
    
    logger.info('工序清洗规则生成完成!')


if __name__ == '__main__':
    main()
