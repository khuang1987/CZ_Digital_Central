import pyodbc

conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=localhost\SQLEXPRESS;"
    "DATABASE=mddap_v2;"
    "Trusted_Connection=yes;"
    "Encrypt=no;"
)

with pyodbc.connect(conn_str) as conn:
    conn.timeout = 30
    cur = conn.cursor()
    cur.execute("SET LOCK_TIMEOUT 5000")
    cur.execute("SET DEADLOCK_PRIORITY LOW")

    LT_COL = "[LT(d)]"
    PT_COL = "[PT(d)]"
    ST_COL = "[ST(d)]"
    SAMPLE_N = 100
    
    print("=" * 80)
    print("v_mes_metrics 视图数据质量检查")
    print("=" * 80)
    
    # 1. 视图总记录数
    cur.execute("SELECT COUNT(*) FROM dbo.raw_mes")
    total_est = cur.fetchone()[0]
    total = None
    print(f"\n视图总记录数(估算=raw_mes): {total_est:,}")

    try:
        cur.execute("SELECT TOP 1 1 FROM dbo.v_mes_metrics")
        ok = cur.fetchone() is not None
        print(f"视图可查询(Top1): {ok}")
    except Exception as e:
        print(f"视图可查询(Top1)失败: {e}")

    try:
        cur.execute("""
            SELECT TOP 5
                machine,
                TrackOutTime,
                factory_name
            FROM dbo.v_mes_metrics
            WHERE TrackOutTime IS NOT NULL
            ORDER BY TrackOutTime DESC
        """)
        rows = cur.fetchall()
        print(f"\n样本数据行数(Top5): {len(rows)}")
        for r in rows:
            print(r)
    except Exception as e:
        print(f"样本数据查询失败: {e}")
    
    # 2. Routing 匹配完成度（有 unit_time 的记录）
    print("\n" + "=" * 80)
    print("Routing 匹配完成度")
    print("=" * 80)

    matched_rate = None

    try:
        cur.execute(
            f"""
            WITH sample_ids AS (
                SELECT TOP {SAMPLE_N} id
                FROM dbo.raw_mes
                ORDER BY id DESC
            ),
            s AS (
                SELECT
                    v.unit_time,
                    v.{ST_COL} AS ST_d
                FROM dbo.v_mes_metrics v
                INNER JOIN sample_ids si ON v.id = si.id
            )
            SELECT
                COUNT(*) as total,
                COUNT(CASE WHEN unit_time IS NOT NULL THEN 1 END) as has_unit_time,
                COUNT(CASE WHEN ST_d IS NOT NULL THEN 1 END) as has_st_d
            FROM s
            """
        )
        row = cur.fetchone()

        if total is None:
            total = row[0]

        matched_count = row[1]
        matched_rate = matched_count / total * 100 if total and total > 0 else 0

        print(f"\n有 unit_time（单件时间）的记录: {matched_count:,} / {total:,}")
        print(f"Routing 匹配率: {matched_rate:.2f}%")
        print(f"有 ST_d（标准时间天数）的记录: {row[2]:,} / {total:,} ({row[2]/total*100:.2f}%)")
    except Exception as e:
        print(f"Routing 匹配完成度检查失败(可能超时): {e}")
    
    # 3. CompletionStatus 分布
    print("\n" + "=" * 80)
    print("CompletionStatus 分布")
    print("=" * 80)

    try:
        cur.execute(
            f"""
            WITH sample_ids AS (
                SELECT TOP {SAMPLE_N} id
                FROM dbo.raw_mes
                ORDER BY id DESC
            )
            SELECT
                v.CompletionStatus,
                COUNT(*) as cnt,
                CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS DECIMAL(5,2)) as pct
            FROM dbo.v_mes_metrics v
            INNER JOIN sample_ids si ON v.id = si.id
            GROUP BY v.CompletionStatus
            ORDER BY cnt DESC
            """
        )

        print("\nStatus                  记录数          占比")
        print("-" * 60)
        for row in cur.fetchall():
            status = row[0] if row[0] else 'NULL'
            print(f"{status:20s} {row[1]:10,}      {row[2]:6.2f}%")
    except Exception as e:
        print(f"CompletionStatus 检查失败(可能超时): {e}")
    
    # 4. 关键指标字段完整性
    print("\n" + "=" * 80)
    print("关键指标字段完整性")
    print("=" * 80)
    
    key_metrics = [LT_COL, PT_COL, ST_COL, 'unit_time']
    
    for metric in key_metrics:
        try:
            metric_sql = metric if metric.startswith('[') else f"[{metric}]"
            cur.execute(
                f"""
                WITH sample_ids AS (
                    SELECT TOP {SAMPLE_N} id
                    FROM dbo.raw_mes
                    ORDER BY id DESC
                ),
                s AS (
                    SELECT
                        v.{metric_sql} AS metric_value
                    FROM dbo.v_mes_metrics v
                    INNER JOIN sample_ids si ON v.id = si.id
                )
                SELECT
                    COUNT(*) as total,
                    COUNT(CASE WHEN metric_value IS NOT NULL THEN 1 END) as has_value,
                    AVG(CASE WHEN metric_value IS NOT NULL THEN CAST(metric_value AS FLOAT) END) as avg_value
                FROM s
                """
            )
            row = cur.fetchone()
            has_value = row[1]
            pct = has_value / row[0] * 100 if row[0] > 0 else 0
            avg_val = row[2] if row[2] is not None else 0

            print(f"\n{metric:15s}: {has_value:8,} / {row[0]:8,} ({pct:5.2f}%)")
            if avg_val > 0:
                print(f"                 平均值: {avg_val:.4f}")
        except Exception as e:
            print(f"\n{metric:15s}: 检查失败(可能超时): {e}")
    
    # 5. 按年份和工厂统计匹配率
    print("\n" + "=" * 80)
    print("按年份统计 Routing 匹配率")
    print("=" * 80)

    try:
        cur.execute(
            f"""
            WITH sample_ids AS (
                SELECT TOP {SAMPLE_N} id
                FROM dbo.raw_mes
                ORDER BY id DESC
            )
            SELECT
                YEAR(v.TrackOutTime) as year,
                COUNT(*) as total,
                COUNT(CASE WHEN v.unit_time IS NOT NULL THEN 1 END) as matched,
                CAST(COUNT(CASE WHEN v.unit_time IS NOT NULL THEN 1 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) as match_rate
            FROM dbo.v_mes_metrics v
            INNER JOIN sample_ids si ON v.id = si.id
            WHERE v.TrackOutTime IS NOT NULL
            GROUP BY YEAR(v.TrackOutTime)
            ORDER BY year
            """
        )

        print("\n年份    总记录数      匹配数      匹配率")
        print("-" * 60)
        for row in cur.fetchall():
            print(f"{row[0]:4d}  {row[1]:10,}  {row[2]:10,}    {row[3]:6.2f}%")
    except Exception as e:
        print(f"按年份统计失败(可能超时): {e}")
    
    print("\n" + "=" * 80)
    print("按工厂统计 Routing 匹配率")
    print("=" * 80)

    try:
        cur.execute(
            f"""
            WITH sample_ids AS (
                SELECT TOP {SAMPLE_N} id
                FROM dbo.raw_mes
                ORDER BY id DESC
            )
            SELECT
                v.factory_name,
                COUNT(*) as total,
                COUNT(CASE WHEN v.unit_time IS NOT NULL THEN 1 END) as matched,
                CAST(COUNT(CASE WHEN v.unit_time IS NOT NULL THEN 1 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) as match_rate
            FROM dbo.v_mes_metrics v
            INNER JOIN sample_ids si ON v.id = si.id
            GROUP BY v.factory_name
            ORDER BY v.factory_name
            """
        )

        print("\n工厂            总记录数      匹配数      匹配率")
        print("-" * 60)
        for row in cur.fetchall():
            print(f"{row[0]:15s} {row[1]:10,}  {row[2]:10,}    {row[3]:6.2f}%")
    except Exception as e:
        print(f"按工厂统计失败(可能超时): {e}")
    
    # 6. 样本数据展示
    print("\n" + "=" * 80)
    print("样本数据（前5条有完整计算的记录）")
    print("=" * 80)

    try:
        cur.execute(
            f"""
            WITH sample_ids AS (
                SELECT TOP {SAMPLE_N} id
                FROM dbo.raw_mes
                ORDER BY id DESC
            )
            SELECT TOP 5
                v.BatchNumber, v.Operation, v.CFN, v.TrackOutQuantity,
                v.{LT_COL} AS LT_d,
                v.{PT_COL} AS PT_d,
                v.{ST_COL} AS ST_d,
                v.unit_time, v.CompletionStatus
            FROM dbo.v_mes_metrics v
            INNER JOIN sample_ids si ON v.id = si.id
            WHERE v.unit_time IS NOT NULL
                AND v.{LT_COL} IS NOT NULL
                AND v.{ST_COL} IS NOT NULL
            ORDER BY v.id
            """
        )

        print("\n")
        for row in cur.fetchall():
            print(f"批次: {row[0]}, Op: {row[1]}, CFN: {row[2]}, Qty: {row[3]}")
            print(f"  LT(d)={row[4]:.2f}, PT(d)={row[5] if row[5] else 'N/A'}, ST(d)={row[6]:.2f}, unit_time={row[7]:.2f}min")
            print(f"  Status: {row[8]}")
            print()
    except Exception as e:
        print(f"样本完整计算记录查询失败(可能超时): {e}")
    
    print("=" * 80)
    if matched_rate is None:
        print("⚠️ Routing 匹配率：未计算（检查超时/失败），请在数据库空闲时重试或加索引优化")
    elif matched_rate >= 70:
        print(f"✓ Routing 匹配率 {matched_rate:.2f}% >= 70%，数据质量良好")
    elif matched_rate >= 60:
        print(f"⚠️ Routing 匹配率 {matched_rate:.2f}% 在 60-70% 之间，可以使用但建议优化")
    else:
        print(f"❌ Routing 匹配率 {matched_rate:.2f}% < 60%，建议检查匹配逻辑")
    print("=" * 80)
