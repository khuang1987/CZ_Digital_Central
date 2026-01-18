"""
调试MES脚本导入问题
"""

import sys
import os
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def debug_mes_import():
    """调试MES脚本导入"""
    try:
        # 导入MES批量报告脚本
        sys.path.insert(0, str(project_root / "data_pipelines" / "sources" / "mes" / "etl"))
        import etl_dataclean_mes_batch_report
        
        print("=== MES脚本导入调试 ===")
        print(f"脚本文件位置: {etl_dataclean_mes_batch_report.__file__}")
        print(f"BASE_DIR: {etl_dataclean_mes_batch_report.BASE_DIR}")
        print(f"CONFIG_PATH: {etl_dataclean_mes_batch_report.CONFIG_PATH}")
        print()
        
        # 检查预期文件是否存在
        expected_file = project_root / "data_pipelines" / "sources" / "mes" / "etl" / "etl_dataclean_mes_batch_report.py"
        print(f"预期文件位置: {expected_file}")
        print(f"预期文件是否存在: {expected_file.exists()}")
        print()
        
        # 检查sys.path顺序
        print("=== sys.path 顺序 ===")
        for i, path in enumerate(sys.path[:5]):
            print(f"{i}: {path}")
        
    except Exception as e:
        print(f"调试失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_mes_import()
