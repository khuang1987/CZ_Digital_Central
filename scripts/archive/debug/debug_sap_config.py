"""
调试SAP脚本配置路径问题
"""

import sys
import os
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def debug_sap_config():
    """调试SAP配置路径"""
    try:
        # 导入SAP脚本
        sys.path.insert(0, str(project_root / "data_pipelines" / "sources" / "sap" / "etl"))
        import etl_dataclean_sap_routing
        
        print("=== SAP脚本配置路径调试 ===")
        print(f"SAP脚本目录: {os.path.dirname(os.path.abspath(etl_dataclean_sap_routing.__file__))}")
        print(f"CONFIG_PATH: {etl_dataclean_sap_routing.CONFIG_PATH}")
        print(f"文件是否存在: {os.path.exists(etl_dataclean_sap_routing.CONFIG_PATH)}")
        print()
        
        # 手动检查路径解析
        from shared_infrastructure.utils.path_resolver import get_path_resolver
        resolver = get_path_resolver()
        
        print("=== PathResolver配置 ===")
        print(f"项目根目录: {resolver.project_root}")
        print("原始config_paths配置:")
        for key, path in resolver.config.get("config_paths", {}).items():
            print(f"  {key}: {path}")
        print()
        
        # 手动测试路径解析
        base_dir = os.path.dirname(os.path.abspath(etl_dataclean_sap_routing.__file__))
        print(f"基础目录: {base_dir}")
        
        config_path = resolver.get_config_path("sap_routing", "sap", base_dir)
        print(f"解析后的配置路径: {config_path}")
        print(f"文件是否存在: {os.path.exists(config_path)}")
        
    except Exception as e:
        print(f"调试失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_sap_config()
