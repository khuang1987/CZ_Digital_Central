"""
调试路径解析器问题
"""

import sys
import os
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def debug_path_resolver():
    """调试路径解析器"""
    try:
        from shared_infrastructure.utils.path_resolver import get_path_resolver, get_config_path
        
        resolver = get_path_resolver()
        
        print("=== 调试路径解析器 ===")
        print(f"项目根目录: {resolver.project_root}")
        print()
        
        print("=== 配置内容 ===")
        print("config_paths:")
        for key, path in resolver.config.get("config_paths", {}).items():
            print(f"  {key}: {path}")
        print()
        
        print("=== 路径解析结果 ===")
        config_path = get_config_path("sap_routing", "sap")
        print(f"SAP配置路径: {config_path}")
        print(f"文件是否存在: {os.path.exists(config_path)}")
        print()
        
        # 手动检查预期路径
        expected_path = os.path.join(project_root, "data_pipelines", "sources", "sap", "config", "config_sap_routing.yaml")
        print(f"预期路径: {expected_path}")
        print(f"预期文件是否存在: {os.path.exists(expected_path)}")
        print()
        
        # 检查相对路径解析
        print("=== 相对路径测试 ===")
        test_relative = "../config/config_sap_routing.yaml"
        base_dir = os.path.join(project_root, "data_pipelines", "sources", "sap", "etl")
        resolved = os.path.abspath(os.path.join(base_dir, test_relative))
        print(f"基础目录: {base_dir}")
        print(f"相对路径: {test_relative}")
        print(f"解析结果: {resolved}")
        print(f"文件是否存在: {os.path.exists(resolved)}")
        
    except Exception as e:
        print(f"调试失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_path_resolver()
