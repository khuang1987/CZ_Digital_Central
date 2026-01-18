"""
测试SAP ETL脚本迁移后的功能
验证导入路径和路径解析是否正确工作
"""

import sys
import os
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

def test_sap_script_import():
    """测试SAP脚本的导入是否正常"""
    try:
        # 导入SAP脚本
        sys.path.insert(0, str(project_root / "data_pipelines" / "sources" / "sap" / "etl"))
        import etl_dataclean_sap_routing
        print("✅ SAP脚本导入成功")
        return True
    except ImportError as e:
        print(f"❌ SAP脚本导入失败: {e}")
        return False
    except Exception as e:
        print(f"❌ SAP脚本导入出现其他错误: {e}")
        return False

def test_path_resolver():
    """测试路径解析器功能"""
    try:
        from shared_infrastructure.utils.path_resolver import get_path_resolver, get_config_path, get_log_path
        
        resolver = get_path_resolver()
        
        # 测试配置路径解析
        config_path = get_config_path("sap_routing", "sap")
        print(f"✅ SAP配置路径: {config_path}")
        
        # 测试日志路径解析
        log_path = get_log_path("sap")
        print(f"✅ SAP日志路径: {log_path}")
        
        # 测试路径解析器
        all_paths = resolver.get_all_paths_for_source("sap")
        print(f"✅ SAP所有路径: {list(all_paths.keys())}")
        
        return True
    except Exception as e:
        print(f"❌ 路径解析器测试失败: {e}")
        return False

def test_sap_script_functionality():
    """测试SAP脚本的基本功能（不执行完整ETL）"""
    try:
        sys.path.insert(0, str(project_root / "data_pipelines" / "sources" / "sap" / "etl"))
        import etl_dataclean_sap_routing
        
        # 测试配置加载 - 使用脚本预计算的CONFIG_PATH
        config = etl_dataclean_sap_routing.load_config(etl_dataclean_sap_routing.CONFIG_PATH)
        print("✅ SAP配置加载成功")
        
        # 测试日志设置
        etl_dataclean_sap_routing.setup_logging(config)
        print("✅ SAP日志设置成功")
        
        return True
    except Exception as e:
        print(f"❌ SAP脚本功能测试失败: {e}")
        return False

if __name__ == "__main__":
    print("🧪 开始测试SAP ETL脚本迁移...")
    print("=" * 50)
    
    tests = [
        ("路径解析器测试", test_path_resolver),
        ("SAP脚本导入测试", test_sap_script_import),
        ("SAP脚本功能测试", test_sap_script_functionality),
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n📋 {test_name}:")
        if test_func():
            passed += 1
        else:
            print(f"   测试失败，需要修复")
    
    print("\n" + "=" * 50)
    print(f"📊 测试结果: {passed}/{total} 通过")
    
    if passed == total:
        print("🎉 所有测试通过！SAP脚本迁移成功")
    else:
        print("⚠️  部分测试失败，需要修复问题")
