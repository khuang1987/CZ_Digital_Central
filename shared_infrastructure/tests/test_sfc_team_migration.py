"""
测试SFC团队通过率脚本迁移后的功能
验证导入路径和路径解析是否正确工作
"""

import sys
import os
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

def test_sfc_team_script_import():
    """测试SFC团队通过率脚本的导入是否正常"""
    try:
        # 导入SFC团队通过率脚本
        sys.path.insert(0, str(project_root / "data_pipelines" / "sources" / "sfc" / "etl"))
        import etl_dataclean_sfc_team_passrate
        print("✅ SFC团队通过率脚本导入成功")
        return True
    except ImportError as e:
        print(f"❌ SFC团队通过率脚本导入失败: {e}")
        return False
    except Exception as e:
        print(f"❌ SFC团队通过率脚本导入出现其他错误: {e}")
        return False

def test_sfc_team_path_resolution():
    """测试SFC团队通过率脚本路径解析"""
    try:
        sys.path.insert(0, str(project_root / "data_pipelines" / "sources" / "sfc" / "etl"))
        import etl_dataclean_sfc_team_passrate
        
        print("=== SFC团队通过率脚本路径配置 ===")
        print(f"CONFIG_PATH: {etl_dataclean_sfc_team_passrate.CONFIG_PATH}")
        print(f"LOG_PATH: {etl_dataclean_sfc_team_passrate.LOG_PATH}")
        print(f"STATE_PATH: {etl_dataclean_sfc_team_passrate.STATE_PATH}")
        print(f"BASE_DIR: {etl_dataclean_sfc_team_passrate.BASE_DIR}")
        print()
        
        # 检查文件是否存在
        config_exists = os.path.exists(etl_dataclean_sfc_team_passrate.CONFIG_PATH)
        print(f"✅ 配置文件存在: {config_exists}")
        
        if config_exists:
            print("✅ SFC团队通过率路径解析正确")
            return True
        else:
            print("❌ SFC团队通过率配置文件不存在")
            return False
            
    except Exception as e:
        print(f"❌ SFC团队通过率路径解析测试失败: {e}")
        return False

def test_sfc_team_script_functionality():
    """测试SFC团队通过率脚本的基本功能（不执行完整ETL）"""
    try:
        sys.path.insert(0, str(project_root / "data_pipelines" / "sources" / "sfc" / "etl"))
        import etl_dataclean_sfc_team_passrate
        
        # 测试配置加载
        config = etl_dataclean_sfc_team_passrate.load_config(etl_dataclean_sfc_team_passrate.CONFIG_PATH)
        print("✅ SFC团队通过率配置加载成功")
        
        # 测试日志设置
        etl_dataclean_sfc_team_passrate.setup_logging(config)
        print("✅ SFC团队通过率日志设置成功")
        
        return True
    except Exception as e:
        print(f"❌ SFC团队通过率脚本功能测试失败: {e}")
        return False

if __name__ == "__main__":
    print("🧪 开始测试SFC团队通过率脚本迁移...")
    print("=" * 50)
    
    tests = [
        ("SFC团队通过率脚本导入测试", test_sfc_team_script_import),
        ("SFC团队通过率路径解析测试", test_sfc_team_path_resolution),
        ("SFC团队通过率脚本功能测试", test_sfc_team_script_functionality),
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
        print("🎉 所有测试通过！SFC团队通过率脚本迁移成功")
    else:
        print("⚠️  部分测试失败，需要修复问题")
