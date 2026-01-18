"""
测试MES增量处理脚本迁移后的功能
验证导入路径和路径解析是否正确工作
"""

import sys
import os
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

def test_mes_partitions_script_import():
    """测试MES增量处理脚本的导入是否正常"""
    try:
        # 导入MES增量处理脚本
        sys.path.insert(0, str(project_root / "data_pipelines" / "sources" / "mes" / "etl"))
        import etl_dataclean_mes_batch_report_partitions
        print("✅ MES增量处理脚本导入成功")
        return True
    except ImportError as e:
        print(f"❌ MES增量处理脚本导入失败: {e}")
        return False
    except Exception as e:
        print(f"❌ MES增量处理脚本导入出现其他错误: {e}")
        return False

def test_mes_partitions_path_resolution():
    """测试MES增量处理脚本路径解析"""
    try:
        sys.path.insert(0, str(project_root / "data_pipelines" / "sources" / "mes" / "etl"))
        import etl_dataclean_mes_batch_report_partitions
        
        print("=== MES增量处理脚本路径配置 ===")
        print(f"CONFIG_PATH: {etl_dataclean_mes_batch_report_partitions.CONFIG_PATH}")
        print(f"LOG_PATH: {etl_dataclean_mes_batch_report_partitions.LOG_PATH}")
        print(f"STATE_PATH: {etl_dataclean_mes_batch_report_partitions.STATE_PATH}")
        print(f"BASE_DIR: {etl_dataclean_mes_batch_report_partitions.BASE_DIR}")
        print()
        
        # 检查文件是否存在
        config_exists = os.path.exists(etl_dataclean_mes_batch_report_partitions.CONFIG_PATH)
        print(f"✅ 配置文件存在: {config_exists}")
        
        if config_exists:
            print("✅ MES增量处理路径解析正确")
            return True
        else:
            print("❌ MES增量处理配置文件不存在")
            return False
            
    except Exception as e:
        print(f"❌ MES增量处理路径解析测试失败: {e}")
        return False

def test_mes_partitions_script_functionality():
    """测试MES增量处理脚本的基本功能（不执行完整ETL）"""
    try:
        sys.path.insert(0, str(project_root / "data_pipelines" / "sources" / "mes" / "etl"))
        import etl_dataclean_mes_batch_report_partitions
        
        # 测试配置加载
        config = etl_dataclean_mes_batch_report_partitions.load_config(etl_dataclean_mes_batch_report_partitions.CONFIG_PATH)
        print("✅ MES增量处理配置加载成功")
        
        # 测试ETL处理器初始化
        processor = etl_dataclean_mes_batch_report_partitions.IncrementalETLProcessor(etl_dataclean_mes_batch_report_partitions.CONFIG_PATH)
        print("✅ MES增量处理器初始化成功")
        
        return True
    except Exception as e:
        print(f"❌ MES增量处理脚本功能测试失败: {e}")
        return False

if __name__ == "__main__":
    print("🧪 开始测试MES增量处理脚本迁移...")
    print("=" * 50)
    
    tests = [
        ("MES增量处理脚本导入测试", test_mes_partitions_script_import),
        ("MES增量处理路径解析测试", test_mes_partitions_path_resolution),
        ("MES增量处理脚本功能测试", test_mes_partitions_script_functionality),
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
        print("🎉 所有测试通过！MES增量处理脚本迁移成功")
    else:
        print("⚠️  部分测试失败，需要修复问题")
