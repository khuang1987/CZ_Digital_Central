"""
调试项目根目录计算
"""

import os
import sys

def debug_project_root():
    """调试项目根目录计算"""
    # 模拟SAP脚本位置
    script_path = r"C:\Users\huangk14\OneDrive - Medtronic PLC\Huangkai Files\B1_Project\250418_MDDAP_project\data_pipelines\sources\sap\etl\etl_dataclean_sap_routing.py"
    
    # 使用相同的计算逻辑
    current_dir = os.path.dirname(os.path.abspath(script_path))
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
    
    print("=== 项目根目录计算调试 ===")
    print(f"脚本路径: {script_path}")
    print(f"current_dir: {current_dir}")
    print(f"project_root: {project_root}")
    print()
    
    # 检查shared_infrastructure目录是否存在
    shared_infra_path = os.path.join(project_root, "shared_infrastructure")
    print(f"shared_infrastructure路径: {shared_infra_path}")
    print(f"shared_infrastructure存在: {os.path.exists(shared_infra_path)}")
    
    # 检查sys.path
    print("\n=== sys.path检查 ===")
    print(f"当前sys.path[0]: {sys.path[0]}")
    print(f"项目根目录是否在sys.path: {project_root in sys.path}")
    
    # 模拟添加到sys.path
    sys.path.insert(0, project_root)
    print(f"添加后sys.path[0]: {sys.path[0]}")
    
    # 尝试导入
    try:
        import shared_infrastructure
        print("✅ shared_infrastructure导入成功")
    except ImportError as e:
        print(f"❌ shared_infrastructure导入失败: {e}")

if __name__ == "__main__":
    debug_project_root()
