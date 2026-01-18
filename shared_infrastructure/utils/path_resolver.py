"""
路径解析器
提供统一的路径管理，解决ETL脚本中的硬编码路径问题
"""

import os
import yaml
from typing import Dict, Any, Optional
from pathlib import Path


class PathResolver:
    """路径解析器类，提供统一的路径管理功能"""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        初始化路径解析器
        
        Args:
            config_path: paths.yaml配置文件路径，默认使用相对路径
        """
        if config_path is None:
            # 从shared-infrastructure/utils/定位到config/paths.yaml
            current_dir = Path(__file__).parent
            config_path = current_dir.parent / "config" / "paths.yaml"
        
        self.config = self._load_config(config_path)
        self.project_root = self._find_project_root()
        self._resolve_all_paths()
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """加载路径配置文件"""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"路径配置文件不存在: {config_path}")
        except yaml.YAMLError as e:
            raise ValueError(f"路径配置文件格式错误: {e}")
    
    def _find_project_root(self) -> str:
        """查找项目根目录"""
        current_path = Path(__file__).parent
        
        # 向上查找项目根目录（包含README.md或MIGRATION_PLAN.md的目录）
        while current_path.parent != current_path:
            if (current_path / "README.md").exists() or (current_path / "MIGRATION_PLAN.md").exists():
                return str(current_path.absolute())
            current_path = current_path.parent
        
        # 如果没找到，使用当前脚本的上3级目录作为项目根目录
        return str(Path(__file__).parent.parent.parent.absolute())
    
    def _resolve_all_paths(self):
        """保持配置文件中的原始路径，不在初始化时解析"""
        # 不进行任何路径解析，保持原始字符串
        # 路径解析将在具体方法中按需进行
        pass
    
    def _resolve_path(self, path: str, source: Optional[str] = None, department: Optional[str] = None) -> str:
        """
        解析单个路径
        
        Args:
            path: 原始路径
            source: 数据源名称（用于变量替换）
            department: 部门名称（用于变量替换）
        """
        # 变量替换
        if source and "{source}" in path:
            path = path.replace("{source}", source)
        if department and "{department}" in path:
            path = path.replace("{department}", department)
        
        # 解析相对路径
        if not os.path.isabs(path):
            # 相对于项目根目录
            resolved_path = os.path.join(self.project_root, path)
        else:
            resolved_path = path
        
        return os.path.abspath(resolved_path)
    
    def get_path(self, category: str, name: str, source: Optional[str] = None, department: Optional[str] = None) -> str:
        """
        获取指定类别的路径
        
        Args:
            category: 路径类别（如 'config_paths', 'state_paths'）
            name: 路径名称
            source: 数据源名称
            department: 部门名称
        """
        if category not in self.config:
            raise ValueError(f"未知的路径类别: {category}")
        
        if name not in self.config[category]:
            raise ValueError(f"在类别 {category} 中未找到路径名称: {name}")
        
        path = self.config[category][name]
        
        # 对于配置文件和状态文件路径，返回原始字符串以便后续解析
        if category in ["config_paths", "state_paths"]:
            return path
        else:
            return self._resolve_path(path, source, department)
    
    def get_config_path(self, script_name: str, source: Optional[str] = None, base_dir: Optional[str] = None) -> str:
        """获取配置文件路径"""
        if base_dir is None:
            # 默认使用调用者的目录
            import inspect
            frame = inspect.currentframe().f_back
            base_dir = os.path.dirname(os.path.abspath(frame.f_globals['__file__']))
        
        config_path = self.get_path("config_paths", script_name, source)
        if not os.path.isabs(config_path):
            # 相对于base_dir解析
            config_path = os.path.abspath(os.path.join(base_dir, config_path))
        
        return config_path
    
    def get_state_path(self, script_name: str, source: Optional[str] = None, base_dir: Optional[str] = None) -> str:
        """获取状态文件路径"""
        if base_dir is None:
            # 默认使用调用者的目录
            import inspect
            frame = inspect.currentframe().f_back
            base_dir = os.path.dirname(os.path.abspath(frame.f_globals['__file__']))
        
        state_path = self.get_path("state_paths", script_name, source)
        if not os.path.isabs(state_path):
            # 相对于base_dir解析
            state_path = os.path.abspath(os.path.join(base_dir, state_path))
        
        return state_path
    
    def get_log_path(self, source: str) -> str:
        """获取日志文件路径"""
        return self.get_path("logging_paths", source, source)
    
    def get_test_data_path(self, source: str, department: Optional[str] = None) -> str:
        """获取测试数据路径"""
        if department is None:
            # 根据数据源自动推断部门
            department = self.get_department_for_source(source)
        return self.get_path("test_data_paths", source, source, department)
    
    def get_output_data_path(self, source: str, department: Optional[str] = None) -> str:
        """获取输出数据路径"""
        if department is None:
            department = self.get_department_for_source(source)
        return self.get_path("data_source_paths", "output_data", source, department)
    
    def get_department_for_source(self, source: str) -> str:
        """根据数据源获取对应的部门"""
        source_to_dept = self.config.get("source_to_department", {})
        
        if source not in source_to_dept:
            raise ValueError(f"未找到数据源 {source} 对应的部门映射")
        
        dept_mapping = source_to_dept[source]
        if isinstance(dept_mapping, list):
            # 如果映射到多个部门，返回第一个
            return dept_mapping[0]
        else:
            return dept_mapping
    
    def ensure_directory_exists(self, path: str):
        """确保目录存在"""
        directory = os.path.dirname(path) if os.path.isfile(path) else path
        os.makedirs(directory, exist_ok=True)
    
    def get_all_paths_for_source(self, source: str) -> Dict[str, str]:
        """获取指定数据源的所有路径"""
        department = self.get_department_for_source(source)
        paths = {}
        
        # 配置文件
        for config_name in self.config.get("config_paths", {}):
            if source in config_name:
                paths[f"config_{config_name}"] = self.get_config_path(config_name, source)
        
        # 状态文件
        for state_name in self.config.get("state_paths", {}):
            if source in state_name:
                paths[f"state_{state_name}"] = self.get_state_path(state_name, source)
        
        # 日志文件
        paths["log"] = self.get_log_path(source)
        
        # 测试数据
        paths["test_data"] = self.get_test_data_path(source, department)
        
        # 输出数据
        paths["output_data"] = self.get_output_data_path(source, department)
        
        return paths


# 创建全局路径解析器实例
_global_resolver = None


def get_path_resolver() -> PathResolver:
    """获取全局路径解析器实例"""
    global _global_resolver
    if _global_resolver is None:
        _global_resolver = PathResolver()
    return _global_resolver


def resolve_path(category: str, name: str, source: Optional[str] = None, department: Optional[str] = None) -> str:
    """便捷函数：获取路径"""
    return get_path_resolver().get_path(category, name, source, department)


def get_config_path(script_name: str, source: Optional[str] = None, base_dir: Optional[str] = None) -> str:
    """便捷函数：获取配置文件路径"""
    return get_path_resolver().get_config_path(script_name, source, base_dir)


def get_state_path(script_name: str, source: Optional[str] = None, base_dir: Optional[str] = None) -> str:
    """便捷函数：获取状态文件路径"""
    return get_path_resolver().get_state_path(script_name, source, base_dir)


def get_log_path(source: str) -> str:
    """便捷函数：获取日志文件路径"""
    return get_path_resolver().get_log_path(source)
