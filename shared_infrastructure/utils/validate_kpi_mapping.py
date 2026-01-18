#!/usr/bin/env python3
"""
KPI映射验证脚本
验证YAML文件的完整性、一致性和数据流完整性
"""

import yaml
import os
from typing import Dict, List, Set, Any
from pathlib import Path

class KpiMappingValidator:
    def __init__(self, base_path: str):
        self.base_path = Path(base_path)
        self.kpi_mapping_path = self.base_path / "business-domains" / "kpi-mapping"
        self.errors = []
        self.warnings = []
        
    def load_yaml_file(self, filename: str) -> Dict:
        """加载YAML文件"""
        file_path = self.kpi_mapping_path / filename
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except Exception as e:
            self.errors.append(f"无法加载文件 {filename}: {str(e)}")
            return {}
    
    def validate_yaml_syntax(self, filename: str) -> bool:
        """验证YAML语法"""
        file_path = self.kpi_mapping_path / filename
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                yaml.safe_load(f)
            return True
        except yaml.YAMLError as e:
            self.errors.append(f"YAML语法错误 {filename}: {str(e)}")
            return False
        except Exception as e:
            self.errors.append(f"文件读取错误 {filename}: {str(e)}")
            return False
    
    def get_all_kpis_from_department_matrix(self, data: Dict) -> Set[str]:
        """从部门KPI矩阵中提取所有KPI"""
        kpis = set()
        if 'department_kpi_matrix' not in data:
            self.errors.append("department_kpi_matrix.yaml 缺少 department_kpi_matrix 根节点")
            return kpis
        
        for dept_name, dept_info in data['department_kpi_matrix'].items():
            if isinstance(dept_info, dict) and 'kpis' in dept_info:
                for kpi_name in dept_info['kpis'].keys():
                    kpis.add(kpi_name)
        
        return kpis
    
    def get_all_kpis_from_dependencies(self, data: Dict) -> Set[str]:
        """从KPI依赖映射中提取所有KPI"""
        kpis = set()
        if 'kpi_source_dependencies' not in data:
            self.errors.append("kpi_source_dependencies.yaml 缺少 kpi_source_dependencies 根节点")
            return kpis
        
        for kpi_name in data['kpi_source_dependencies'].keys():
            kpis.add(kpi_name)
        
        return kpis
    
    def get_cross_department_kpis(self, data: Dict) -> Set[str]:
        """从跨部门KPI中提取所有KPI"""
        kpis = set()
        if 'cross_department_kpis' not in data:
            self.errors.append("cross_department_kpis.yaml 缺少 cross_department_kpis 根节点")
            return kpis
        
        for kpi_name in data['cross_department_kpis'].keys():
            kpis.add(kpi_name)
        
        return kpis
    
    def get_technical_sources_from_business_mapping(self, data: Dict) -> Set[str]:
        """从业务系统映射中提取技术数据源"""
        sources = set()
        if 'business_system_mapping' not in data:
            self.errors.append("business_system_mapping.yaml 缺少 business_system_mapping 根节点")
            return sources
        
        for system_name, system_info in data['business_system_mapping'].items():
            if isinstance(system_info, dict) and 'technical_source' in system_info:
                sources.add(system_info['technical_source'])
        
        # 检查data_source_summary中的数据源
        if 'data_source_summary' in data:
            sources.update(data['data_source_summary'].keys())
        
        return sources
    
    def validate_kpi_consistency(self):
        """验证KPI一致性"""
        print("验证KPI一致性...")
        
        # 加载数据
        dept_matrix = self.load_yaml_file("department_kpi_matrix.yaml")
        dependencies = self.load_yaml_file("kpi_source_dependencies.yaml")
        cross_dept = self.load_yaml_file("cross_department_kpis.yaml")
        
        # 提取KPI集合
        dept_kpis = self.get_all_kpis_from_department_matrix(dept_matrix)
        dep_kpis = self.get_all_kpis_from_dependencies(dependencies)
        cross_kpis = self.get_cross_department_kpis(cross_dept)
        
        # 验证数量
        print(f"部门KPI矩阵中的KPI数量: {len(dept_kpis)}")
        print(f"依赖映射中的KPI数量: {len(dep_kpis)}")
        print(f"跨部门KPI数量: {len(cross_kpis)}")
        
        # 检查一致性
        missing_in_deps = dept_kpis - dep_kpis
        missing_in_dept = dep_kpis - dept_kpis
        
        if missing_in_deps:
            self.errors.append(f"KPI在部门矩阵中存在但在依赖映射中缺失: {missing_in_deps}")
        
        if missing_in_dept:
            self.errors.append(f"KPI在依赖映射中存在但在部门矩阵中缺失: {missing_in_dept}")
        
        # 验证跨部门KPI依赖
        for cross_kpi, cross_info in cross_dept.get('cross_department_kpis', {}).items():
            if 'kpi_dependencies' in cross_info:
                for dep_kpi in cross_info['kpi_dependencies']:
                    if dep_kpi not in dept_kpis:
                        self.errors.append(f"跨部门KPI {cross_kpi} 依赖的KPI {dep_kpi} 不存在")
        
        return len(dept_kpis) == len(dep_kpis) and not missing_in_deps and not missing_in_dept
    
    def validate_data_source_consistency(self):
        """验证数据源一致性"""
        print("验证数据源一致性...")
        
        # 加载数据
        dependencies = self.load_yaml_file("kpi_source_dependencies.yaml")
        business_mapping = self.load_yaml_file("business_system_mapping.yaml")
        
        # 提取数据源
        tech_sources = self.get_technical_sources_from_business_mapping(business_mapping)
        
        # 检查依赖映射中的数据源
        used_sources = set()
        for kpi_name, kpi_info in dependencies.get('kpi_source_dependencies', {}).items():
            if 'technical_sources' in kpi_info:
                used_sources.update(kpi_info['technical_sources'])
            if 'primary_sources' in kpi_info:
                used_sources.update(kpi_info['primary_sources'])
            if 'secondary_sources' in kpi_info:
                used_sources.update(kpi_info['secondary_sources'])
        
        print(f"业务系统映射中的技术数据源: {tech_sources}")
        print(f"KPI依赖中使用的数据源: {used_sources}")
        
        # 检查未定义的数据源
        undefined_sources = used_sources - tech_sources
        if undefined_sources:
            self.errors.append(f"KPI依赖中使用但未在业务系统映射中定义的数据源: {undefined_sources}")
        
        # 检查未使用的数据源
        unused_sources = tech_sources - used_sources
        if unused_sources:
            self.warnings.append(f"在业务系统映射中定义但未在KPI依赖中使用的数据源: {unused_sources}")
        
        return len(undefined_sources) == 0
    
    def validate_department_structure(self):
        """验证部门结构"""
        print("验证部门结构...")
        
        dept_matrix = self.load_yaml_file("department_kpi_matrix.yaml")
        
        if 'department_kpi_matrix' not in dept_matrix:
            self.errors.append("缺少 department_kpi_matrix 根节点")
            return False
        
        expected_departments = {
            'production-dept', 'quality-dept', 'ci-dept', 'supply-chain-dept',
            'finance-dept', 'equipment-dept', 'facilities-dept', 'safety-dept'
        }
        
        actual_departments = set(dept_matrix['department_kpi_matrix'].keys())
        
        missing_depts = expected_departments - actual_departments
        extra_depts = actual_departments - expected_departments
        
        if missing_depts:
            self.errors.append(f"缺失的部门: {missing_depts}")
        
        if extra_depts:
            self.warnings.append(f"额外的部门: {extra_depts}")
        
        # 验证每个部门的KPI数量
        kpi_counts = {}
        total_kpis = 0
        
        for dept_name, dept_info in dept_matrix['department_kpi_matrix'].items():
            if isinstance(dept_info, dict) and 'kpis' in dept_info:
                kpi_count = len(dept_info['kpis'])
                kpi_counts[dept_name] = kpi_count
                total_kpis += kpi_count
        
        print(f"各部门KPI数量: {kpi_counts}")
        print(f"总KPI数量: {total_kpis}")
        
        if total_kpis != 50:
            self.errors.append(f"总KPI数量应为50，实际为{total_kpis}")
        
        return len(missing_depts) == 0 and total_kpis == 50
    
    def validate_data_flow_completeness(self):
        """验证数据流完整性"""
        print("验证数据流完整性...")
        
        dependencies = self.load_yaml_file("kpi_source_dependencies.yaml")
        
        for kpi_name, kpi_info in dependencies.get('kpi_source_dependencies', {}).items():
            # 检查必需字段
            required_fields = ['primary_sources', 'data_flow', 'calculation_logic', 'business_systems']
            for field in required_fields:
                if field not in kpi_info:
                    self.errors.append(f"KPI {kpi_name} 缺少必需字段: {field}")
            
            # 检查数据流
            if 'data_flow' in kpi_info:
                for flow_item in kpi_info['data_flow']:
                    if 'source' not in flow_item or 'table' not in flow_item or 'fields' not in flow_item:
                        self.errors.append(f"KPI {kpi_name} 数据流项缺少必需字段")
        
        return True
    
    def run_validation(self) -> bool:
        """运行完整验证"""
        print("开始KPI映射验证...")
        print("=" * 50)
        
        # 验证YAML语法
        yaml_files = [
            "department_kpi_matrix.yaml",
            "kpi_source_dependencies.yaml", 
            "business_system_mapping.yaml",
            "cross_department_kpis.yaml"
        ]
        
        syntax_valid = True
        for yaml_file in yaml_files:
            if not self.validate_yaml_syntax(yaml_file):
                syntax_valid = False
        
        if not syntax_valid:
            print("YAML语法验证失败，停止后续验证")
            return False
        
        # 运行各项验证
        kpi_consistent = self.validate_kpi_consistency()
        source_consistent = self.validate_data_source_consistency()
        dept_valid = self.validate_department_structure()
        flow_complete = self.validate_data_flow_completeness()
        
        # 输出结果
        print("=" * 50)
        print("验证结果:")
        print(f"KPI一致性: {'✓' if kpi_consistent else '✗'}")
        print(f"数据源一致性: {'✓' if source_consistent else '✗'}")
        print(f"部门结构: {'✓' if dept_valid else '✗'}")
        print(f"数据流完整性: {'✓' if flow_complete else '✗'}")
        
        if self.errors:
            print(f"\n错误 ({len(self.errors)}):")
            for error in self.errors:
                print(f"  ✗ {error}")
        
        if self.warnings:
            print(f"\n警告 ({len(self.warnings)}):")
            for warning in self.warnings:
                print(f"  ⚠ {warning}")
        
        if not self.errors and not self.warnings:
            print("\n🎉 所有验证通过！")
        
        return len(self.errors) == 0

def main():
    """主函数"""
    # 获取当前脚本所在的项目根目录
    script_dir = Path(__file__).parent
    base_path = script_dir.parent.parent
    
    validator = KpiMappingValidator(base_path)
    success = validator.run_validation()
    
    if success:
        print("\n✅ 验证完成，可以进入下一阶段")
        return 0
    else:
        print("\n❌ 验证失败，请修复错误后重试")
        return 1

if __name__ == "__main__":
    exit(main())
