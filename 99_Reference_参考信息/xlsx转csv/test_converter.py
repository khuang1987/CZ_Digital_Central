#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
XLSX转CSV工具测试脚本
用于验证转换功能是否正常工作
"""

import pandas as pd
import os
import tempfile
from pathlib import Path

def create_test_excel():
    """创建测试用的Excel文件"""
    print("创建测试Excel文件...")
    
    # 创建测试数据
    data1 = {
        '姓名': ['张三', '李四', '王五', '赵六'],
        '年龄': [25, 30, 35, 28],
        '部门': ['技术部', '销售部', '人事部', '财务部'],
        '工资': [8000, 12000, 9000, 10000]
    }
    
    data2 = {
        '产品': ['产品A', '产品B', '产品C'],
        '销量': [100, 150, 80],
        '单价': [50.5, 75.2, 120.0],
        '总价': [5050, 11280, 9600]
    }
    
    # 创建临时目录
    temp_dir = tempfile.mkdtemp()
    test_file = os.path.join(temp_dir, 'test_data.xlsx')
    
    # 创建Excel文件
    with pd.ExcelWriter(test_file, engine='openpyxl') as writer:
        df1 = pd.DataFrame(data1)
        df2 = pd.DataFrame(data2)
        
        df1.to_excel(writer, sheet_name='员工信息', index=False)
        df2.to_excel(writer, sheet_name='销售数据', index=False)
    
    print(f"测试文件已创建: {test_file}")
    return test_file, temp_dir

def test_conversion(test_file, output_dir):
    """测试转换功能"""
    print("\n开始测试转换功能...")
    
    try:
        # 读取Excel文件
        excel_file = pd.ExcelFile(test_file)
        sheet_names = excel_file.sheet_names
        print(f"发现工作表: {sheet_names}")
        
        # 转换每个工作表
        converted_files = []
        for sheet_name in sheet_names:
            print(f"转换工作表: {sheet_name}")
            
            # 读取数据
            df = pd.read_excel(test_file, sheet_name=sheet_name)
            
            # 生成输出文件名
            base_name = Path(test_file).stem
            if len(sheet_names) > 1:
                output_filename = f"{base_name}_{sheet_name}.csv"
            else:
                output_filename = f"{base_name}.csv"
                
            output_file = os.path.join(output_dir, output_filename)
            
            # 保存为CSV
            df.to_csv(output_file, encoding='utf-8-sig', sep=',', index=False, na_rep='')
            
            converted_files.append(output_file)
            print(f"已保存: {output_filename}")
            
            # 验证文件内容
            df_check = pd.read_csv(output_file, encoding='utf-8-sig')
            print(f"验证成功: {output_filename} 包含 {len(df_check)} 行数据")
        
        print(f"\n转换测试完成！共转换 {len(converted_files)} 个文件")
        return True
        
    except Exception as e:
        print(f"转换测试失败: {str(e)}")
        return False

def main():
    """主测试函数"""
    print("=" * 50)
    print("XLSX转CSV工具测试")
    print("=" * 50)
    
    # 检查依赖包
    try:
        import pandas as pd
        import openpyxl
        import xlrd
        print("✓ 所有依赖包已安装")
    except ImportError as e:
        print(f"✗ 缺少依赖包: {e}")
        print("请运行: pip install pandas openpyxl xlrd")
        return
    
    # 创建测试文件
    test_file, temp_dir = create_test_excel()
    
    # 测试转换
    success = test_conversion(test_file, temp_dir)
    
    if success:
        print("\n" + "=" * 50)
        print("✓ 所有测试通过！工具可以正常使用")
        print("=" * 50)
    else:
        print("\n" + "=" * 50)
        print("✗ 测试失败，请检查错误信息")
        print("=" * 50)
    
    # 清理临时文件
    try:
        import shutil
        shutil.rmtree(temp_dir)
        print(f"已清理临时文件: {temp_dir}")
    except:
        pass

if __name__ == "__main__":
    main() 