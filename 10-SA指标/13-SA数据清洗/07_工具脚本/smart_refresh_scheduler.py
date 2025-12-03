"""
一键智能更新脚本 - 按依赖顺序处理4个数据源
处理顺序：日历 → SAP → SFC → MES

使用方法:
1. 立即执行: python smart_refresh_scheduler.py --mode immediate
2. 定时调度: python smart_refresh_scheduler.py --mode scheduled --schedule-time 08:00
3. 查看状态: python smart_refresh_scheduler.py --mode status

功能特点:
- [OK] 按依赖顺序处理4个数据源
- [OK] 详细的步骤执行日志
- [OK] 自动状态跟踪和错误处理
- [OK] 灵活的执行模式
- [OK] 完整的执行报告
"""

import os
import sys
import json
import subprocess
from datetime import datetime
import logging
import schedule
import time

class SmartRefreshScheduler:
    """智能刷新调度器 - 简化版本"""
    
    def __init__(self):
        # 获取脚本目录
        self.script_dir = os.path.dirname(__file__)
        self.etl_dir = os.path.join(self.script_dir, '..', '01_核心ETL程序')
        self.config_dir = os.path.join(self.script_dir, '..', '03_配置文件', 'config')
        self.log_dir = os.path.join(self.script_dir, '..', '06_日志文件')
        
        # 确保目录存在
        os.makedirs(self.log_dir, exist_ok=True)
        
        # 设置日志
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(os.path.join(self.log_dir, 'daily_refresh.log'), encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # ETL脚本路径 - 按依赖顺序定义
        self.etl_scripts = {
            'calendar': {
                'name': '日历数据处理',
                'script': None,  # 日历数据通常在配置中定义，暂无独立脚本
                'required': False,
                'description': '加载工厂日历数据'
            },
            'sap': {
                'name': 'SAP工艺数据处理',
                'script': os.path.join(self.etl_dir, 'etl_dataclean_sap_routing.py'),
                'required': True,
                'description': '处理SAP标准工艺时间数据'
            },
            'sfc': {
                'name': 'SFC批次数据处理',
                'script': os.path.join(self.etl_dir, 'etl_dataclean_sfc_batch_report.py'),
                'required': True,
                'description': '处理SFC车间作业控制数据',
                'depends_on': ['sap']
            },
            'mes': {
                'name': 'MES批次数据处理',
                'script': os.path.join(self.etl_dir, 'etl_dataclean_mes_batch_report.py'),
                'required': True,
                'description': '处理MES制造执行系统数据',
                'depends_on': ['sap', 'sfc']
            }
        }
        
        # 状态文件
        self.state_file = os.path.join(self.log_dir, 'daily_refresh_state.json')
        
        self.logger.info("智能刷新调度器初始化完成")
        self.logger.info(f"ETL目录: {self.etl_dir}")
        self.logger.info(f"日志目录: {self.log_dir}")
        self.logger.info("数据源处理顺序: 日历 → SAP → SFC → MES")
    
    def load_state(self):
        """加载运行状态"""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                self.logger.warning(f"读取状态文件失败: {e}")
        
        return {
            'last_run': None,
            'successful_runs': 0,
            'failed_runs': 0,
            'last_success_time': None
        }
    
    def save_state(self, state):
        """保存运行状态"""
        try:
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(state, f, indent=2, ensure_ascii=False, default=str)
        except Exception as e:
            self.logger.warning(f"保存状态文件失败: {e}")
    
    def run_etl_sequential(self):
        """按依赖顺序执行4个数据源的ETL处理"""
        self.logger.info("=" * 60)
        self.logger.info("开始按依赖顺序执行4个数据源ETL刷新")
        self.logger.info("处理顺序: 日历 → SAP → SFC → MES")
        self.logger.info("=" * 60)
        
        start_time = datetime.now()
        state = self.load_state()
        
        # 初始化执行结果
        execution_results = {}
        overall_success = True
        
        try:
            # 按顺序处理每个数据源
            for step_key, step_info in self.etl_scripts.items():
                step_start_time = datetime.now()
                
                self.logger.info(f"\n[PROCESSING] 步骤 {step_key.upper()}: {step_info['name']}")
                self.logger.info(f"描述: {step_info['description']}")
                
                # 检查依赖关系
                if 'depends_on' in step_info:
                    for dep in step_info['depends_on']:
                        if dep not in execution_results or not execution_results[dep]['success']:
                            self.logger.error(f"[ERROR] 依赖项 {dep} 处理失败，跳过 {step_key}")
                            execution_results[step_key] = {
                                'success': False,
                                'error': f'Dependency {dep} failed',
                                'duration': 0
                            }
                            overall_success = False
                            continue
                
                # 执行ETL脚本
                step_result = self._execute_etl_step(step_key, step_info)
                execution_results[step_key] = step_result
                
                if not step_result['success']:
                    self.logger.error(f"[ERROR] {step_info['name']} 处理失败")
                    overall_success = False
                    if step_info.get('required', True):
                        self.logger.error(f"必需步骤失败，终止后续处理")
                        break
                else:
                    self.logger.info(f"[OK] {step_info['name']} 处理成功，耗时 {step_result['duration']:.2f}秒")
            
            # 更新状态
            end_time = datetime.now()
            total_duration = (end_time - start_time).total_seconds()
            
            state.update({
                'last_run': start_time.isoformat(),
                'total_duration': total_duration,
                'execution_results': execution_results,
                'overall_success': overall_success
            })
            
            if overall_success:
                state['successful_runs'] += 1
                state['last_success_time'] = start_time.isoformat()
                self.logger.info("[SUCCESS] 所有数据源ETL处理完成！")
            else:
                state['failed_runs'] += 1
                self.logger.error("[FAILED] 部分或全部数据源ETL处理失败！")
            
            # 创建执行报告
            self.create_sequential_report(start_time, execution_results, overall_success)
            
            return overall_success
            
        except Exception as e:
            self.logger.error(f"[ERROR] ETL顺序处理发生异常: {e}")
            state['last_run'] = start_time.isoformat()
            state['failed_runs'] += 1
            self.create_failure_report(start_time, e)
            return False
        
        finally:
            # 保存状态
            self.save_state(state)
            
            end_time = datetime.now()
            total_duration = (end_time - start_time).total_seconds()
            self.logger.info(f"执行完成，总耗时: {total_duration:.2f}秒")
    
    def _execute_etl_step(self, step_key: str, step_info: dict) -> dict:
        """执行单个ETL步骤"""
        start_time = datetime.now()
        
        try:
            # 特殊处理日历数据（无独立脚本）
            if step_key == 'calendar':
                # 日历数据通常在配置中定义，无需独立处理
                self.logger.info("[INFO] 日历数据: 通过配置文件加载，无需独立ETL脚本")
                return {
                    'success': True,
                    'duration': 0,
                    'message': 'Calendar data loaded via configuration'
                }
            
            # 检查脚本文件是否存在
            script_path = step_info['script']
            if not script_path or not os.path.exists(script_path):
                error_msg = f"ETL脚本不存在: {script_path}"
                self.logger.error(error_msg)
                return {
                    'success': False,
                    'duration': 0,
                    'error': error_msg
                }
            
            # 构建命令 - 使用增量模式
            cmd = [
                sys.executable,  # 使用当前Python解释器
                script_path,
                '--mode', 'incremental'
            ]
            
            self.logger.info(f"执行命令: {' '.join(cmd)}")
            
            # 执行ETL脚本
            result = subprocess.run(
                cmd,
                cwd=self.etl_dir,
                capture_output=True,
                text=True,
                encoding='utf-8',
                errors='ignore'  # 忽略编码错误，避免Windows中文输出问题
            )
            
            # 记录输出
            if result.stdout:
                self.logger.info(f"ETL输出:\n{result.stdout}")
            
            if result.stderr:
                self.logger.warning(f"ETL警告:\n{result.stderr}")
            
            # 检查执行结果
            if result.returncode == 0:
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                
                return {
                    'success': True,
                    'duration': duration,
                    'return_code': result.returncode
                }
            else:
                raise subprocess.CalledProcessError(result.returncode, cmd)
                
        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            self.logger.error(f"[ERROR] {step_info['name']} 执行失败: {e}")
            
            return {
                'success': False,
                'duration': duration,
                'error': str(e)
            }
    
    def create_sequential_report(self, start_time, execution_results, overall_success):
        """创建顺序执行报告"""
        report = {
            'timestamp': start_time.isoformat(),
            'status': 'success' if overall_success else 'failed',
            'duration_seconds': (datetime.now() - start_time).total_seconds(),
            'processing_order': 'calendar → sap → sfc → mes',
            'execution_results': execution_results,
            'summary': {
                'total_steps': len(execution_results),
                'successful_steps': sum(1 for r in execution_results.values() if r['success']),
                'failed_steps': sum(1 for r in execution_results.values() if not r['success'])
            }
        }
        
        report_file = os.path.join(self.log_dir, f'etl_sequential_{start_time.strftime("%Y%m%d_%H%M%S")}.json')
        
        try:
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False, default=str)
            self.logger.info(f"顺序执行报告已保存: {report_file}")
        except Exception as e:
            self.logger.warning(f"保存顺序执行报告失败: {e}")
    
    def create_success_report(self, start_time, result):
        """创建成功执行报告（保留兼容性）"""
        report = {
            'timestamp': start_time.isoformat(),
            'status': 'success',
            'duration_seconds': (datetime.now() - start_time).total_seconds(),
            'return_code': result.returncode,
            'script': self.main_etl_script if hasattr(self, 'main_etl_script') else 'sequential_processing'
        }
        
        report_file = os.path.join(self.log_dir, f'etl_success_{start_time.strftime("%Y%m%d_%H%M%S")}.json')
        
        try:
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False, default=str)
            self.logger.info(f"成功报告已保存: {report_file}")
        except Exception as e:
            self.logger.warning(f"保存成功报告失败: {e}")
    
    def create_failure_report(self, start_time, error):
        """创建失败执行报告"""
        report = {
            'timestamp': start_time.isoformat(),
            'status': 'failed',
            'error': str(error),
            'script': self.main_etl_script
        }
        
        report_file = os.path.join(self.log_dir, f'etl_failure_{start_time.strftime("%Y%m%d_%H%M%S")}.json')
        
        try:
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False, default=str)
            self.logger.info(f"失败报告已保存: {report_file}")
        except Exception as e:
            self.logger.warning(f"保存失败报告失败: {e}")
    
    def run_daily_refresh(self):
        """执行每日刷新任务"""
        self.logger.info("[START] 开始每日定时刷新任务")
        
        success = self.run_etl_sequential()
        
        if success:
            self.logger.info("[SUCCESS] 每日刷新任务完成")
        else:
            self.logger.error("[FAILED] 每日刷新任务失败")
        
        return success
    
    def run_immediate_refresh(self):
        """立即执行刷新（手动触发）"""
        self.logger.info("[START] 手动触发立即刷新")
        return self.run_etl_sequential()
    
    def show_status(self):
        """显示运行状态"""
        state = self.load_state()
        
        print("\n" + "=" * 40)
        print("[STATUS] 智能刷新调度器状态")
        print("=" * 40)
        print(f"最后运行: {state.get('last_run', '从未运行')}")
        print(f"成功次数: {state.get('successful_runs', 0)}")
        print(f"失败次数: {state.get('failed_runs', 0)}")
        print(f"最后成功: {state.get('last_success_time', '从未成功')}")
        print("=" * 40)
        
        return state

def main():
    """主函数 - 支持多种运行模式"""
    
    import argparse
    
    parser = argparse.ArgumentParser(description='一键智能更新脚本 - 每日增量刷新数据')
    parser.add_argument('--mode', choices=['immediate', 'scheduled', 'status'], 
                       default='immediate', help='运行模式: immediate(立即执行), scheduled(定时调度), status(查看状态)')
    parser.add_argument('--schedule-time', type=str, default='08:00', 
                       help='定时执行时间 (格式: HH:MM，默认08:00)')
    
    args = parser.parse_args()
    
    scheduler = SmartRefreshScheduler()
    
    if args.mode == 'immediate':
        # 立即执行模式
        print("[START] 立即执行增量刷新...")
        success = scheduler.run_immediate_refresh()
        
        if success:
            print("[SUCCESS] 增量刷新完成！")
            sys.exit(0)
        else:
            print("[ERROR] 增量刷新失败！")
            sys.exit(1)
    
    elif args.mode == 'scheduled':
        # 定时调度模式
        print(f"⏰ 启动定时调度模式，每天 {args.schedule_time} 执行")
        print("按 Ctrl+C 停止调度器")
        
        # 设置每日定时任务
        schedule.every().day.at(args.schedule_time).do(scheduler.run_daily_refresh)
        
        # 显示当前状态
        scheduler.show_status()
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # 每分钟检查一次
        except KeyboardInterrupt:
            print("\n⏹️  调度器已停止")
    
    elif args.mode == 'status':
        # 状态查看模式
        scheduler.show_status()

if __name__ == "__main__":
    main()

if __name__ == "__main__":
    main()
