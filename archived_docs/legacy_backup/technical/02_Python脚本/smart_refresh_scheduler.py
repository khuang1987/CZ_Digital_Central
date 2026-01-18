"""
智能刷新调度器
根据数据变化情况智能决定刷新策略
"""

import os
import sys
import json
import pandas as pd
from datetime import datetime, timedelta
import logging
import schedule
import time

# 添加ETL工具函数
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '01_核心ETL程序'))
from etl_utils import load_config

class SmartRefreshScheduler:
    """智能刷新调度器"""
    
    def __init__(self):
        self.config_path = os.path.join(os.path.dirname(__file__), '..', '03_配置文件', 'config', 'config_mes_batch_report.yaml')
        self.cfg = load_config(self.config_path)
        self.state_file = "smart_refresh_state.json"
        
        # 设置日志
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('../06_日志文件/smart_refresh.log', encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def check_data_changes(self):
        """检查数据变化情况"""
        
        try:
            # 读取ETL状态文件
            etl_state_file = self.cfg.get("incremental", {}).get("state_file", "")
            if os.path.exists(etl_state_file):
                with open(etl_state_file, 'r', encoding='utf-8') as f:
                    etl_state = json.load(f)
                
                last_update = datetime.fromisoformat(etl_state.get('last_update', '2020-01-01'))
                new_records = etl_state.get('new_records_count', 0)
                
                # 读取当前数据文件
                output_file = self.cfg.get("output", {}).get("base_dir", "")
                latest_file = os.path.join(output_file, "MES_batch_report_latest.parquet")
                
                if os.path.exists(latest_file):
                    current_df = pd.read_parquet(latest_file)
                    current_records = len(current_df)
                    
                    # 计算变化率
                    change_rate = new_records / max(current_records, 1)
                    
                    self.logger.info(f"数据变化检查:")
                    self.logger.info(f"  最后更新: {last_update}")
                    self.logger.info(f"  当前总记录: {current_records}")
                    self.logger.info(f"  新增记录: {new_records}")
                    self.logger.info(f"  变化率: {change_rate:.2%}")
                    
                    return {
                        'last_update': last_update,
                        'new_records': new_records,
                        'total_records': current_records,
                        'change_rate': change_rate,
                        'needs_refresh': self._needs_refresh(change_rate, last_update)
                    }
            
            return {'needs_refresh': True, 'reason': 'no_state_file'}
            
        except Exception as e:
            self.logger.error(f"检查数据变化失败: {e}")
            return {'needs_refresh': True, 'reason': 'error'}
    
    def _needs_refresh(self, change_rate, last_update):
        """判断是否需要刷新"""
        
        now = datetime.now()
        hours_since_update = (now - last_update).total_seconds() / 3600
        
        # 刷新策略
        if hours_since_update > 24:  # 超过24小时强制刷新
            return True
        elif change_rate > 0.05:  # 变化率超过5%
            return True
        elif hours_since_update > 8 and change_rate > 0.01:  # 超过8小时且变化率超过1%
            return True
        else:
            return False
    
    def create_refresh_report(self):
        """创建刷新报告"""
        
        change_info = self.check_data_changes()
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'refresh_needed': change_info.get('needs_refresh', False),
            'data_changes': change_info,
            'recommendations': self._get_recommendations(change_info)
        }
        
        # 保存报告
        report_file = os.path.join(os.path.dirname(__file__), '..', '06_日志文件', 'refresh_report.json')
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False, default=str)
        
        self.logger.info(f"刷新报告已保存: {report_file}")
        return report
    
    def _get_recommendations(self, change_info):
        """获取刷新建议"""
        
        recommendations = []
        
        if change_info.get('needs_refresh'):
            change_rate = change_info.get('change_rate', 0)
            
            if change_rate > 0.1:
                recommendations.append("数据变化较大(>10%)，建议立即执行全量ETL刷新")
            elif change_rate > 0.05:
                recommendations.append("数据变化中等(5-10%)，建议执行增量ETL刷新")
            else:
                recommendations.append("数据变化较小(<5%)，可延迟到下次定时刷新")
        else:
            recommendations.append("数据变化很小，无需立即刷新")
        
        # PowerBI刷新建议
        recommendations.append("PowerBI建议使用增量刷新模式，只加载最近7天数据")
        
        return recommendations
    
    def run_etl_if_needed(self):
        """如果需要则运行ETL"""
        
        report = self.create_refresh_report()
        
        if report['refresh_needed']:
            self.logger.info("检测到需要刷新，开始执行ETL...")
            
            # 运行ETL脚本
            etl_script = os.path.join(os.path.dirname(__file__), '..', '01_核心ETL程序', 'etl_dataclean_mes_batch_report.py')
            
            try:
                os.system(f'cd "{os.path.dirname(etl_script)}" && python "{os.path.basename(etl_script)}"')
                self.logger.info("ETL执行完成")
                
                # 创建增量分区
                partition_script = os.path.join(os.path.dirname(__file__), '..', '01_核心ETL程序', 'create_incremental_partitions.py')
                if os.path.exists(partition_script):
                    os.system(f'cd "{os.path.dirname(partition_script)}" && python "{os.path.basename(partition_script)}"')
                    self.logger.info("增量分区创建完成")
                
            except Exception as e:
                self.logger.error(f"ETL执行失败: {e}")
        else:
            self.logger.info("无需刷新，跳过ETL执行")

def main():
    """主函数"""
    
    scheduler = SmartRefreshScheduler()
    
    # 立即执行一次检查
    scheduler.run_etl_if_needed()
    
    # 设置定时任务
    schedule.every(4).hours.do(scheduler.run_etl_if_needed)  # 每4小时检查一次
    schedule.every().day.at("08:00").do(scheduler.run_etl_if_needed)  # 每天8点强制检查
    schedule.every().day.at("18:00").do(scheduler.run_etl_if_needed)  # 每天18点强制检查
    
    print("智能刷新调度器已启动")
    print(" - 每4小时检查一次数据变化")
    print(" - 每天8:00和18:00强制检查")
    print(" - 按Ctrl+C停止")
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # 每分钟检查一次
    except KeyboardInterrupt:
        print("\n调度器已停止")

if __name__ == "__main__":
    main()
