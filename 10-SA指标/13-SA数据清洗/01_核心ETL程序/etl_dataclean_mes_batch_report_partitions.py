"""
MES数据清洗ETL - 增量处理优化版本
基于行业最佳实践的增量数据处理架构
去除RAW层，实现高效的记录级变更检测
保持与etl_dataclean_mes_batch_report.py相同的数据清洗逻辑
"""

import os
import sys
import pandas as pd
import numpy as np
import json
import hashlib
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple, Set
import logging
import argparse
from pathlib import Path

# 添加ETL工具函数
sys.path.append(os.path.dirname(__file__))
from etl_utils import load_config, setup_logging, prompt_refresh_mode
from etl_dataclean_mes_batch_report import (
    process_mes_data, merge_sfc_data, merge_standard_time,
    calculate_lt, calculate_pt, calculate_st, calculate_completion_status,
    load_calendar_table, process_all_data, remove_duplicates
)

class IncrementalETLProcessor:
    """增量处理ETL处理器 - 行业最佳实践版本"""
    
    def __init__(self, config_path: str):
        """
        初始化ETL处理器
        
        Args:
            config_path: 配置文件路径
        """
        self.config = load_config(config_path)
        self.logger = self._setup_logging()
        
        # 获取分层架构配置
        layered_cfg = self.config.get("layered_architecture", {})
        
        # 设置系统-实体-分层架构目录
        if layered_cfg.get("enabled", True):
            # 使用配置文件中的设置
            self.system_name = layered_cfg.get("system", {}).get("name", "MES")
            self.entity_name = layered_cfg.get("system", {}).get("entity", "batch_report")
            
            output_cfg = layered_cfg.get("output", {})
            self.base_dir = Path(output_cfg.get("base_dir", r"C:\Users\huangk14\OneDrive - Medtronic PLC\CZ Production - 文档\General\POWER BI 数据源 V2\A1_ETL_Output"))
            
            layers_cfg = output_cfg.get("layers", {})
            self.processed_dir = self.base_dir / self.system_name / self.entity_name / layers_cfg.get("processed", "01_PROCESSED")
            self.publish_dir = self.base_dir / self.system_name / self.entity_name / layers_cfg.get("publish", "02_PUBLISH")
            self.metadata_dir = self.base_dir / layers_cfg.get("metadata", "03_METADATA") / self.system_name / self.entity_name
            
            # 获取哈希字段配置
            self.hash_fields = layered_cfg.get("record_level_incremental", {}).get("hash_fields", ["BatchNumber", "Operation", "TrackOutTime"])
        else:
            # 回退到默认配置
            self.base_dir = Path(r"C:\Users\huangk14\OneDrive - Medtronic PLC\CZ Production - 文档\General\POWER BI 数据源 V2\A1_ETL_Output")
            self.system_name = "MES"
            self.entity_name = "batch_report"
            self.processed_dir = self.base_dir / self.system_name / self.entity_name / "01_PROCESSED"
            self.publish_dir = self.base_dir / self.system_name / self.entity_name / "02_PUBLISH"
            self.metadata_dir = self.base_dir / "03_METADATA" / self.system_name / self.entity_name
            self.hash_fields = ["BatchNumber", "Operation", "TrackOutTime"]
        
        # 创建目录结构
        self._create_directories()
        
        # 数据血缘追踪
        self.data_lineage = {
            "process_id": f"etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "start_time": datetime.now().isoformat(),
            "source_files": [],
            "processed_files": [],
            "published_files": [],
            "transformations": [],
            "incremental_stats": {}
        }
        
        # 记录级变更检测
        self.processed_records_hash_file = self.metadata_dir / "processed_records_hash.parquet"
        self.watermark_file = self.metadata_dir / "watermark.json"
    
    def _setup_logging(self) -> logging.Logger:
        """设置日志配置"""
        log_config = self.config.get("logging", {})
        log_file = log_config.get("file", "../06_日志文件/etl_incremental.log")
        log_file = os.path.join(os.path.dirname(__file__), log_file) if not os.path.isabs(log_file) else log_file
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        
        logging.basicConfig(
            level=getattr(logging, log_config.get("level", "INFO")),
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        
        return logging.getLogger(__name__)
    
    def _create_directories(self):
        """创建两层架构目录结构"""
        for directory in [self.processed_dir, self.publish_dir, self.metadata_dir]:
            directory.mkdir(parents=True, exist_ok=True)
            self.logger.info(f"创建目录: {directory}")
    
    def _calculate_record_hash(self, batch_number: str, operation: str, trackout_time: str) -> str:
        """
        计算记录级哈希值用于变更检测
        
        Args:
            batch_number: 批次号
            operation: 工序
            trackout_time: 完成时间
            
        Returns:
            记录哈希值
        """
        # 使用配置文件中的哈希字段
        hash_values = []
        for field in self.hash_fields:
            if field == "BatchNumber":
                hash_values.append(str(batch_number or ""))
            elif field == "Operation":
                hash_values.append(str(operation or ""))
            elif field == "TrackOutTime":
                hash_values.append(str(trackout_time or ""))
            else:
                # 对于其他字段，这里暂时为空，未来可以扩展
                hash_values.append("")
        
        hash_input = "_".join(hash_values)
        return hashlib.md5(hash_input.encode('utf-8')).hexdigest()
    
    def _load_processed_hashes(self) -> Set[str]:
        """加载已处理记录的哈希值"""
        if self.processed_records_hash_file.exists():
            try:
                df = pd.read_parquet(self.processed_records_hash_file)
                return set(df['record_hash'].tolist())
            except Exception as e:
                self.logger.warning(f"加载已处理哈希失败: {e}")
        
        return set()
    
    def _save_processed_hashes(self, new_hashes: Set[str]):
        """保存已处理记录的哈希值"""
        try:
            # 创建新的哈希DataFrame
            new_hash_df = pd.DataFrame({'record_hash': list(new_hashes)})
            
            # 合并现有哈希
            existing_hashes = self._load_processed_hashes()
            all_hashes = existing_hashes.union(new_hashes)
            
            # 保存合并后的哈希
            final_hash_df = pd.DataFrame({'record_hash': list(all_hashes)})
            final_hash_df.to_parquet(self.processed_records_hash_file, compression='snappy', index=False)
            
            self.logger.info(f"保存记录哈希: {len(all_hashes)} 个记录")
            
        except Exception as e:
            self.logger.error(f"保存记录哈希失败: {e}")
    
    def _load_watermark(self) -> Dict:
        """加载处理水位线"""
        if self.watermark_file.exists():
            try:
                with open(self.watermark_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                self.logger.warning(f"加载水位线失败: {e}")
        
        return {
            "last_processed_date": None,
            "last_processed_time": None,
            "total_records_processed": 0
        }
    
    def _save_watermark(self, latest_date: str, latest_time: str, total_records: int):
        """保存处理水位线"""
        watermark = {
            "last_processed_date": latest_date,
            "last_processed_time": latest_time,
            "total_records_processed": total_records,
            "updated_at": datetime.now().isoformat()
        }
        
        with open(self.watermark_file, 'w', encoding='utf-8') as f:
            json.dump(watermark, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"更新水位线: {latest_date}, 总记录数: {total_records}")
    
    def _identify_new_records(self, mes_df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        识别新增和变更的记录
        
        Args:
            mes_df: 原始MES数据
            
        Returns:
            (新增记录DataFrame, 统计信息)
        """
        self.logger.info("开始记录级变更检测...")
        
        # 加载已处理记录哈希
        processed_hashes = self._load_processed_hashes()
        
        # 计算当前记录哈希
        mes_df = mes_df.copy()
        mes_df['record_hash'] = mes_df.apply(
            lambda row: self._calculate_record_hash(
                row.get('BatchNumber', ''),
                row.get('Operation', ''),
                str(row.get('TrackOutTime', ''))
            ),
            axis=1
        )
        
        # 识别新记录
        new_records_mask = ~mes_df['record_hash'].isin(processed_hashes)
        new_records_df = mes_df[new_records_mask].copy()
        
        # 统计信息
        stats = {
            "total_records": len(mes_df),
            "new_records": len(new_records_df),
            "duplicate_records": len(mes_df) - len(new_records_df),
            "duplicate_percentage": round((len(mes_df) - len(new_records_df)) / len(mes_df) * 100, 2)
        }
        
        self.logger.info(f"变更检测完成: 总记录 {stats['total_records']}, 新记录 {stats['new_records']}, 重复记录 {stats['duplicate_percentage']}%")
        
        # 移除临时哈希列
        new_records_df = new_records_df.drop('record_hash', axis=1)
        
        return new_records_df, stats
    
    def _process_to_cleaned_data(self, source_files: List[str]) -> Tuple[pd.DataFrame, Dict]:
        """
        处理数据到清洗后的状态
        
        Args:
            source_files: 源文件列表
            
        Returns:
            (清洗后的DataFrame, 处理统计信息)
        """
        self.logger.info("开始数据清洗处理...")
        
        # 加载原始数据
        try:
            # 直接使用原始脚本的主处理函数
            cleaned_df = process_all_data(self.config, force_full_refresh=True)  # 先全量处理，后续做增量检测
            
            if cleaned_df is None or cleaned_df.empty:
                self.logger.error("数据清洗失败")
                return pd.DataFrame(), {}
            
            # 识别新记录（基于哈希检测）
            new_records_df, incremental_stats = self._identify_new_records(cleaned_df)
            
            self.logger.info(f"数据清洗完成: {len(cleaned_df)} 行，识别新记录: {len(new_records_df)} 行")
            
            return new_records_df, incremental_stats
            
        except Exception as e:
            self.logger.error(f"数据清洗处理失败: {e}")
            return pd.DataFrame(), {}
    
    def _save_to_processed_layer(self, cleaned_df: pd.DataFrame, incremental_stats: Dict) -> List[str]:
        """
        保存清洗后的数据到PROCESSED层
        
        Args:
            cleaned_df: 清洗后的DataFrame
            incremental_stats: 增量处理统计信息
            
        Returns:
            保存的文件路径列表
        """
        self.logger.info("保存数据到PROCESSED层...")
        
        processed_files = []
        today = datetime.now()
        
        # 创建日期分区目录
        date_partition_dir = self.processed_dir / today.strftime("%Y/%m/%d")
        date_partition_dir.mkdir(parents=True, exist_ok=True)
        
        if not cleaned_df.empty:
            # 保存增量文件
            timestamp = today.strftime("%Y%m%d_%H%M%S")
            incremental_file = date_partition_dir / f"incremental_mes_batch_report_{timestamp}.parquet"
            
            cleaned_df.to_parquet(incremental_file, compression='snappy', index=False)
            self.logger.info(f"保存增量文件: {incremental_file}")
            
            processed_files.append(str(incremental_file))
            
            # 更新记录哈希
            new_hashes = set()
            for _, row in cleaned_df.iterrows():
                record_hash = self._calculate_record_hash(
                    row.get('BatchNumber', ''),
                    row.get('Operation', ''),
                    str(row.get('TrackOutTime', ''))
                )
                new_hashes.add(record_hash)
            
            self._save_processed_hashes(new_hashes)
            
            # 更新水位线
            if 'TrackOutDate' in cleaned_df.columns:
                latest_date = cleaned_df['TrackOutDate'].max().strftime('%Y-%m-%d')
                latest_time = cleaned_df['TrackOutTime'].max().strftime('%Y-%m-%d %H:%M:%S')
                watermark = self._load_watermark()
                total_records = watermark.get("total_records_processed", 0) + len(cleaned_df)
                self._save_watermark(latest_date, latest_time, total_records)
        
        # 生成处理元数据
        process_metadata = {
            "process_id": self.data_lineage["process_id"],
            "process_date": today.isoformat(),
            "incremental_stats": incremental_stats,
            "output_files": processed_files,
            "records_processed": len(cleaned_df),
            "transformations": [
                "record_level_change_detection",
                "data_cleaning",
                "field_mapping",
                "validation",
                "lt_calculation",
                "pt_calculation",
                "st_calculation",
                "completion_status_calculation"
            ]
        }
        
        metadata_path = date_partition_dir / f"process_metadata_{timestamp}.parquet"
        pd.DataFrame([process_metadata]).to_parquet(metadata_path, compression='snappy', index=False)
        
        return processed_files
    
    def _merge_incremental_to_full(self) -> str:
        """
        合并增量数据到全量文件
        
        Returns:
            合并后的全量文件路径
        """
        self.logger.info("合并增量数据到全量文件...")
        
        today = datetime.now()
        today_dir = self.processed_dir / today.strftime("%Y/%m/%d")
        
        # 收集所有增量文件
        all_incremental_files = []
        for date_dir in self.processed_dir.rglob("incremental_mes_batch_report_*.parquet"):
            all_incremental_files.append(date_dir)
        
        if not all_incremental_files:
            self.logger.warning("没有找到增量文件")
            return ""
        
        # 按时间排序并读取
        all_incremental_files.sort()
        all_data = []
        
        for file_path in all_incremental_files:
            try:
                df = pd.read_parquet(file_path)
                if not df.empty:
                    all_data.append(df)
                    self.logger.info(f"读取增量文件: {file_path} ({len(df)} 行)")
            except Exception as e:
                self.logger.warning(f"读取增量文件失败 {file_path}: {e}")
        
        if not all_data:
            self.logger.warning("没有有效的增量数据")
            return ""
        
        # 合并所有数据
        merged_df = pd.concat(all_data, ignore_index=True)
        
        # 去重（基于业务键）
        if 'BatchNumber' in merged_df.columns and 'Operation' in merged_df.columns and 'TrackOutTime' in merged_df.columns:
            before_dedup = len(merged_df)
            merged_df = merged_df.drop_duplicates(subset=['BatchNumber', 'Operation', 'TrackOutTime'], keep='last')
            after_dedup = len(merged_df)
            self.logger.info(f"去重: {before_dedup} -> {after_dedup} 行")
        
        # 保存全量文件
        timestamp = today.strftime("%Y%m%d_%H%M%S")
        full_file = today_dir / f"mes_batch_report_full_{timestamp}.parquet"
        
        merged_df.to_parquet(full_file, compression='snappy', index=False)
        self.logger.info(f"保存全量文件: {full_file} ({len(merged_df)} 行)")
        
        return str(full_file)
    
    def _publish_to_powerbi(self, full_file_path: str) -> List[str]:
        """
        发布数据到PUBLISH层供Power BI使用
        
        Args:
            full_file_path: 全量文件路径
            
        Returns:
            发布文件路径列表
        """
        self.logger.info("发布数据到PUBLISH层...")
        
        published_files = []
        
        if not full_file_path or not os.path.exists(full_file_path):
            self.logger.warning("没有有效的全量文件用于发布")
            return published_files
        
        # 读取全量数据
        try:
            full_df = pd.read_parquet(full_file_path)
        except Exception as e:
            self.logger.error(f"读取全量文件失败: {e}")
            return published_files
        
        today = datetime.now()
        
        # 创建latest目录
        latest_dir = self.publish_dir / "latest"
        latest_dir.mkdir(parents=True, exist_ok=True)
        
        # 创建daily目录
        daily_dir = self.publish_dir / "daily"
        daily_dir.mkdir(parents=True, exist_ok=True)
        
        # 保存latest文件（覆盖）
        latest_file = latest_dir / "mes_batch_report_latest.parquet"
        full_df.to_parquet(latest_file, compression='snappy', index=False)
        self.logger.info(f"保存latest文件: {latest_file}")
        
        published_files.append(str(latest_file))
        
        # 保存daily快照
        daily_file = daily_dir / f"mes_batch_report_{today.strftime('%Y%m%d')}.parquet"
        full_df.to_parquet(daily_file, compression='snappy', index=False)
        self.logger.info(f"保存daily快照: {daily_file}")
        
        published_files.append(str(daily_file))
        
        # 生成Power BI元数据
        powerbi_metadata = {
            "table_name": "mes_batch_report",
            "last_update": datetime.now().isoformat(),
            "process_id": self.data_lineage["process_id"],
            "total_records": len(full_df),
            "data_period": {
                "start": full_df['TrackOutDate'].min().strftime('%Y-%m-%d') if 'TrackOutDate' in full_df.columns and not full_df['TrackOutDate'].isna().all() else None,
                "end": full_df['TrackOutDate'].max().strftime('%Y-%m-%d') if 'TrackOutDate' in full_df.columns and not full_df['TrackOutDate'].isna().all() else None
            },
            "refresh_frequency": "daily",
            "powerbi_columns": [
                {"name": col, "type": str(full_df[col].dtype)}
                for col in full_df.columns
            ]
        }
        
        metadata_path = self.publish_dir / "powerbi_metadata.json"
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(powerbi_metadata, f, indent=2, ensure_ascii=False)
        
        self.data_lineage["published_files"] = [
            {
                "type": "latest",
                "path": str(latest_file),
                "rows": len(full_df)
            },
            {
                "type": "daily",
                "path": str(daily_file), 
                "rows": len(full_df)
            }
        ]
        
        return published_files
    
    def _save_data_lineage(self):
        """保存数据血缘信息"""
        self.data_lineage["end_time"] = datetime.now().isoformat()
        self.data_lineage["duration_seconds"] = (
            datetime.fromisoformat(self.data_lineage["end_time"]) - 
            datetime.fromisoformat(self.data_lineage["start_time"])
        ).total_seconds()
        
        lineage_path = self.metadata_dir / "data_lineage.json"
        
        # 读取现有血缘信息
        existing_lineage = []
        if lineage_path.exists():
            with open(lineage_path, 'r', encoding='utf-8') as f:
                existing_lineage = json.load(f)
        
        # 添加新的血缘信息
        existing_lineage.append(self.data_lineage)
        
        # 保留最近50条记录
        if len(existing_lineage) > 50:
            existing_lineage = existing_lineage[-50:]
        
        with open(lineage_path, 'w', encoding='utf-8') as f:
            json.dump(existing_lineage, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"数据血缘信息保存至: {lineage_path}")
    
    def process(self, refresh_mode: str = "incremental"):
        """
        执行完整的ETL处理流程
        
        Args:
            refresh_mode: 刷新模式 ("incremental" 或 "full")
        """
        try:
            self.logger.info("开始增量处理ETL...")
            
            # 1. 获取源文件
            source_files = [self.config.get("source", {}).get("mes_path", "")]
            self.logger.info(f"源文件: {source_files}")
            
            # 2. 数据清洗处理（包含增量检测）
            cleaned_df, incremental_stats = self._process_to_cleaned_data(source_files)
            
            # 3. 保存到PROCESSED层
            processed_files = self._save_to_processed_layer(cleaned_df, incremental_stats)
            
            # 4. 合并增量到全量文件
            full_file_path = self._merge_incremental_to_full()
            
            # 5. 发布到PUBLISH层
            published_files = self._publish_to_powerbi(full_file_path)
            
            # 6. 保存数据血缘
            self.data_lineage["incremental_stats"] = incremental_stats
            self._save_data_lineage()
            
            self.logger.info("增量处理ETL完成!")
            self.logger.info(f"增量统计: {incremental_stats}")
            self.logger.info(f"处理记录数: {len(cleaned_df)}")
            self.logger.info(f"生成文件数: {len(processed_files + published_files)}")
            
        except Exception as e:
            self.logger.error(f"ETL处理失败: {e}")
            raise

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='MES数据清洗ETL - 增量处理优化版本')
    parser.add_argument('--refresh-mode', choices=['incremental', 'full'], 
                       default='incremental', help='刷新模式')
    parser.add_argument('--config', type=str, 
                       default="../03_配置文件/config/config_mes_batch_report.yaml",
                       help='配置文件路径')
    
    args = parser.parse_args()
    
    # 如果没有指定参数，询问用户刷新模式
    if len(sys.argv) == 1:
        args.refresh_mode = prompt_refresh_mode()
    
    # 初始化处理器
    config_path = os.path.join(os.path.dirname(__file__), args.config)
    processor = IncrementalETLProcessor(config_path)
    
    # 执行ETL处理
    processor.process(refresh_mode=args.refresh_mode)

if __name__ == "__main__":
    main()
