#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pymysql
import clickhouse_connect
from typing import Dict, List, Optional
import logging
from datetime import datetime
import sys
import time
import yaml
import os
import traceback

from feishu_notifier import FeishuNotifier


class Config:
    """Configuration manager"""
    
    def __init__(self, config_file: str = 'conf.yaml'):
        self.config_file = config_file
        self.config = self._load_config()
        self._setup_logging()
        
    def _load_config(self) -> Dict:
        if not os.path.exists(self.config_file):
            raise FileNotFoundError(f"Configuration file not found: {self.config_file}")
        
        with open(self.config_file, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        return config
    
    def _setup_logging(self):
        log_config = self.config.get('logging', {})
        
        log_level = getattr(logging, log_config.get('level', 'INFO'))
        log_format = log_config.get('format', '%(asctime)s - %(levelname)s - %(message)s')
        date_format = log_config.get('date_format', '%Y-%m-%d %H:%M:%S')
        file_prefix = log_config.get('file_prefix', 'migration')
        console_output = log_config.get('console_output', True)
        
        handlers = []
        log_filename = f"{file_prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        handlers.append(logging.FileHandler(log_filename, encoding='utf-8'))
        
        if console_output:
            handlers.append(logging.StreamHandler(sys.stdout))
        
        logging.basicConfig(
            level=log_level,
            format=log_format,
            datefmt=date_format,
            handlers=handlers
        )
        
        self.logger = logging.getLogger(__name__)
        self.logger.info("Configuration loaded from: %s", self.config_file)
    
    @property
    def mysql_config(self) -> Dict:
        return self.config.get('mysql', {})
    
    @property
    def clickhouse_config(self) -> Dict:
        return self.config.get('clickhouse', {})
    
    @property
    def migration_config(self) -> Dict:
        return self.config.get('migration', {})
    
    @property
    def performance_config(self) -> Dict:
        return self.config.get('performance', {})
    
    @property
    def advanced_config(self) -> Dict:
        return self.config.get('advanced', {})
    
    @property
    def feishu_config(self) -> Dict:
        return self.config.get('feishu', {})


class MySQLToClickHouseMigration:
    """MySQL to ClickHouse Migration Tool - Optimized for Large Tables"""
    
    TYPE_MAPPING = {
        'tinyint': 'Int8',
        'smallint': 'Int16',
        'mediumint': 'Int32',
        'int': 'Int32',
        'integer': 'Int32',
        'bigint': 'Int64',
        'float': 'Float32',
        'double': 'Float64',
        'decimal': 'Decimal',
        'char': 'String',
        'varchar': 'String',
        'text': 'String',
        'tinytext': 'String',
        'mediumtext': 'String',
        'longtext': 'String',
        'date': 'Date',
        'datetime': 'DateTime',
        'timestamp': 'DateTime',
        'time': 'String',
        'year': 'Int16',
        'blob': 'String',
        'tinyblob': 'String',
        'mediumblob': 'String',
        'longblob': 'String',
        'json': 'String',
        'enum': 'String',
        'set': 'String'
    }
    
    def __init__(self, config: Config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.mysql_conn = None
        self.ch_client = None
        
        self.mysql_config = config.mysql_config
        self.clickhouse_config = config.clickhouse_config
        self.migration_config = config.migration_config
        self.performance_config = config.performance_config
        self.advanced_config = config.advanced_config
        
        # Initialize Feishu notifier from config
        self.notifier = FeishuNotifier(config.feishu_config)
        
        self.migration_stats = {
            'total_tables': 0,
            'success_tables': 0,
            'failed_tables': 0,
            'total_rows': 0,
            'total_time': 0,
            'table_details': []
        }
        
    def __enter__(self):
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        
    def connect(self):
        try:
            self.mysql_conn = pymysql.connect(
                host=self.mysql_config['host'],
                port=self.mysql_config.get('port', 3306),
                user=self.mysql_config['user'],
                password=self.mysql_config['password'],
                database=self.mysql_config['database'],
                charset=self.mysql_config.get('charset', 'utf8mb4'),
                cursorclass=pymysql.cursors.DictCursor,
                connect_timeout=self.performance_config.get('connection_timeout', 30),
                read_timeout=self.performance_config.get('read_timeout', 60)
            )
            self.logger.info("[SUCCESS] Connected to MySQL: %s:%d / %s", 
                           self.mysql_config['host'], 
                           self.mysql_config.get('port', 3306),
                           self.mysql_config['database'])
            
            self.ch_client = clickhouse_connect.get_client(
                host=self.clickhouse_config['host'],
                port=self.clickhouse_config.get('port', 8123),
                username=self.clickhouse_config.get('user', 'default'),
                password=self.clickhouse_config.get('password', ''),
                database=self.clickhouse_config['database'],
                connect_timeout=self.performance_config.get('connection_timeout', 30),
                send_receive_timeout=self.performance_config.get('read_timeout', 60)
            )
            self.logger.info("[SUCCESS] Connected to ClickHouse: %s:%d / %s", 
                           self.clickhouse_config['host'], 
                           self.clickhouse_config.get('port', 8123),
                           self.clickhouse_config['database'])
            
        except Exception as e:
            self.logger.error("[ERROR] Connection failed: %s", e)
            raise
            
    def close(self):
        if self.mysql_conn:
            self.mysql_conn.close()
            self.logger.info("[INFO] MySQL connection closed")
        if self.ch_client:
            self.ch_client.close()
            self.logger.info("[INFO] ClickHouse connection closed")
    
    def get_mysql_table_structure(self, table_name: str) -> List[Dict]:
        cursor = self.mysql_conn.cursor()
        cursor.execute(f"DESCRIBE `{table_name}`")
        columns = cursor.fetchall()
        cursor.close()
        self.logger.info("[INFO] Got table structure: %s (%d columns)", table_name, len(columns))
        return columns
        
    def get_primary_key(self, table_name: str) -> List[str]:
        cursor = self.mysql_conn.cursor()
        cursor.execute(f"""
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
            WHERE TABLE_SCHEMA = '{self.mysql_config['database']}'
            AND TABLE_NAME = '{table_name}'
            AND CONSTRAINT_NAME = 'PRIMARY'
            ORDER BY ORDINAL_POSITION
        """)
        primary_keys = [row['COLUMN_NAME'] for row in cursor.fetchall()]
        cursor.close()
        
        if primary_keys:
            self.logger.info("[INFO] Primary keys: %s", ', '.join(primary_keys))
        else:
            self.logger.warning("[WARNING] No primary key, using tuple()")
        return primary_keys
        
    def convert_mysql_type_to_clickhouse(self, mysql_type: str, is_nullable: str) -> str:
        base_type = mysql_type.split('(')[0].lower()
        is_unsigned = 'unsigned' in mysql_type.lower()
        ch_type = self.TYPE_MAPPING.get(base_type, 'String')
        
        if is_unsigned and ch_type.startswith('Int'):
            ch_type = 'U' + ch_type
        if base_type == 'decimal' and '(' in mysql_type:
            precision = mysql_type.split('(')[1].split(')')[0]
            ch_type = f'Decimal({precision})'
        if is_nullable == 'YES':
            ch_type = f'Nullable({ch_type})'
        return ch_type
        
    def generate_clickhouse_ddl(self, mysql_table: str, ch_table: str, 
                                columns: List[Dict], primary_keys: List[str]) -> str:
        column_defs = []
        for col in columns:
            field_name = col['Field']
            mysql_type = col['Type']
            is_nullable = col['Null']
            ch_type = self.convert_mysql_type_to_clickhouse(mysql_type, is_nullable)
            column_defs.append(f"    `{field_name}` {ch_type}")
            
        columns_ddl = ',\n'.join(column_defs)
        order_by = ', '.join([f'`{pk}`' for pk in primary_keys]) if primary_keys else 'tuple()'
        
        ddl = f"""
CREATE TABLE IF NOT EXISTS `{ch_table}`
(
{columns_ddl}
)
ENGINE = MergeTree()
ORDER BY ({order_by})
SETTINGS index_granularity = 8192
"""
        return ddl
        
    def drop_table_if_exists(self, table_name: str):
        if not self.advanced_config.get('drop_table_before_create', True):
            return
        try:
            self.ch_client.command(f"DROP TABLE IF EXISTS `{table_name}`")
            self.logger.info("[SUCCESS] Dropped old table: %s", table_name)
        except Exception as e:
            self.logger.error("[ERROR] Failed to drop table: %s", e)
            raise
            
    def create_clickhouse_table(self, ddl: str):
        try:
            self.ch_client.command(ddl)
            self.logger.info("[SUCCESS] Created ClickHouse table")
        except Exception as e:
            self.logger.error("[ERROR] Failed to create table: %s", e)
            raise
    
    def _format_time(self, seconds: float) -> str:
        if seconds < 0:
            return "N/A"
        if seconds < 60:
            return f"{seconds:.0f}s"
        elif seconds < 3600:
            m = int(seconds // 60)
            s = int(seconds % 60)
            return f"{m}m {s}s"
        else:
            h = int(seconds // 3600)
            m = int((seconds % 3600) // 60)
            s = int(seconds % 60)
            return f"{h}h {m}m {s}s"
    
    def _format_number(self, num: int) -> str:
        return f"{num:,}"
    
    def migrate_data(self, mysql_table: str, ch_table: str, batch_size: int = 10000):
        """
        Optimized data migration using SSCursor to prevent memory overflow
        """
        # ✅ 使用 SSCursor (Server Side Cursor) 进行流式读取
        cursor = self.mysql_conn.cursor(pymysql.cursors.SSCursor)
        
        try:
            self.logger.info("[INFO] Counting total rows...")
            # Count 操作使用普通游标（更快）
            count_cursor = self.mysql_conn.cursor()
            count_cursor.execute(f"SELECT COUNT(*) as cnt FROM `{mysql_table}`")
            result = count_cursor.fetchone()
            total_rows = result['cnt'] if isinstance(result, dict) else result[0]
            count_cursor.close()
            
            if total_rows == 0:
                if self.advanced_config.get('skip_empty_tables', True):
                    self.logger.warning("[WARNING] Empty table, skipping")
                    return 0, 0
            
            total_batches = (total_rows + batch_size - 1) // batch_size
            
            self.logger.info("[INFO] Migration plan:")
            self.logger.info("  - Total rows: %s", self._format_number(total_rows))
            self.logger.info("  - Batch size: %s", self._format_number(batch_size))
            self.logger.info("  - Total batches: %s", self._format_number(total_batches))
            
            # ✅ 获取列名（使用独立游标）
            schema_cursor = self.mysql_conn.cursor()
            schema_cursor.execute(f"SELECT * FROM `{mysql_table}` LIMIT 1")
            first_row = schema_cursor.fetchone()
            column_names = list(first_row.keys())
            schema_cursor.close()
            
            # ✅ 流式查询（关键改动 - 不会一次性加载所有数据到内存）
            self.logger.info("[INFO] Starting streaming query...")
            cursor.execute(f"SELECT * FROM `{mysql_table}`")
            
            migrated_rows = 0
            batch_data = []
            batch_count = 0
            start_time = time.time()
            last_log_time = start_time
            log_interval = self.migration_config.get('log_interval', 3)
            last_batch_count = 0
            bar_width = self.migration_config.get('progress_bar_width', 40)
            
            # ✅ 逐行读取（SSCursor 每次只读取一行到内存）
            while True:
                row = cursor.fetchone()
                if not row:
                    # 处理最后一批数据
                    if batch_data:
                        self.ch_client.insert(ch_table, batch_data, column_names=column_names)
                        migrated_rows += len(batch_data)
                        batch_count += 1
                        
                        elapsed_time = time.time() - start_time
                        avg_speed = migrated_rows / elapsed_time if elapsed_time > 0 else 0
                        progress_bar = '=' * bar_width
                        
                        self.logger.info("[PROGRESS] [%s] 100.00%% | %s/%s rows | "
                                       "Batch %s/%s | Speed: %s rows/s | Time: %s",
                                       progress_bar, self._format_number(migrated_rows), 
                                       self._format_number(total_rows), self._format_number(batch_count),
                                       self._format_number(total_batches), self._format_number(int(avg_speed)),
                                       self._format_time(elapsed_time))
                    break
                
                # ✅ 处理单行数据
                # 注意：SSCursor 返回 tuple，不是 dict
                row_data = []
                for i, col_name in enumerate(column_names):
                    value = row[i]
                    if isinstance(value, datetime):
                        value = value.strftime('%Y-%m-%d %H:%M:%S')
                    row_data.append(value)
                batch_data.append(row_data)
                
                # 批量插入
                if len(batch_data) >= batch_size:
                    self.ch_client.insert(ch_table, batch_data, column_names=column_names)
                    migrated_rows += len(batch_data)
                    batch_count += 1
                    
                    current_time = time.time()
                    elapsed_time = current_time - start_time
                    avg_speed = migrated_rows / elapsed_time if elapsed_time > 0 else 0
                    
                    should_log = (current_time - last_log_time >= log_interval) or \
                                (batch_count % 10 == 0) or (batch_count == 1)
                    
                    if should_log:
                        progress = (migrated_rows / total_rows) * 100
                        remaining_rows = total_rows - migrated_rows
                        eta_seconds = remaining_rows / avg_speed if avg_speed > 0 else 0
                        filled = int(bar_width * progress / 100)
                        progress_bar = '=' * filled + '>' + '.' * (bar_width - filled - 1)
                        
                        recent_interval = current_time - last_log_time
                        recent_batches = batch_count - last_batch_count
                        recent_rows = recent_batches * batch_size
                        current_speed = recent_rows / recent_interval if recent_interval > 0 else avg_speed
                        
                        self.logger.info("[PROGRESS] [%s] %.2f%% | %s/%s rows | "
                                       "Batch %s/%s | Speed: %s rows/s | ETA: %s",
                                       progress_bar, progress, 
                                       self._format_number(migrated_rows),
                                       self._format_number(total_rows),
                                       self._format_number(batch_count),
                                       self._format_number(total_batches),
                                       self._format_number(int(current_speed)),
                                       self._format_time(eta_seconds))
                        
                        last_log_time = current_time
                        last_batch_count = batch_count
                    
                    batch_data = []  # ✅ 清空批次数据，释放内存
            
            total_time = time.time() - start_time
            avg_speed = migrated_rows / total_time if total_time > 0 else 0
            
            self.logger.info("[SUCCESS] Data migration completed!")
            self.logger.info("[STATS] Rows: %s | Time: %s | Speed: %s rows/s",
                            self._format_number(migrated_rows),
                            self._format_time(total_time),
                            self._format_number(int(avg_speed)))
            
            return migrated_rows, avg_speed
        
        finally:
            cursor.close()  # ✅ 确保游标关闭
        
    def verify_migration(self, mysql_table: str, ch_table: str) -> bool:
        cursor = self.mysql_conn.cursor()
        cursor.execute(f"SELECT COUNT(*) as cnt FROM `{mysql_table}`")
        result = cursor.fetchone()
        mysql_count = result['cnt'] if isinstance(result, dict) else result[0]
        cursor.close()
        
        result = self.ch_client.query(f"SELECT COUNT(*) as cnt FROM `{ch_table}`")
        ch_count = result.first_row[0]
        
        self.logger.info("[VERIFY] MySQL: %s | ClickHouse: %s",
                        self._format_number(mysql_count),
                        self._format_number(ch_count))
        
        if mysql_count == ch_count:
            self.logger.info("[SUCCESS] Verification passed!")
            return True
        else:
            self.logger.error("[ERROR] Verification failed! Diff: %s",
                            self._format_number(abs(mysql_count - ch_count)))
            return False
            
    def migrate_table(self, mysql_table: str, ch_table: str, 
                     batch_size: Optional[int] = None, verify: Optional[bool] = None):
        if batch_size is None:
            batch_size = self.migration_config.get('default_batch_size', 10000)
        if verify is None:
            verify = self.migration_config.get('default_verify', True)
        
        start_time = time.time()
        self.logger.info("=" * 100)
        self.logger.info("[START] Table: %s -> %s", mysql_table, ch_table)
        self.logger.info("=" * 100)
        
        table_result = {
            'mysql_table': mysql_table,
            'ch_table': ch_table,
            'rows': 0,
            'time_used': 0,
            'speed': 0,
            'status': 'failed',
            'verified': False,
            'error': None
        }
        
        try:
            self.logger.info("[STEP 1/5] Getting table structure...")
            columns = self.get_mysql_table_structure(mysql_table)
            primary_keys = self.get_primary_key(mysql_table)
            
            self.logger.info("[STEP 2/5] Dropping old table...")
            self.drop_table_if_exists(ch_table)
            
            self.logger.info("[STEP 3/5] Creating new table...")
            ddl = self.generate_clickhouse_ddl(mysql_table, ch_table, columns, primary_keys)
            self.create_clickhouse_table(ddl)
            
            self.logger.info("[STEP 4/5] Migrating data...")
            migrated_rows, avg_speed = self.migrate_data(mysql_table, ch_table, batch_size)
            
            table_result['rows'] = migrated_rows
            table_result['speed'] = avg_speed
            
            if verify:
                self.logger.info("[STEP 5/5] Verifying...")
                verified_success = self.verify_migration(mysql_table, ch_table)
                table_result['verified'] = verified_success
            
            elapsed_time = time.time() - start_time
            table_result['time_used'] = elapsed_time
            table_result['status'] = 'success'
            
            self.logger.info("=" * 100)
            self.logger.info("[SUCCESS] Completed in %s", self._format_time(elapsed_time))
            self.logger.info("=" * 100)
            
            return table_result
            
        except Exception as e:
            elapsed_time = time.time() - start_time
            table_result['time_used'] = elapsed_time
            table_result['error'] = str(e)
            
            self.logger.error("[ERROR] Failed: %s", e)
            self.logger.error("=" * 100)
            
            if not self.advanced_config.get('continue_on_error', False):
                raise
            else:
                return table_result
    
    def migrate_all_tables(self):
        tables = self.migration_config.get('tables', [])
        
        if not tables:
            self.logger.warning("[WARNING] No tables configured")
            return
        
        total_tables = len(tables)
        self.migration_stats['total_tables'] = total_tables
        
        self.logger.info("[INFO] Starting migration of %d table(s)", total_tables)
        
        # Send Feishu start notification
        self.notifier.notify_start(
            tables=tables,
            mysql_db=self.mysql_config['database'],
            clickhouse_db=self.clickhouse_config['database']
        )
        
        overall_start_time = time.time()
        
        for idx, table_config in enumerate(tables, 1):
            mysql_table = table_config.get('mysql_table')
            ch_table = table_config.get('ch_table')
            batch_size = table_config.get('batch_size')
            verify = table_config.get('verify')
            
            if not mysql_table or not ch_table:
                self.logger.error("[ERROR] Invalid config: %s", table_config)
                continue
            
            self.logger.info("")
            self.logger.info("=" * 100)
            self.logger.info("[TABLE %d/%d] %s -> %s", idx, total_tables, mysql_table, ch_table)
            self.logger.info("=" * 100)
            
            try:
                table_result = self.migrate_table(mysql_table, ch_table, batch_size, verify)
                
                if table_result['status'] == 'success':
                    self.migration_stats['success_tables'] += 1
                else:
                    self.migration_stats['failed_tables'] += 1
                
                self.migration_stats['total_rows'] += table_result['rows']
                self.migration_stats['table_details'].append(table_result)
                
            except Exception as e:
                self.migration_stats['failed_tables'] += 1
                self.logger.error("[ERROR] Failed table %s: %s", mysql_table, e)
                
                # Send Feishu failure notification
                self.notifier.notify_failure({
                    'failed_table': mysql_table,
                    'error_message': str(e),
                    'total_tables': total_tables,
                    'completed_tables': idx - 1,
                    'traceback': traceback.format_exc()
                })
                
                if not self.advanced_config.get('continue_on_error', False):
                    raise
        
        overall_time = time.time() - overall_start_time
        self.migration_stats['total_time'] = overall_time
        
        if overall_time > 0 and self.migration_stats['total_rows'] > 0:
            self.migration_stats['avg_speed'] = self.migration_stats['total_rows'] / overall_time
        else:
            self.migration_stats['avg_speed'] = 0
        
        # Send Feishu success notification
        if self.migration_stats['failed_tables'] == 0:
            self.notifier.notify_success(self.migration_stats)
        else:
            self.logger.warning("[WARNING] Completed with %d failures", 
                              self.migration_stats['failed_tables'])


def main():
    config = Config('conf.yaml')
    
    with MySQLToClickHouseMigration(config) as migration:
        migration.migrate_all_tables()


if __name__ == '__main__':
    main()
