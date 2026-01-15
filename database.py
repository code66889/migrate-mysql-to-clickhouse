#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3
import json
from datetime import datetime
from typing import Dict, List, Optional
import os


class TaskDatabase:
    """SQLite数据库管理类，用于记录迁移任务"""
    
    def __init__(self, db_path: str = 'migration_tasks.db'):
        self.db_path = db_path
        self._init_database()
    
    def _init_database(self):
        """初始化数据库表"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 任务表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_name TEXT,
                config_snapshot TEXT,
                status TEXT,
                start_time TEXT,
                end_time TEXT,
                total_tables INTEGER DEFAULT 0,
                success_tables INTEGER DEFAULT 0,
                failed_tables INTEGER DEFAULT 0,
                total_rows INTEGER DEFAULT 0,
                total_time REAL DEFAULT 0,
                error_message TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # 任务日志表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS task_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id INTEGER,
                log_level TEXT,
                log_message TEXT,
                log_time TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (task_id) REFERENCES tasks (id)
            )
        ''')
        
        # 表迁移详情表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS table_migrations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id INTEGER,
                mysql_table TEXT,
                ch_table TEXT,
                status TEXT,
                rows INTEGER DEFAULT 0,
                time_used REAL DEFAULT 0,
                speed REAL DEFAULT 0,
                verified BOOLEAN DEFAULT 0,
                error_message TEXT,
                FOREIGN KEY (task_id) REFERENCES tasks (id)
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def create_task(self, task_name: str, config_snapshot: dict) -> int:
        """创建新任务"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO tasks (task_name, config_snapshot, status, start_time)
            VALUES (?, ?, ?, ?)
        ''', (
            task_name,
            json.dumps(config_snapshot, ensure_ascii=False),
            'running',
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ))
        
        task_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return task_id
    
    def update_task_status(self, task_id: int, status: str, 
                          stats: Optional[Dict] = None,
                          error_message: Optional[str] = None):
        """更新任务状态"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        update_fields = ['status = ?']
        values = [status]
        
        if status == 'completed' or status == 'failed':
            update_fields.append('end_time = ?')
            values.append(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        
        if stats:
            if 'total_tables' in stats:
                update_fields.append('total_tables = ?')
                values.append(stats['total_tables'])
            if 'success_tables' in stats:
                update_fields.append('success_tables = ?')
                values.append(stats['success_tables'])
            if 'failed_tables' in stats:
                update_fields.append('failed_tables = ?')
                values.append(stats['failed_tables'])
            if 'total_rows' in stats:
                update_fields.append('total_rows = ?')
                values.append(stats['total_rows'])
            if 'total_time' in stats:
                update_fields.append('total_time = ?')
                values.append(stats['total_time'])
        
        if error_message:
            update_fields.append('error_message = ?')
            values.append(error_message)
        
        values.append(task_id)
        
        cursor.execute(f'''
            UPDATE tasks 
            SET {', '.join(update_fields)}
            WHERE id = ?
        ''', values)
        
        conn.commit()
        conn.close()
    
    def add_log(self, task_id: int, log_level: str, log_message: str):
        """添加任务日志"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO task_logs (task_id, log_level, log_message, log_time)
            VALUES (?, ?, ?, ?)
        ''', (
            task_id,
            log_level,
            log_message,
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ))
        
        conn.commit()
        conn.close()
    
    def add_table_migration(self, task_id: int, mysql_table: str, ch_table: str,
                           status: str, rows: int = 0, time_used: float = 0,
                           speed: float = 0, verified: bool = False,
                           error_message: Optional[str] = None):
        """添加表迁移记录"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO table_migrations 
            (task_id, mysql_table, ch_table, status, rows, time_used, speed, verified, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            task_id,
            mysql_table,
            ch_table,
            status,
            rows,
            time_used,
            speed,
            1 if verified else 0,
            error_message
        ))
        
        conn.commit()
        conn.close()
    
    def get_task(self, task_id: int) -> Optional[Dict]:
        """获取任务详情"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM tasks WHERE id = ?', (task_id,))
        row = cursor.fetchone()
        conn.close()
        
        if row:
            task = dict(row)
            task['config_snapshot'] = json.loads(task['config_snapshot'])
            return task
        return None
    
    def get_all_tasks(self, limit: int = 50) -> List[Dict]:
        """获取所有任务列表"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM tasks 
            ORDER BY created_at DESC 
            LIMIT ?
        ''', (limit,))
        
        rows = cursor.fetchall()
        conn.close()
        
        tasks = []
        for row in rows:
            task = dict(row)
            task['config_snapshot'] = json.loads(task['config_snapshot'])
            tasks.append(task)
        
        return tasks
    
    def get_task_logs(self, task_id: int, limit: int = 1000) -> List[Dict]:
        """获取任务日志"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM task_logs 
            WHERE task_id = ? 
            ORDER BY log_time DESC 
            LIMIT ?
        ''', (task_id, limit))
        
        rows = cursor.fetchall()
        conn.close()
        
        return [dict(row) for row in rows]
    
    def get_table_migrations(self, task_id: int) -> List[Dict]:
        """获取表迁移详情"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM table_migrations 
            WHERE task_id = ? 
            ORDER BY id
        ''', (task_id,))
        
        rows = cursor.fetchall()
        conn.close()
        
        result = []
        for row in rows:
            r = dict(row)
            r['verified'] = bool(r['verified'])
            result.append(r)
        
        return result

