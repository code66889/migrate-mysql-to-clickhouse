#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from flask import Flask, render_template, request, jsonify, redirect, url_for
import yaml
import os
import threading
import traceback
from datetime import datetime
import logging
import sys

from database import TaskDatabase
from mysql_to_clickhouse import Config, MySQLToClickHouseMigration

app = Flask(__name__)
app.config['SECRET_KEY'] = 'migration-tool-secret-key'

db = TaskDatabase()


class TaskLogger:
    """自定义日志处理器，将日志写入数据库"""
    
    def __init__(self, task_id: int, db: TaskDatabase):
        self.task_id = task_id
        self.db = db
        self.logger = logging.getLogger(f'task_{task_id}')
        self.logger.setLevel(logging.INFO)
        
        # 创建自定义处理器
        handler = TaskLogHandler(self.task_id, self.db)
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        
        self.logger.addHandler(handler)
        self.logger.propagate = False
    
    def get_logger(self):
        return self.logger


class TaskLogHandler(logging.Handler):
    """自定义日志处理器"""
    
    def __init__(self, task_id: int, db: TaskDatabase):
        super().__init__()
        self.task_id = task_id
        self.db = db
    
    def emit(self, record):
        try:
            log_level = record.levelname
            log_message = self.format(record)
            self.db.add_log(self.task_id, log_level, log_message)
        except Exception:
            pass


def run_migration_task(task_id: int, config_dict: dict):
    """在后台线程中运行迁移任务"""
    try:
        # 保存配置到临时文件
        temp_config_file = f'conf_task_{task_id}.yaml'
        with open(temp_config_file, 'w', encoding='utf-8') as f:
            yaml.dump(config_dict, f, allow_unicode=True, default_flow_style=False)
        
        # 创建任务日志记录器
        task_logger = TaskLogger(task_id, db)
        logger = task_logger.get_logger()
        
        # 更新标准输出，让原有代码的日志也能记录
        original_handlers = logging.root.handlers[:]
        logging.root.handlers = []
        
        # 创建新的处理器
        handler = TaskLogHandler(task_id, db)
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logging.root.addHandler(handler)
        
        try:
            # 加载配置并运行迁移
            config = Config(temp_config_file)
            
            with MySQLToClickHouseMigration(config) as migration:
                # 重定向migration的logger
                migration.logger = logger
                
                # 运行迁移
                migration.migrate_all_tables()
                
                # 更新任务状态
                stats = migration.migration_stats
                db.update_task_status(
                    task_id,
                    'completed',
                    stats={
                        'total_tables': stats.get('total_tables', 0),
                        'success_tables': stats.get('success_tables', 0),
                        'failed_tables': stats.get('failed_tables', 0),
                        'total_rows': stats.get('total_rows', 0),
                        'total_time': stats.get('total_time', 0)
                    }
                )
                
                # 记录表迁移详情
                for table_detail in stats.get('table_details', []):
                    db.add_table_migration(
                        task_id,
                        table_detail.get('mysql_table', ''),
                        table_detail.get('ch_table', ''),
                        table_detail.get('status', 'failed'),
                        table_detail.get('rows', 0),
                        table_detail.get('time_used', 0),
                        table_detail.get('speed', 0),
                        table_detail.get('verified', False),
                        table_detail.get('error')
                    )
                
                logger.info(f"任务 {task_id} 完成")
        
        except Exception as e:
            error_msg = str(e)
            error_traceback = traceback.format_exc()
            logger.error(f"任务失败: {error_msg}\n{error_traceback}")
            
            db.update_task_status(
                task_id,
                'failed',
                error_message=error_msg
            )
        
        finally:
            # 恢复原始日志处理器
            logging.root.handlers = original_handlers
            
            # 删除临时配置文件
            if os.path.exists(temp_config_file):
                os.remove(temp_config_file)
    
    except Exception as e:
        db.update_task_status(
            task_id,
            'failed',
            error_message=f"任务启动失败: {str(e)}"
        )


@app.route('/')
def index():
    """主页 - 显示配置编辑界面"""
    config_file = 'conf.yaml'
    config = {}
    
    if os.path.exists(config_file):
        with open(config_file, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f) or {}
    
    breadcrumb = [
        {'text': '配置管理', 'url': None}
    ]
    return render_template('index.html', config=config, breadcrumb=breadcrumb)


@app.route('/api/config', methods=['GET'])
def get_config():
    """获取当前配置"""
    config_file = 'conf.yaml'
    config = {}
    
    if os.path.exists(config_file):
        with open(config_file, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f) or {}
    
    return jsonify(config)


@app.route('/api/config', methods=['POST'])
def save_config():
    """保存配置"""
    try:
        config = request.json
        
        # 保存到conf.yaml
        with open('conf.yaml', 'w', encoding='utf-8') as f:
            yaml.dump(config, f, allow_unicode=True, default_flow_style=False)
        
        return jsonify({'success': True, 'message': '配置保存成功'})
    
    except Exception as e:
        return jsonify({'success': False, 'message': f'保存失败: {str(e)}'}), 500


@app.route('/api/task/start', methods=['POST'])
def start_task():
    """启动迁移任务"""
    try:
        config = request.json.get('config')
        task_name = request.json.get('task_name', f'迁移任务_{datetime.now().strftime("%Y%m%d_%H%M%S")}')
        
        if not config:
            return jsonify({'success': False, 'message': '配置不能为空'}), 400
        
        # 创建任务记录
        task_id = db.create_task(task_name, config)
        
        # 在后台线程中运行任务
        thread = threading.Thread(target=run_migration_task, args=(task_id, config))
        thread.daemon = True
        thread.start()
        
        return jsonify({
            'success': True,
            'message': '任务已启动',
            'task_id': task_id
        })
    
    except Exception as e:
        return jsonify({'success': False, 'message': f'启动失败: {str(e)}'}), 500


@app.route('/tasks')
def tasks():
    """任务列表页面"""
    breadcrumb = [
        {'text': '任务历史', 'url': None}
    ]
    return render_template('tasks.html', breadcrumb=breadcrumb)


@app.route('/api/tasks', methods=['GET'])
def get_tasks():
    """获取任务列表"""
    limit = request.args.get('limit', 50, type=int)
    tasks = db.get_all_tasks(limit)
    return jsonify(tasks)


@app.route('/task/<int:task_id>')
def task_detail(task_id):
    """任务详情页面"""
    task = db.get_task(task_id)
    if not task:
        return "任务不存在", 404
    
    breadcrumb = [
        {'text': '任务历史', 'url': '/tasks'},
        {'text': f'任务 #{task_id}', 'url': None}
    ]
    return render_template('task_detail.html', task=task, task_id=task_id, breadcrumb=breadcrumb)


@app.route('/api/task/<int:task_id>', methods=['GET'])
def get_task(task_id):
    """获取任务详情"""
    task = db.get_task(task_id)
    if not task:
        return jsonify({'error': '任务不存在'}), 404
    
    logs = db.get_task_logs(task_id, limit=1000)
    table_migrations = db.get_table_migrations(task_id)
    
    return jsonify({
        'task': task,
        'logs': logs,
        'table_migrations': table_migrations
    })


@app.route('/api/tasks/recent', methods=['GET'])
def get_recent_tasks():
    """获取最近的任务（用于配置页面显示）"""
    limit = request.args.get('limit', 10, type=int)
    tasks = db.get_all_tasks(limit)
    return jsonify(tasks)


if __name__ == '__main__':
    print("=" * 60)
    print("MySQL to ClickHouse 迁移工具 Web 界面")
    print("=" * 60)
    print("访问地址: http://127.0.0.1:5000")
    print("=" * 60)
    app.run(host='0.0.0.0', port=5000, debug=True)

