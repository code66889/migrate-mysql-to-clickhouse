#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import logging
from typing import Dict, List, Optional
from datetime import datetime


class FeishuNotifier:
    """Feishu notification handler - Fixed version with proper line breaks"""
    
    def __init__(self, config: Dict):
        """Initialize from config dictionary"""
        self.config = config
        self.enabled = config.get('enabled', False)
        self.webhook_url = config.get('webhook_url', '')
        self.logger = logging.getLogger(__name__)
        
        self.notify_on_start = config.get('notify_on_start', True)
        self.notify_on_success = config.get('notify_on_success', True)
        self.notify_on_failure = config.get('notify_on_failure', True)
        
        self.env_name = config.get('env_name', 'Production')
        self.project_name = config.get('project_name', 'Data Migration')
        self.mention_users = config.get('mention_users', [])
        self.mention_all = config.get('mention_all', False)
        
        if self.enabled and not self.webhook_url:
            self.logger.warning("[WARNING] Feishu enabled but webhook_url not configured")
            self.enabled = False
        elif self.enabled:
            self.logger.info("[INFO] Feishu notification enabled")
    
    def send_card(self, title: str, content: Dict, color: str = "blue") -> bool:
        """Send message card to Feishu"""
        if not self.enabled:
            return False
        
        try:
            card = {
                "msg_type": "interactive",
                "card": {
                    "config": {
                        "wide_screen_mode": True
                    },
                    "header": {
                        "title": {
                            "tag": "plain_text",
                            "content": title
                        },
                        "template": color
                    },
                    "elements": content.get('elements', [])
                }
            }
            
            response = requests.post(
                self.webhook_url,
                json=card,
                headers={'Content-Type': 'application/json'},
                timeout=10
            )
            
            result = response.json()
            
            if result.get('code') == 0:
                self.logger.info("[FEISHU] Message sent successfully")
                return True
            else:
                self.logger.error("[FEISHU] Failed to send: %s", result.get('msg'))
                return False
                
        except Exception as e:
            self.logger.error("[FEISHU] Error: %s", e)
            return False
    
    def _build_mention_element(self) -> Optional[Dict]:
        """Build mention element"""
        if not self.mention_users and not self.mention_all:
            return None
        
        if self.mention_all:
            return {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": "<at id=all></at>"
                }
            }
        
        mentions = " ".join([f"<at id={uid}></at>" for uid in self.mention_users])
        return {
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": mentions
            }
        }
    
    def _format_number(self, num: int) -> str:
        """Format number with thousand separators"""
        return f"{num:,}"
    
    def _format_time(self, seconds: float) -> str:
        """Format seconds to readable time"""
        if seconds < 60:
            return f"{seconds:.0f}s"
        elif seconds < 3600:
            m = int(seconds // 60)
            s = int(seconds % 60)
            return f"{m}m {s}s"
        else:
            h = int(seconds // 3600)
            m = int((seconds % 3600) // 60)
            return f"{h}h {m}m"
    
    def notify_start(self, tables: List[Dict], mysql_db: str, clickhouse_db: str):
        """Notify migration start"""
        if not self.notify_on_start:
            return
        
        title = f"[START] {self.project_name}"
        
        # Build table list
        table_list = []
        for idx, table in enumerate(tables, 1):
            mt = table.get('mysql_table', 'N/A')
            ct = table.get('ch_table', 'N/A')
            bs = table.get('batch_size', 'default')
            table_list.append(f"{idx}. **{mt}** -> **{ct}** (batch: {bs})")
        
        # Use proper line breaks for Feishu
        elements = [
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": (
                        f"**Environment**: {self.env_name}\n\n"
                        f"**Time**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                        f"**Source DB**: {mysql_db}\n\n"
                        f"**Target DB**: {clickhouse_db}\n\n"
                        f"**Tables Count**: {len(tables)}"
                    )
                }
            },
            {"tag": "hr"},
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": "**Table List**:\n\n" + "\n\n".join(table_list)
                }
            }
        ]
        
        mention = self._build_mention_element()
        if mention:
            elements.append({"tag": "hr"})
            elements.append(mention)
        
        self.send_card(title, {"elements": elements}, color="blue")
    
    def notify_success(self, summary: Dict):
        """Notify migration success"""
        if not self.notify_on_success:
            return
        
        title = f"[SUCCESS] {self.project_name}"
        
        total = summary.get('total_tables', 0)
        success = summary.get('success_tables', 0)
        rows = summary.get('total_rows', 0)
        time_used = summary.get('total_time', 0)
        speed = summary.get('avg_speed', 0)
        
        # Build table details
        table_details = []
        for info in summary.get('table_details', []):
            mt = info.get('mysql_table', 'N/A')
            r = info.get('rows', 0)
            t = info.get('time_used', 0)
            s = info.get('speed', 0)
            st = info.get('status', 'unknown')
            
            icon = "[OK]" if st == "success" else "[FAIL]"
            table_details.append(
                f"{icon} **{mt}**: "
                f"{self._format_number(r)} rows, "
                f"{self._format_time(t)}, "
                f"{self._format_number(int(s))} rows/s"
            )
        
        elements = [
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": (
                        f"**Environment**: {self.env_name}\n\n"
                        f"**Complete Time**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                        f"**Success Tables**: {success}/{total}\n\n"
                        f"**Total Rows**: {self._format_number(rows)}\n\n"
                        f"**Total Time**: {self._format_time(time_used)}\n\n"
                        f"**Avg Speed**: {self._format_number(int(speed))} rows/s"
                    )
                }
            }
        ]
        
        if table_details:
            elements.append({"tag": "hr"})
            elements.append({
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": "**Migration Details**:\n\n" + "\n\n".join(table_details)
                }
            })
        
        mention = self._build_mention_element()
        if mention:
            elements.append({"tag": "hr"})
            elements.append(mention)
        
        self.send_card(title, {"elements": elements}, color="green")
    
    def notify_failure(self, error_info: Dict):
        """Notify migration failure"""
        if not self.notify_on_failure:
            return
        
        title = f"[FAILURE] {self.project_name}"
        
        failed = error_info.get('failed_table', 'N/A')
        error = error_info.get('error_message', 'Unknown error')
        total = error_info.get('total_tables', 0)
        completed = error_info.get('completed_tables', 0)
        tb = error_info.get('traceback', '')
        
        elements = [
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": (
                        f"**Environment**: {self.env_name}\n\n"
                        f"**Failed Time**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                        f"**Failed Table**: {failed}\n\n"
                        f"**Progress**: {completed}/{total} tables"
                    )
                }
            },
            {"tag": "hr"},
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"**Error Message**:\n\n```\n{error}\n```"
                }
            }
        ]
        
        if tb:
            if len(tb) > 1000:
                tb = tb[:1000] + "\n... (truncated)"
            
            elements.append({"tag": "hr"})
            elements.append({
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"**Traceback**:\n\n```\n{tb}\n```"
                }
            })
        
        mention = self._build_mention_element()
        if mention:
            elements.append({"tag": "hr"})
            elements.append(mention)
        
        self.send_card(title, {"elements": elements}, color="red")
