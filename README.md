# MySQL to ClickHouse 数据迁移工具

[![Python](https://img.shields.io/badge/Python-3.7+-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Flask](https://img.shields.io/badge/Flask-3.0-blue.svg)](https://flask.palletsprojects.com/)

一个功能强大、易于使用的 MySQL 到 ClickHouse 数据迁移工具，支持 Web 界面管理和命令行执行，适用于大规模数据迁移场景。

## ✨ 功能特性

### 核心功能
- 🚀 **高性能迁移**：采用流式读取和批量插入，支持大规模数据迁移
- 📊 **Web 管理界面**：现代化的 Web UI，可视化配置和任务管理
- 📝 **任务历史记录**：完整的任务执行历史，支持日志查看和状态跟踪
- ✅ **数据验证**：自动验证迁移数据的完整性和一致性
- 🔔 **飞书通知**：支持飞书机器人通知，实时了解任务状态

### 技术特性
- **流式处理**：使用 SSCursor 避免内存溢出，支持超大表迁移
- **断点续传**：支持错误处理和继续执行
- **性能优化**：可配置批次大小、连接超时等参数
- **类型映射**：自动处理 MySQL 到 ClickHouse 的数据类型转换
- **表结构同步**：自动创建 ClickHouse 表结构


## 技术架构

### 系统架构图

```mermaid
graph TB
    subgraph "用户界面层"
        A[Web 管理界面<br/>Flask + HTML/CSS/JS]
        B[命令行接口<br/>Python CLI]
    end
    
    subgraph "应用服务层"
        C[配置管理<br/>Config Manager]
        D[任务调度<br/>Task Scheduler]
        E[数据迁移引擎<br/>Migration Engine]
        F[任务历史管理<br/>Task History]
    end
    
    subgraph "数据存储层"
        G[(SQLite 数据库<br/>任务记录/日志)]
        H[配置文件<br/>conf.yaml]
    end
    
    subgraph "数据源"
        I[(MySQL 数据库<br/>源数据)]
    end
    
    subgraph "数据目标"
        J[(ClickHouse 数据库<br/>目标数据)]
    end
    
    subgraph "通知服务"
        K[飞书机器人<br/>Feishu Notifier]
    end
    
    A --> C
    A --> D
    A --> F
    B --> C
    B --> D
    
    C --> H
    D --> E
    D --> F
    F --> G
    
    E --> I
    E --> J
    E --> K
    
    style A fill:#6366f1,stroke:#4f46e5,color:#fff
    style B fill:#6366f1,stroke:#4f46e5,color:#fff
    style E fill:#10b981,stroke:#059669,color:#fff
    style I fill:#f59e0b,stroke:#d97706,color:#fff
    style J fill:#3b82f6,stroke:#2563eb,color:#fff
```

### 数据迁移流程

```mermaid
sequenceDiagram
    participant User as 用户
    participant Web as Web界面
    participant Engine as 迁移引擎
    participant MySQL as MySQL数据库
    participant CH as ClickHouse数据库
    participant DB as SQLite数据库
    participant Feishu as 飞书通知
    
    User->>Web: 1. 配置数据库连接
    User->>Web: 2. 添加迁移表
    User->>Web: 3. 启动迁移任务
    
    Web->>DB: 创建任务记录
    Web->>Engine: 启动迁移任务
    
    Engine->>Feishu: 发送开始通知
    Engine->>MySQL: 获取表结构
    Engine->>CH: 创建目标表
    
    loop 批量迁移数据
        Engine->>MySQL: 流式读取数据(SSCursor)
        Engine->>CH: 批量插入数据
        Engine->>DB: 记录进度日志
    end
    
    Engine->>MySQL: 验证数据行数
    Engine->>CH: 验证数据行数
    Engine->>DB: 更新任务状态
    Engine->>Feishu: 发送完成通知
    
    Web->>DB: 查询任务详情
    Web->>User: 显示迁移结果
```




## 📦 安装

### 环境要求
- Python 3.7+
- MySQL 数据库
- ClickHouse 数据库

### 安装步骤

1. **克隆仓库**
```bash
git clone https://github.com/your-username/mysql_to_clickhouse.git
cd mysql_to_clickhouse
```

2. **安装依赖**
```bash
pip install -r requirements.txt
```

3. **配置数据库连接**
```bash
cp conf.yaml-template conf.yaml
# 编辑 conf.yaml 文件，填写数据库连接信息
```

## 🚀 快速开始

### 方式一：Web 界面（推荐）

1. **启动 Web 服务**
```bash
python app.py
```

2. **访问 Web 界面**
打开浏览器访问：`http://127.0.0.1:5000`
<img width="1814" height="1277" alt="image" src="https://github.com/user-attachments/assets/dbf6f085-5658-4f35-80cf-b29641e30695" />

4. **配置和启动**
   - 在配置页面填写 MySQL 和 ClickHouse 连接信息
   - 添加要迁移的表
   - 点击"启动迁移任务"开始迁移

### 方式二：命令行

1. **编辑配置文件**
```bash
vim conf.yaml
```

2. **运行迁移**
```bash
python mysql_to_clickhouse.py
```

## 📖 配置说明

### 配置文件结构

配置文件 `conf.yaml` 包含以下主要部分：

```yaml
# MySQL 数据库配置
mysql:
  host: "your-mysql-host"
  port: 3306
  user: "your-username"
  password: "your-password"
  database: "your-database"
  charset: "utf8mb4"

# ClickHouse 数据库配置
clickhouse:
  host: "your-clickhouse-host"
  port: 8123
  user: "default"
  password: "your-password"
  database: "your-database"

# 迁移任务配置
migration:
  tables:
    - mysql_table: "source_table"
      ch_table: "target_table"
      batch_size: 10000
      verify: true
```

详细配置说明请参考 `conf.yaml-template` 文件。

## 🎯 使用场景

- **数据仓库迁移**：将 MySQL 数据迁移到 ClickHouse 进行分析
- **数据同步**：定期同步 MySQL 数据到 ClickHouse
- **数据备份**：将 MySQL 数据备份到 ClickHouse
- **性能优化**：将查询频繁的数据迁移到 ClickHouse 提升性能

## 📊 Web 界面功能

### 配置管理
- 可视化配置数据库连接
- 批量配置迁移表
- 实时保存配置

### 任务管理
- 查看所有迁移任务历史
- 实时查看任务执行状态
- 查看详细的执行日志
- 查看每个表的迁移详情
<img width="1444" height="369" alt="image" src="https://github.com/user-attachments/assets/46ed1b11-de08-4be1-b5fe-cc3c0376241b" />

<img width="1426" height="402" alt="image" src="https://github.com/user-attachments/assets/cc71fa47-127b-4b0d-a56b-b5847f9fd430" />


### 任务详情
- 任务基本信息（状态、时间、统计）
- 表迁移详情（行数、速度、验证结果）
- 完整执行日志
- 配置快照
<img width="1449" height="760" alt="image" src="https://github.com/user-attachments/assets/ab2753a8-e3dc-4f2c-8b86-b89be448576d" />


## 🔧 高级功能

### 性能调优
- 调整批次大小：根据数据量和网络情况调整 `batch_size`
- 连接超时设置：配置 `connection_timeout` 和 `read_timeout`
- MySQL 获取大小：调整 `mysql_fetch_size` 优化读取性能
<img width="912" height="250" alt="image" src="https://github.com/user-attachments/assets/0d5880aa-7a51-4c87-a9c3-7eb16b3d9398" />


### 错误处理
- `continue_on_error: true`：遇到错误时继续执行其他表
- `skip_empty_tables: true`：自动跳过空表
- 详细的错误日志记录
<img width="928" height="210" alt="image" src="https://github.com/user-attachments/assets/20390271-7a76-47ff-a579-6493c0cb3046" />


### 飞书通知
配置飞书机器人 Webhook，支持：
- 任务开始通知
- 任务成功通知
- 任务失败通知
- 进度更新通知（可选）
<img width="922" height="544" alt="image" src="https://github.com/user-attachments/assets/b74746d3-e596-4ca4-a4a0-c6e66db11f9c" />

## 📁 项目结构

```
mysql_to_clickhouse/
├── app.py                    # Flask Web 应用
├── database.py               # SQLite 数据库管理
├── mysql_to_clickhouse.py    # 核心迁移逻辑
├── feishu_notifier.py        # 飞书通知模块
├── conf.yaml                 # 配置文件（需自行创建）
├── conf.yaml-template        # 配置模板
├── requirements.txt          # Python 依赖
├── templates/                # Web 模板
│   ├── base.html
│   ├── index.html
│   ├── tasks.html
│   └── task_detail.html
└── README.md                 # 项目说明
```

## 🛠️ 技术栈

- **后端框架**：Flask 3.0
- **数据库驱动**：pymysql, clickhouse-connect
- **数据存储**：SQLite（任务记录）
- **前端技术**：HTML5, CSS3, JavaScript
- **配置管理**：PyYAML

## 📝 使用示例

### 迁移单个表
```yaml
migration:
  tables:
    - mysql_table: "users"
      ch_table: "users"
      batch_size: 10000
      verify: true
```

### 迁移多个表
```yaml
migration:
  tables:
    - mysql_table: "users"
      ch_table: "users"
    - mysql_table: "orders"
      ch_table: "orders"
    - mysql_table: "products"
      ch_table: "products"
```

### 自定义批次大小
```yaml
migration:
  default_batch_size: 50000  # 增大批次大小提升性能
```

## ⚠️ 注意事项

1. **数据备份**：迁移前请确保数据已备份
2. **网络稳定**：大规模迁移需要稳定的网络连接
3. **资源监控**：迁移过程中注意监控数据库和服务器资源
4. **权限要求**：确保 MySQL 和 ClickHouse 用户有足够的权限
5. **字符集**：建议使用 `utf8mb4` 字符集以支持完整的 Unicode

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

1. Fork 本仓库
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 开启 Pull Request

## 📄 许可证

本项目采用 MIT 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情

## 🙏 致谢

感谢所有为本项目做出贡献的开发者！

## 📮 联系方式

如有问题或建议，请提交 Issue 或联系维护者。

---

⭐ 如果这个项目对你有帮助，请给个 Star！

