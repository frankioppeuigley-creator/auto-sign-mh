# Auto Sign

自动签到、领券、抽奖脚本，支持 Docker 部署和代理池。

## 功能

- 自动登录获取 Token
- 自动领券、分享、抽奖
- 自动查询积分和余额
- 支持代理池（每个账号独立代理）
- 失败自动重试（最多 3 次）
- 时间窗口均匀分布（避开凌晨 2-6 点）
- 支持热新增账号
- 执行结果发送邮件

## 架构设计

```
┌─────────────────────────────────────────────────────────────────┐
│                        时间窗口分布策略                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  06:00 ─────────────────────────────────────────── 23:30          │
│    │                                                              │
│    ├── 07:23 执行批次1 (4个账号)                                   │
│    │                                                              │
│    ├── 10:45 执行批次2 (5个账号)                                   │
│    │                                                              │
│    ├── 14:12 执行批次3 (3个账号)                                   │
│    │                                                              │
│    ├── 18:30 执行批次4 (5个账号)                                   │
│    │                                                              │
│    └── 22:15 执行批次5 (4个账号)                                   │
│                                                                   │
│  23:30 ─────────────────────────────────────────── 02:00          │
│    │                                                              │
│    └── 重试缓冲区：处理失败账号                                    │
│                                                                   │
│  02:00 ─────────────────────────────────────────── 06:00          │
│    │                                                              │
│    └── 睡眠窗口：不执行任何任务                                    │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

## 快速开始

### 1. 准备配置文件

编辑 `config.json`：

```json
{
  "accounts": [
    "13800138000",
    "13900139000"
  ],
  "email": {
    "sender": "your_email@qq.com",
    "receiver": "receiver@qq.com",
    "smtp_server": "smtp.qq.com",
    "smtp_port": 465,
    "auth_code": "your_smtp_auth_code"
  },
  "user_agents": [
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) ..."
  ]
}
```

### 2. 启动服务

```bash
# 构建并启动（后台运行）
docker-compose up -d

# 查看日志
docker-compose logs -f app
```

### 3. 常用命令

```bash
# 查看队列状态和调度表
docker-compose exec app python run_proxy.py --status

# 立即执行（不等待调度时间）
docker-compose exec app python run_proxy.py --once

# 手动生成调度表
docker-compose exec app python run_proxy.py --generate

# 手动生成重试调度表
docker-compose exec app python run_proxy.py --retry

# 热新增账号
docker-compose exec app python run_proxy.py --add 13800138000 13900139000

# 清空所有数据
docker-compose exec app python run_proxy.py --clear

# 重启服务
docker-compose restart

# 停止服务
docker-compose down
```

## 运行逻辑

### 调度流程

1. **06:00 生成调度表**
   - 读取所有账号，随机打乱
   - 随机切分成批次（3-8个/批）
   - 将批次均匀分配到 06:00-23:30 的时间窗口
   - 写入 Redis ZSET（score=执行时间戳）

2. **心跳循环（每30秒）**
   - 检查是否有到达执行时间的批次
   - 有：获取代理，并发执行
   - 没有：继续等待

3. **23:30 生成重试调度表**
   - 收集所有失败账号
   - 分配到 23:30-02:00 的重试窗口
   - 继续由心跳循环执行

4. **发送日报**
   - 所有任务完成后发送邮件
   - 等待到明天 06:00

### 失败处理

| 失败次数 | 处理方式 |
|----------|----------|
| 第1次 | 加入重试队列 |
| 第2次 | 加入重试队列 |
| 第3次 | 放弃，记录到 giveup |

### 热新增账号

```bash
docker-compose exec app python run_proxy.py --add 13800138000
```

- 立即加入 pending 队列
- 如果今天已有调度表，自动追加新批次
- 无需重启服务

## Redis 数据结构

| Key | Type | 说明 |
|-----|------|------|
| `schedule` | ZSET | 调度表（score=时间戳，member=批次数据） |
| `pending` | ZSET | 待处理账号（score=重试次数） |
| `done` | Set | 已完成账号 |
| `giveup` | Set | 放弃账号（失败≥3次） |
| `retry_queue` | ZSET | 重试队列（score=重试次数） |
| `retry_count` | Hash | 重试次数记录 |
| `queue_date` | String | 当前队列日期 |
| `reports` | List | 执行报告 |

## 配置参数

```python
DAY_START_HOUR = 6       # 每天开始执行时间
DAY_END_HOUR = 23        # 正常执行结束时间
DAY_END_MINUTE = 30      # 正常执行结束分钟
RETRY_END_HOUR = 2       # 重试缓冲区结束时间（次日凌晨）
BATCH_SIZE_MIN = 3       # 每批最少账号数
BATCH_SIZE_MAX = 8       # 每批最多账号数
TICK_INTERVAL = 30       # 心跳间隔（秒）
MAX_RETRY = 3            # 最大重试次数
```

## Docker 部署

```bash
# 构建并启动
docker-compose up -d

# 查看日志
docker-compose logs -f app

# 进入容器
docker-compose exec app bash

# 查看 Redis 数据
docker-compose exec redis redis-cli
```

## 文件说明

```
├── docker-compose.yml    # Docker 容器编排
├── Dockerfile            # 应用镜像定义
├── requirements.txt      # Python 依赖
├── config.json           # 账号和邮件配置
├── run_proxy.py          # 主程序（代理模式，调度器）
├── run_all.py            # 主程序（无代理模式）
└── README.md             # 说明文档
```
