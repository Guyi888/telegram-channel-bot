# Telegram Channel Management Bot

功能完整的 Telegram 频道管理机器人，基于 python-telegram-bot v20 + Pyrogram 构建。

## 功能模块

| 模块 | 说明 |
|------|------|
| 管理员消息转发 | 管理员私信自动转发至目标频道，支持 MediaGroup 聚合 |
| 用户投稿系统 | 用户投稿 → 管理员审核 → 发布，支持匿名/署名/自定义签名 |
| 跨频道采集 | 通过 Pyrogram 个人账号监听来源频道并自动转发 |
| 广告按钮系统 | 按时段配置广告套餐，自动在发布内容下附加按钮 |
| 互动按钮 | 👍/👎 互斥点赞/点踩，💬 讨论群组跳转 |
| 内容自动分类 | 基于 jieba + 关键词权重自动给内容打分类标签 |
| 违禁词过滤 | 精确 + 模糊匹配，仅对用户投稿生效 |
| 广告过滤 | 自动过滤含链接/用户名的采集内容，支持自定义关键词 |

## 环境要求

- Python 3.8+
- 依赖见 `requirements.txt`

## 快速开始

```bash
# 安装依赖
pip install -r requirements.txt

# 复制并配置环境变量
cp .env.example .env

# 启动
python main.py
```

## 环境变量

| 变量 | 说明 |
|------|------|
| `BOT_TOKEN` | Bot Token（从 @BotFather 获取） |
| `SUPER_ADMIN_IDS` | 超级管理员 Telegram ID，多个用逗号分隔 |
| `TARGET_CHANNEL_ID` | 目标频道 ID |
| `REVIEW_GROUP_ID` | 审核群组 ID |
| `API_ID` | Telegram API ID（Pyrogram 采集用） |
| `API_HASH` | Telegram API Hash |
| `PHONE_NUMBER` | 采集账号手机号 |
| `DATABASE_PATH` | SQLite 数据库路径，默认 `data/bot.db` |

## 主要命令

```
/panel        — 管理员控制台
/status       — 运行状态
/addtarget    — 添加目标频道
/addsource    — 添加采集来源频道
/addadmin     — 添加管理员
/adfilter     — 广告过滤设置
/buttons      — 广告套餐管理
/categories   — 内容分类管理
/badwords     — 违禁词管理
```

## 技术栈

- [python-telegram-bot](https://github.com/python-telegram-bot/python-telegram-bot) v20
- [Pyrogram](https://github.com/pyrogram/pyrogram)
- aiosqlite
- jieba
- APScheduler
