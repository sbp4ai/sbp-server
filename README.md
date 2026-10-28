# sbp-server · SBP 公网中继

SBP 的「服务端」：部署在公网、持有公网 IP 的那一侧。负责鉴权、设备码绑定、路由、
JSONL 持久化、倒序分页、seq 单点分配。它**不解析 agent 语义、不执行本地命令**。

## 快速开始

```bash
pip install -r server/requirements.txt
PYTHONPATH=server SBP_PORT=8787 python -m sbp_server.app
```

## 边界（硬约束）

| 负责 | 绝不负责 |
|---|---|
| 鉴权、设备码绑定、路由、JSONL 持久化、分页、seq 分配 | 解析 agent 语义、执行本地命令 |

## 与协议的关系

- 帧格式、错误码、安全模型见 [sbp-spec](https://github.com/slamKunTech/sbp-spec)
- 本机侧驱动见 [sbp-agent](https://github.com/slamKunTech/sbp-agent)
- 客户端见 [sbp-client](https://github.com/slamKunTech/sbp-client)

## 目录

```
server/sbp_server/
  app.py     aiohttp 应用 + 路由 + WS hub
  core.py    绑定 / 设备 / 存储 / 分页核心
```
