# sbp-server · SBP Public Relay

The "server side" of SBP: the side deployed on the public internet with a public IP. It is responsible for
auth, device-code binding, routing, JSONL persistence, reverse paging, and centralized seq allocation.
It **does not parse agent semantics and does not execute local commands**.

## Quick Start

```bash
pip install -r server/requirements.txt
PYTHONPATH=server SBP_PORT=8787 python -m sbp_server.app
```

## Boundaries (hard constraints)

| Responsible for | Never responsible for |
|---|---|
| auth, device-code binding, routing, JSONL persistence, paging, seq allocation | parsing agent semantics, executing local commands |

## Dependency: sbp-sdk (git submodule)

This repository pulls in [sbp-sdk](https://github.com/sbp4ai/sbp-sdk) via **git submodule**, mounted at
`server/sdk/`; in code `SDK_DIR = REPO / "sdk"` points to it. The IM card's "lazy-loaded web page" paths
(`/sbp-sdk.js`, `/sdk/*`) are served from this submodule, so **you must clone with the submodule**:

```bash
git clone --recurse-submodules https://github.com/sbp4ai/sbp-server.git
# if already cloned without the submodule:
git submodule update --init --recursive
# to bump sbp-sdk to latest:
git submodule update --remote sdk
```

## Relation to the protocol / other repositories

- Frame format, error codes, and security model: [sbp-spec](https://github.com/sbp4ai/sbp-spec)
- The on-device driver: [sbp-agent](https://github.com/sbp4ai/sbp-agent)
- Web client library: [sbp-sdk](https://github.com/sbp4ai/sbp-sdk) (this repo's submodule)
  + [sbp-minis](https://github.com/sbp4ai/sbp-minis) (WeChat mini-program)
  + [sbp-im-embed](https://github.com/sbp4ai/sbp-im-embed) (IM-embed skill)

## Directory

```
server/sbp_server/
  app.py     aiohttp app + routing + WS hub
  core.py    binding / device / storage / paging core
```

---

# sbp-server · SBP 公网中继（中文）

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

## 依赖：sbp-sdk（git submodule）

本仓库通过 **git submodule** 把 [sbp-sdk](https://github.com/sbp4ai/sbp-sdk) 挂在
`server/sdk/`，代码里 `SDK_DIR = REPO / "sdk"` 指向它。IM 卡片的「懒加载网页」路径
（`/sbp-sdk.js`、`/sdk/*`）正是由这个 submodule 提供，所以**克隆时必须带上 submodule**：

```bash
git clone --recurse-submodules https://github.com/sbp4ai/sbp-server.git
# 已克隆但没带 submodule 时：
git submodule update --init --recursive
# 单独更新 sbp-sdk 到最新：
git submodule update --remote sdk
```

## 与协议 / 其它仓库的关系

- 帧格式、错误码、安全模型见 [sbp-spec](https://github.com/sbp4ai/sbp-spec)
- 本机侧驱动见 [sbp-agent](https://github.com/sbp4ai/sbp-agent)
- 网页客户端库见 [sbp-sdk](https://github.com/sbp4ai/sbp-sdk)（本仓库的 submodule）
  + [sbp-minis](https://github.com/sbp4ai/sbp-minis)（微信小程序）
  + [sbp-im-embed](https://github.com/sbp4ai/sbp-im-embed)（IM 内嵌技能）

## 目录

```
server/sbp_server/
  app.py     aiohttp 应用 + 路由 + WS hub
  core.py    绑定 / 设备 / 存储 / 分页核心
```
