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

## 鉴权：三种准入

| 方式 | 配置 | 能看到 |
|---|---|---|
| **HMAC 签名**（推荐，一个客户端配 N 台服务器就用它） | `SBP_HMAC_KEYS=keyId1:secret1,keyId2:secret2` | **本服务器上登记的全部设备**（scope = server，did = `*`） |
| 绑定码换来的 token | `POST /api/bind` | 只看到那一个设备（scope = device） |
| 本地裸跑 | `SBP_OPEN=1`（且没配 HMAC） | 同 server 级，仅调试用 |

握手帧：

```json
{"v":"sbp/0.1","id":"1","type":"req","name":"hello",
 "payload":{"role":"client","auth":{"scheme":"hmac-sha256","keyId":"ecs",
            "ts":1793498584620,"nonce":"n...","sig":"<hex>"}}}
```

签名内容 `HMAC-SHA256(secret, "sbp1\n{keyId}\n{ts}\n{nonce}")`，与 tmux 面板、
微信小程序的实现逐字一致（同一份 keyId/secret 三端通用），ts 偏差超过 5 分钟拒收
（防重放）。HTTP 侧（`GET /api/devices`）也可以带同样的签名：
`?keyId=&ts=&nonce=&sig=` 或 `Authorization: SBP-HMAC keyId=..,ts=..,nonce=..,sig=..`。

## 一台服务器、多台设备

server 级客户端（did = `*`）看到的是这台服务器上**所有**设备：

- `session.list` 返回 `groups`（一台设备一组，组里有 `did`）与扁平 `sessions`
  （每项带 `did`），外加一份 `devices` 摘要；
- 请求里的 `sid` 由服务端反查归属设备再转发 —— 客户端不用维护 sid→设备 的映射
  （先查内存映射，兜底扫一遍 store 索引；整台服务器只有一台在线设备时直接认定它）；
- `session.history` / `session.attach` / `session.answer` / `pane.snapshot`
  **转发到设备并等它的 res 回传**（服务端自己那份 jsonl 只在设备离线时兜底），
  所以「历史」是设备现场读 CLI 的 jsonl 回答的，内容与在本机面板里一致；
  写操作（`session.input` 等）fire-and-forget，立刻回 `{"queued": true}`；
- 设备上下线会推 `device.state` 事件，客户端据此重拉列表。

## 把 ws 暴露到公网（nginx）

```nginx
location /sbp-ecs/ {
    proxy_pass http://127.0.0.1:8787/;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection $connection_upgrade;
    proxy_read_timeout 3600s; proxy_send_timeout 3600s;
    proxy_buffering off;
}
```

⚠️ 客户端访问的是 `/sbp-ecs/ws`：**location 与请求路径都要带末尾斜杠**，
少了会被 nginx 301，而 WebSocket 握手吃不下 301（表现是一直重连）。

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
