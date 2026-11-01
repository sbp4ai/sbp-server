"""SBP server: HTTP (bind/admin/static) + WebSocket hub (client <-> device relay)."""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import os
import time
from pathlib import Path
from typing import Any

from aiohttp import WSMsgType, web

from .core import Store, now_ms, verify

log = logging.getLogger("sbp.server")

VERSION = "sbp/0.1"
REPO = Path(__file__).resolve().parents[2]
SDK_DIR = REPO / "sdk"
SERVER_SUBPROTOCOL = "sbp.v0.1"
#: 服务端级客户端的 did：不看某一台设备，而是看这台服务器上登记的全部设备。
ALL_DEVICES = "*"


def _load_hmac_keys() -> dict[str, str]:
    """.env / 环境里的 SBP_HMAC_KEYS=keyId1:secret1,keyId2:secret2。

    同一个 keyId/secret 在 panel 与这里通用，签名算法逐字一致，
    这样「一个客户端配 N 台服务器」时不用每台都走一遍绑定码。
    """
    out: dict[str, str] = {}
    for part in os.environ.get("SBP_HMAC_KEYS", "").split(","):
        if ":" in part:
            k, v = part.split(":", 1)
            if k.strip() and v.strip():
                out[k.strip()] = v.strip()
    return out


HMAC_KEYS = _load_hmac_keys()
#: 本地调试用：不配任何密钥又显式打开时，允许匿名连入（只能看，写仍要设备在线）
OPEN_MODE = os.environ.get("SBP_OPEN", "") == "1"


def check_hmac(hp: dict) -> bool:
    """校验握手帧里的签名：HMAC-SHA256(secret, "sbp1\\n{keyId}\\n{ts}\\n{nonce}")。

    与 tmux_panel.server._check_hmac、app/minis/utils/hmac.js 完全一致：
    三端共用一份 keyId/secret，ts 偏差 5 分钟内（防重放）。
    """
    if not HMAC_KEYS:
        return False
    a = (hp or {}).get("auth") or {}
    if a.get("scheme") != "hmac-sha256":
        return False
    secret = HMAC_KEYS.get(str(a.get("keyId") or ""))
    if not secret:
        return False
    msg = "sbp1\n%s\n%s\n%s" % (a.get("keyId"), a.get("ts"), a.get("nonce"))
    want = hmac.new(secret.encode(), msg.encode("utf-8"), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(want, str(a.get("sig") or "")):
        return False
    try:
        if abs(int(time.time() * 1000) - int(a.get("ts"))) > 5 * 60 * 1000:
            return False
    except (TypeError, ValueError):
        return False
    return True


# ---------------------------------------------------------------- connections
class DeviceConn:
    def __init__(self, ws: web.WebSocketResponse, did: str):
        self.ws = ws
        self.did = did
        self.caps: list[str] = []
        self.name = ""
        self.agent = ""
        self.screens: dict[str, dict] = {}   # sid -> last screen frame
        self.prompts: dict[str, dict] = {}   # sid -> 当前 open 的 PromptSpec（SBP-0.2）
        self.online_at = now_ms()

    async def send(self, obj: dict) -> None:
        try:
            await self.ws.send_json(obj)
        except Exception:
            pass


class ClientConn:
    def __init__(self, ws: web.WebSocketResponse, did: str, sub: str, caps: list[str]):
        self.ws = ws
        self.did = did
        self.sub = sub
        self.caps = caps
        self.sessions: dict[str, int] = {}   # sid -> last acked seq
        self.sid_did: dict[str, str] = {}    # sid -> 该 sid 属于哪台设备（server 级客户端用）
        self.all = did == ALL_DEVICES        # True：能看到本服务器上所有设备

    async def send(self, obj: dict) -> None:
        try:
            await self.ws.send_json(obj)
        except Exception:
            pass

    def subscribed(self, sid: str) -> bool:
        return sid in self.sessions


# ---------------------------------------------------------------- hub
class Hub:
    def __init__(self, store: Store):
        self.store = store
        self.devices: dict[str, DeviceConn] = {}
        self.clients: set[ClientConn] = set()
        self.sid_did: dict[str, str] = {}       # sid -> did，全局（server 级客户端靠它路由）
        self.pending: dict[str, tuple] = {}     # ref -> (ClientConn, ts)，等设备回包转交

    # -- 路由
    def did_for(self, cl: ClientConn, sid: str) -> str | None:
        """这条 sid 该发给哪台设备。

        设备级客户端（bind 码换来的 token）写死了 did；server 级客户端（HMAC 或
        SBP_OPEN）能看全部设备，所以得反查 sid 的归属 —— 先查内存映射（设备上线
        时上报过），再兜底扫一遍 store 的索引（服务端重启后还没上报的情况）。
        """
        if not cl.all:
            return cl.did
        did = cl.sid_did.get(sid) or self.sid_did.get(sid)
        if did:
            return did
        for d, bucket in self.store._index.items():
            if sid in bucket:
                self.sid_did[sid] = d
                return d
        return None

    def resolve_did(self, cl: ClientConn, sid: str) -> str | None:
        """did_for + 兜底：server 级客户端只知道 sid 时，若整台服务器只有一台设备
        在线，就认定是它（省得客户端为了路由再去维护一份 sid→did 映射）。"""
        did = self.did_for(cl, sid)
        if not did and cl.all and len(self.devices) == 1:
            did = next(iter(self.devices))
        return did

    def _remember(self, did: str, sids) -> None:
        for sid in sids:
            if sid:
                self.sid_did[sid] = did

    # -- 转发给设备
    def _prune(self) -> None:
        now = time.time()
        for ref, (_cl, ts) in list(self.pending.items()):
            if now - ts > 30:
                self.pending.pop(ref, None)

    async def forward(self, cl: ClientConn, frame: dict, did: str, wait: bool) -> bool:
        """把客户端的 req 转给设备。

        wait=True：登记 pending，设备的 res 回来后原样转交给发起的客户端
        （session.history / session.answer / pane.snapshot / session.attach 走这条）。
        wait=False：fire and forget，服务端立刻回 queued（写操作走这条）。
        """
        dev = self.devices.get(did)
        if not dev:
            return False
        fwd = dict(frame)
        fwd["v"] = VERSION
        fwd["type"] = "req"
        fwd["payload"] = {**(frame.get("payload") or {}), "_from": cl.sub, "_caps": cl.caps}
        if wait:
            self._prune()
            self.pending[str(frame.get("id"))] = (cl, time.time())
        await dev.send(fwd)
        return True

    # -- fanout
    async def to_clients(self, did: str, frame: dict, sid: str | None = None) -> None:
        dead = []
        for cl in self.clients:
            if cl.did != did and not cl.all:
                continue
            if sid and not cl.subscribed(sid):
                continue
            try:
                await cl.ws.send_json(frame)
            except Exception:
                dead.append(cl)
        for cl in dead:
            self.clients.discard(cl)

    # -- device frames
    async def on_device_frame(self, dev: DeviceConn, frame: dict) -> None:
        name = frame.get("name")
        sid = frame.get("sid")
        payload = frame.get("payload") or {}

        if name == "session.index":
            for s in payload.get("sessions", []):
                if not s.get("sid"):
                    continue
                if not s.get("title"):
                    s["title"] = "未命名会话"
                s["did"] = dev.did          # server 级客户端靠这个字段分组/路由
                self.store.upsert_session(dev.did, s["sid"], s)
            self._remember(dev.did, [s.get("sid") for s in payload.get("sessions", [])])
            await self.to_clients(dev.did, {
                "v": VERSION, "type": "evt", "name": "session.index",
                "ts": now_ms(), "payload": {"sessions": self.store.index(dev.did)},
            })
            return

        if name == "session.message":
            rec = self.store.append_message(dev.did, sid, {
                "role": payload.get("role", "agent"),
                "kind": payload.get("kind", "text"),
                "text": payload.get("text", ""),
                "ts": payload.get("ts") or now_ms(),
                "meta": payload.get("meta") or {},
            })
            await self.to_clients(dev.did, {
                "v": VERSION, "type": "evt", "name": "session.message",
                "sid": sid, "seq": rec["seq"], "ts": rec["ts"], "payload": rec,
            }, sid=sid)
            return

        if name == "output.delta":
            entry = self.store._index.get(dev.did, {}).get(sid, {})
            frame_out = {
                "v": VERSION, "type": "evt", "name": "output.delta",
                "sid": sid, "seq": int(entry.get("last_seq", 0)),
                "ts": now_ms(), "payload": payload,
            }
            dev.screens[sid] = frame_out
            await self.to_clients(dev.did, frame_out, sid=sid)
            return

        if name == "session.state":
            self.store.upsert_session(dev.did, sid, {
                "status": payload.get("status"),
                "updated_at": now_ms(),
            })
            await self.to_clients(dev.did, {
                "v": VERSION, "type": "evt", "name": "session.state",
                "sid": sid, "ts": now_ms(), "payload": payload,
            }, sid=sid)
            return

        # SBP-0.2：agent 在终端上摆出的选项，原样扇出给订阅了该会话的客户端
        if name == "session.prompt":
            # 记住当前卡片，晚 attach 的客户端也能拿到
            if payload.get("state") == "open":
                dev.prompts[sid] = payload
            else:
                dev.prompts.pop(sid, None)
            await self.to_clients(dev.did, {
                "v": VERSION, "type": "evt", "name": "session.prompt",
                "sid": sid, "ts": now_ms(), "payload": payload,
            }, sid=sid)
            return

        if frame.get("type") == "res" and frame.get("ref"):
            ref = str(frame.get("ref"))
            wait = self.pending.pop(ref, None)
            if wait:
                # 这个请求是某个客户端转过来的，res 只给它，别广播
                await wait[0].send(frame)
                return
            if str(frame.get("name") or "").startswith("hello"):
                return
            # 设备主动回的（比如 session.answer 没走 pending 的旧实现）：发给
            # 订阅这台设备的客户端
            for cl in self.clients:
                if cl.did == dev.did or cl.all:
                    await cl.send(frame)
            return

        # 未知 evt 静默丢弃（前向兼容）

    # -- client requests
    async def on_client_req(self, cl: ClientConn, frame: dict) -> None:
        rid = frame.get("id")
        name = frame.get("name")
        payload = frame.get("payload") or {}

        def res(ok: bool, data: dict | None = None, err: dict | None = None) -> dict:
            out = {"v": VERSION, "type": "res", "ref": rid, "name": name, "ok": ok, "ts": now_ms()}
            if data is not None:
                out["payload"] = data
            if err:
                out["err"] = err
            return out

        if name == "ping":
            # 应用层心跳（小程序拿不到 WS 层的 ping）
            await cl.send(res(True, {"t": payload.get("t"), "ts": now_ms()}))
            return

        if name in ("session.list", "sessions.index"):
            if not cl.all:
                sessions = self.store.index(cl.did)
                dev = self.devices.get(cl.did)
                # 列表字段对齐 panel：short / subtitle / count / kind 都由服务端补好，
                # 客户端（小程序、IM 卡片）就不需要各写一套兜底
                items = [{
                    "sid": s.get("sid", ""),
                    "did": cl.did,
                    "short": (s.get("sid") or "")[:8],
                    "title": s.get("title") or (s.get("sid") or "")[:8],
                    "subtitle": s.get("last_prompt") or s.get("preview") or "",
                    "count": s.get("msg_count", 0),
                    "updated_at": s.get("updated_at", 0),
                    "kind": s.get("kind", "claude"),
                    "agent": (dev.agent if dev else ""),
                } for s in sessions]
                await cl.send(res(True, {
                    "sessions": sessions,
                    "groups": [{
                        "kind": "device", "did": cl.did,
                        "name": (dev.name if dev else "设备"),
                        "cwd": (dev.agent if dev else ""),
                        "live": bool(dev),
                        "items": items,
                        "count": len(items),
                    }],
                    "devices": [{"did": cl.did, "name": (dev.name if dev else "设备"),
                                 "agent": (dev.agent if dev else ""), "live": bool(dev),
                                 "count": len(items)}],
                }))
                return
            # server 级客户端：这台服务器上登记过的设备全列出来，一台一组
            dids = sorted(set(list(self.store._index.keys()) + list(self.devices.keys())))
            groups: list[dict] = []
            flat: list[dict] = []
            for d in dids:
                dev = self.devices.get(d)
                meta = self.store.device(d) or {}
                sessions = self.store.index(d)
                self._remember(d, [s.get("sid") for s in sessions])
                items = [{
                    "sid": s.get("sid", ""),
                    "did": d,
                    "short": (s.get("sid") or "")[:8],
                    "title": s.get("title") or (s.get("sid") or "")[:8],
                    "subtitle": s.get("last_prompt") or s.get("preview") or "",
                    "count": s.get("msg_count", 0),
                    "updated_at": s.get("updated_at", 0),
                    "kind": s.get("kind", "claude"),
                    "agent": s.get("agent") or (dev.agent if dev else meta.get("agent", "")),
                    "status": s.get("status", ""),
                } for s in sessions]
                groups.append({
                    "kind": "device", "did": d,
                    "name": (dev.name if dev else meta.get("name") or f"设备 {d}"),
                    "cwd": (dev.agent if dev else meta.get("agent", "")),
                    "live": bool(dev), "items": items, "count": len(items),
                })
                flat += items
            await cl.send(res(True, {
                "sessions": flat, "groups": groups,
                "devices": [{"did": g["did"], "name": g["name"], "agent": g["cwd"],
                             "live": g["live"], "count": g["count"]} for g in groups],
            }))
            return

        sid = payload.get("sid", "")

        def res_no_device() -> dict:
            return res(False, None, {"code": "E_NO_DEVICE", "msg": "设备离线", "retryable": True})

        # 该 sid 落在哪台设备上；server 级客户端（did="*"）没有写死 did，必须反查
        did = self.resolve_did(cl, sid)
        store_did = did if (did and did != ALL_DEVICES) else (None if cl.all else cl.did)

        if name == "session.history":
            dev = self.devices.get(did) if did else None
            # 设备在线就让它回答：panel 那边读的是 CLI 自己的 jsonl，历史比服务端的全
            if dev and await self.forward(cl, frame, did, wait=True):
                return
            if not store_did:
                await cl.send(res(True, {"sid": sid, "items": [], "has_more": False,
                                         "next_before": None, "next_before_seq": None}))
                return
            h = self.store.history(
                store_did, sid,
                payload.get("before_seq") or payload.get("before"),
                int(payload.get("limit", 20) or 20),
            )
            # 字段名与 panel 对齐，两端的 UI 才不用各写一套
            h["next_before"] = h.get("next_before_seq")
            await cl.send(res(True, {**h, "sid": sid}))
            return

        if name == "session.attach":
            from_seq = int(payload.get("from_seq") or 0)
            cl.sessions[sid] = max(0, from_seq - 1)
            if did and did != ALL_DEVICES:
                cl.sid_did[sid] = did
                self.sid_did[sid] = did
            dev = self.devices.get(did) if did else None
            if dev and await self.forward(cl, frame, did, wait=True):
                return       # 设备的 res 里有 tmux / kind / editable / prompt
            missed = self.store.messages_after(store_did or "", sid, cl.sessions[sid], limit=200)
            await cl.send(res(True, {
                "sid": sid,
                "last_seq": (self.store._index.get(store_did or "", {}).get(sid, {}) or {}).get("last_seq", 0),
                "missed": len(missed),
                "editable": False,
                "prompt": None,
            }))
            for rec in missed:
                await cl.send({
                    "v": VERSION, "type": "evt", "name": "session.message",
                    "sid": sid, "seq": rec["seq"], "ts": rec.get("ts"), "payload": rec,
                })
            return

        if name == "session.detach":
            cl.sessions.pop(sid, None)
            cl.sid_did.pop(sid, None)
            if did and did in self.devices:
                await self.forward(cl, frame, did, wait=False)
            await cl.send(res(True, {"sid": sid}))
            return

        # 需要落到设备侧的请求
        if name in ("session.input", "session.snapshot", "session.resize",
                    "session.spawn", "pane.snapshot", "session.subtitle"):
            # 抓屏要拿回内容，其余写操作 fire and forget 就够
            want_res = name in ("pane.snapshot", "session.spawn")
            if not did or not await self.forward(cl, frame, did, wait=want_res):
                await cl.send(res_no_device())
                return
            if not want_res:
                await cl.send(res(True, {"queued": True}))
            return

        # SBP-0.2 作答：必须等设备的结果，否则 UI 收不到 E_PROMPT_STALE
        if name == "session.answer":
            if not did or not await self.forward(cl, frame, did, wait=True):
                await cl.send(res_no_device())
            return

        await cl.send(res(False, None, {"code": "E_UNKNOWN_NAME", "msg": f"未知请求 {name}"}))

    async def on_client_evt(self, cl: ClientConn, frame: dict) -> None:
        if frame.get("name") == "ack":
            sid = frame.get("sid")
            seq = int(frame.get("seq") or 0)
            if sid:
                cl.sessions[sid] = max(cl.sessions.get(sid, 0), seq)
            return
        # 未知 evt：丢弃


# ---------------------------------------------------------------- app
def _http_auth(req: web.Request) -> bool:
    """HTTP 接口的鉴权：没配 HMAC 密钥就放行（本地裸跑），配了必须签。

    签名可以放在 query（?keyId=&ts=&nonce=&sig=）或
    Authorization: SBP-HMAC keyId=..,ts=..,nonce=..,sig=.. 里，与 panel 一致。
    """
    if not HMAC_KEYS:
        return True
    q = req.query
    a = {"scheme": "hmac-sha256", "keyId": q.get("keyId"), "ts": q.get("ts"),
         "nonce": q.get("nonce"), "sig": q.get("sig")}
    h = req.headers.get("Authorization", "")
    if h.startswith("SBP-HMAC "):
        for part in h[9:].split(","):
            if "=" in part:
                k, v = part.split("=", 1)
                k = k.strip().lower()
                if k == "keyid":
                    a["keyId"] = v.strip()
                elif k in ("ts", "nonce", "sig"):
                    a[k] = v.strip()
    return check_hmac({"auth": a})


def make_app(data_dir: str | None = None) -> web.Application:
    store = Store(data_dir or os.environ.get("SBP_DATA") or (REPO / ".sbp-data"))
    hub = Hub(store)

    async def index(_req: web.Request) -> web.StreamResponse:
        page = (SDK_DIR / "index.html")
        if not page.exists():
            return web.Response(text="sbp-sdk not found", status=500)
        return web.FileResponse(page)

    async def health(_req: web.Request) -> web.Response:
        return web.json_response({
            "ok": True, "v": VERSION,
            "devices_online": len(hub.devices),
            "devices": len(store._devices),
            "clients": len(hub.clients),
            "auth": {"hmac": bool(HMAC_KEYS), "open": OPEN_MODE},
            "uptime_s": int(time.time() - START),
        })

    async def devices(req: web.Request) -> web.Response:
        """这台服务器上登记过的设备（含在线状态）。

        没配 HMAC 密钥时一律放行（本地裸跑）；配了就要签名，签名算法与 WS 握手一致。
        """
        if not _http_auth(req):
            return web.json_response({"ok": False, "err": {"code": "E_AUTH", "msg": "需要签名"}}, status=401)
        out = []
        for did, meta in store._devices.items():
            live = hub.devices.get(did)
            out.append({
                "did": did, "name": (live.name if live else meta.get("name")),
                "agent": (live.agent if live else meta.get("agent", "")),
                "online": bool(live), "revoked": bool(meta.get("revoked")),
                "sessions": len(store._index.get(did, {})),
            })
        return web.json_response({"ok": True, "devices": out})

    async def register(req: web.Request) -> web.Response:
        body = await req.json()
        out = store.register(
            name=str(body.get("name") or "unnamed-device"),
            agent=str(body.get("agent") or "generic"),
            fingerprint=str(body.get("fingerprint") or ""),
        )
        dev = out["device"]
        code = out["bind_code"]
        host = req.headers.get("Host", f"127.0.0.1:{PORT}")
        scheme = req.headers.get("X-Forwarded-Proto", req.scheme)
        return web.json_response({
            "device_id": dev["id"],
            "device_secret": dev["secret"],
            "bind_code": code,
            "bind_url": f"{scheme}://{host}/bind?code={code}",
            "expires_in": store.code_ttl(code),
            "v": VERSION,
        })

    async def bind(req: web.Request) -> web.Response:
        body = await req.json()
        dev = store.consume_code(str(body.get("code", "")), str(body.get("im_user", "anon")), str(body.get("channel", "web")))
        if not dev:
            return web.json_response({"ok": False, "err": {"code": "E_AUTH", "msg": "验证码无效或已过期"}}, status=403)
        token = store.client_token(dev, sub=str(body.get("im_user", "anon")), caps=dev.get("caps", []))
        return web.json_response({
            "ok": True, "device_id": dev["id"], "device_name": dev["name"],
            "client_token": token, "expires_in": 12 * 3600,
        })

    async def unbind(req: web.Request) -> web.Response:
        did = req.match_info["did"]
        ok = store.revoke(did)
        dev = hub.devices.pop(did, None)
        if dev:
            await dev.ws.close(code=4403, message=b"revoked")
        return web.json_response({"ok": ok})

    async def bind_page(_req: web.Request) -> web.Response:
        return web.FileResponse(SDK_DIR / "bind.html")

    async def ws_handler(req: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse(heartbeat=25, protocols=(SERVER_SUBPROTOCOL, VERSION))
        await ws.prepare(req)

        # --- 首帧必须 hello
        try:
            first = await asyncio.wait_for(ws.receive(), timeout=5)
        except asyncio.TimeoutError:
            await ws.close(code=4408, message=b"hello timeout")
            return ws
        if first.type is not WSMsgType.TEXT:
            await ws.close(code=4401, message=b"need hello")
            return ws
        hello = json.loads(first.data or "{}")
        if hello.get("name") != "hello":
            await ws.close(code=4401, message=b"need hello")
            return ws
        hp = hello.get("payload") or {}
        role = hp.get("role")

        if role == "device":
            dev = store.device(str(hp.get("device_id", "")))
            if not dev or dev.get("revoked") or dev.get("secret") != hp.get("secret"):
                await ws.close(code=4401, message=b"bad device secret")
                return ws
            dconn = DeviceConn(ws, dev["id"])
            dconn.caps = hp.get("caps") or []
            dconn.name = dev.get("name", "")
            dconn.agent = dev.get("agent", "")
            hub.devices[dev["id"]] = dconn
            dev["caps"] = dconn.caps
            store._flush_devices()
            hub._remember(dev["id"], [s.get("sid") for s in store.index(dev["id"])])
            await dconn.send({
                "v": VERSION, "type": "res", "ref": hello.get("id"), "name": "hello.ok",
                "ok": True, "ts": now_ms(),
                "payload": {"server": {"v": VERSION, "ts": now_ms()}, "device": {"id": dev["id"], "name": dconn.name}},
            })
            await dconn.send({
                "v": VERSION, "type": "evt", "name": "session.index", "ts": now_ms(),
                "payload": {"sessions": store.index(dev["id"])},
            })
            # 让 server 级客户端知道「这台上线了」，它好重拉一次列表
            await hub.to_clients(dev["id"], {
                "v": VERSION, "type": "evt", "name": "device.state", "ts": now_ms(),
                "payload": {"did": dev["id"], "name": dconn.name, "agent": dconn.agent, "online": True},
            })
            log.info("device online: %s (%s)", dev["id"], dconn.agent)
            try:
                async for msg in ws:
                    if msg.type is not WSMsgType.TEXT:
                        continue
                    frame = json.loads(msg.data or "{}")
                    if frame.get("name") == "ping":
                        await dconn.send({"v": VERSION, "type": "evt", "name": "pong", "ts": now_ms()})
                        continue
                    await hub.on_device_frame(dconn, frame)
            finally:
                hub.devices.pop(dev["id"], None)
                await hub.to_clients(dev["id"], {
                    "v": VERSION, "type": "evt", "name": "device.state", "ts": now_ms(),
                    "payload": {"did": dev["id"], "name": dconn.name, "agent": dconn.agent, "online": False},
                })
                log.info("device offline: %s", dev["id"])
            return ws

        if role == "client":
            # 三种准入：bind 码换来的设备级 token（只看到那一台）、HMAC 签名
            # （server 级，看到这台服务器上全部设备）、SBP_OPEN=1（本地裸跑）。
            tok = str(hp.get("token", ""))
            tp = verify(store.secret, tok) if tok else None
            if tp and tp.get("role") == "client":
                did = str(tp["did"])
                sub = str(tp.get("sub", ""))
                caps = list(tp.get("cap") or [])
                scope = "device"
            elif check_hmac(hp):
                did = ALL_DEVICES
                sub = str((hp.get("auth") or {}).get("keyId") or "hmac")
                caps = ["*"]
                scope = "server"
            elif OPEN_MODE and not HMAC_KEYS:
                did = ALL_DEVICES
                sub = "anon"
                caps = ["*"]
                scope = "server"
            else:
                await ws.close(code=4401, message=b"bad token")
                return ws
            dev = store.device(did) if scope == "device" else None
            if scope == "device" and (not dev or dev.get("revoked")):
                await ws.close(code=4403, message=b"device revoked")
                return ws
            cconn = ClientConn(ws, did, sub, caps)
            if not cconn.caps:
                # token 里没写死能力位 -> 继承设备当前能力（绑定早于 hello 的常见情况）
                live = hub.devices.get(did)
                cconn.caps = list(live.caps) if live else list((dev or {}).get("caps") or [])
            hub.clients.add(cconn)
            await cconn.send({
                "v": VERSION, "type": "res", "ref": hello.get("id"), "name": "hello.ok",
                "ok": True, "ts": now_ms(),
                "payload": {
                    "server": {"v": VERSION, "ts": now_ms(), "hmac": bool(HMAC_KEYS),
                               "devices": len(hub.devices)},
                    "device": ({"id": dev["id"], "name": dev.get("name"),
                                "online": dev["id"] in hub.devices} if dev else None),
                    "scope": scope,
                    "caps": cconn.caps,
                },
            })
            if not cconn.all:
                # 客户端几乎总是比设备晚连上来。设备只在自己连上时 emit 一次
                # session.index，如果不在这里补推一次 store 里已有的会话，
                # 晚到的客户端就会停在「0 个会话」的空页面上。
                # server 级客户端不推：它的列表是跨设备的，自己发 session.list 拿。
                await cconn.send({
                    "v": VERSION, "type": "evt", "name": "session.index", "ts": now_ms(),
                    "payload": {"sessions": store.index(did)},
                })
            try:
                async for msg in ws:
                    if msg.type is not WSMsgType.TEXT:
                        continue
                    frame = json.loads(msg.data or "{}")
                    if frame.get("type") == "req":
                        await hub.on_client_req(cconn, frame)
                    elif frame.get("type") == "evt":
                        await hub.on_client_evt(cconn, frame)
            finally:
                hub.clients.discard(cconn)
            return ws

        await ws.close(code=4401, message=b"bad role")
        return ws

    app = web.Application(client_max_size=4 * 1024 * 1024)
    app["store"] = store
    app["hub"] = hub
    app.router.add_get("/api/health", health)
    app.router.add_get("/api/devices", devices)
    app.router.add_post("/api/device/register", register)
    app.router.add_post("/api/bind", bind)
    app.router.add_post("/api/device/{did}/unbind", unbind)
    app.router.add_get("/ws", ws_handler)
    app.router.add_get("/sbp", ws_handler)
    app.router.add_get("/bind", bind_page)
    app.router.add_get("/", index)
    app.router.add_static("/sdk", SDK_DIR, show_index=False)

    async def sdk_js(_r: web.Request) -> web.StreamResponse:
        return web.FileResponse(SDK_DIR / "sbp-sdk.js", headers={"Cache-Control": "no-cache"})

    app.router.add_get("/sbp-sdk.js", sdk_js)
    return app


START = time.time()
PORT = int(os.environ.get("SBP_PORT", "8787"))


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    web.run_app(make_app(), host=os.environ.get("SBP_HOST", "127.0.0.1"), port=PORT, print=None)


if __name__ == "__main__":
    main()
