"""SBP server: HTTP (bind/admin/static) + WebSocket hub (client <-> device relay)."""
from __future__ import annotations

import asyncio
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

    # -- fanout
    async def to_clients(self, did: str, frame: dict, sid: str | None = None) -> None:
        dead = []
        for cl in self.clients:
            if cl.did != did:
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
                self.store.upsert_session(dev.did, s["sid"], s)
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
            # 回给发起请求的客户端
            for cl in self.clients:
                if cl.did == dev.did:
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
            sessions = self.store.index(cl.did)
            dev = self.devices.get(cl.did)
            # 列表字段对齐 panel：short / subtitle / count / kind 都由服务端补好，
            # 客户端（小程序、IM 卡片）就不需要各写一套兜底
            items = [{
                "sid": s.get("sid", ""),
                "short": (s.get("sid") or "")[:8],
                "title": s.get("title") or (s.get("sid") or "")[:8],
                "subtitle": s.get("last_prompt") or "",
                "count": s.get("msg_count", 0),
                "updated_at": s.get("updated_at", 0),
                "kind": s.get("kind", "claude"),
                "agent": (dev.agent if dev else ""),
            } for s in sessions]
            await cl.send(res(True, {
                "sessions": sessions,
                "groups": [{
                    "kind": "device",
                    "name": (dev.name if dev else "设备"),
                    "cwd": (dev.agent if dev else ""),
                    "live": bool(dev),
                    "items": items,
                    "count": len(items),
                }],
            }))
            return

        if name == "session.history":
            sid = payload.get("sid", "")
            h = self.store.history(
                cl.did, sid,
                payload.get("before_seq") or payload.get("before"),
                int(payload.get("limit", 20) or 20),
            )
            # 字段名与 panel 对齐，两端的 UI 才不用各写一套
            h["next_before"] = h.get("next_before_seq")
            await cl.send(res(True, {**h, "sid": sid}))
            return

        if name == "session.attach":
            sid = payload.get("sid", "")
            from_seq = int(payload.get("from_seq") or 0)
            cl.sessions[sid] = max(0, from_seq - 1)
            missed = self.store.messages_after(cl.did, sid, cl.sessions[sid], limit=200)
            dev = self.devices.get(cl.did)
            await cl.send(res(True, {
                "sid": sid,
                "last_seq": (self.store._index.get(cl.did, {}).get(sid, {}) or {}).get("last_seq", 0),
                "missed": len(missed),
                "editable": bool(dev),                # 设备在线即可写
                "prompt": dev.prompts.get(sid) if dev else None,
            }))
            for rec in missed:
                await cl.send({
                    "v": VERSION, "type": "evt", "name": "session.message",
                    "sid": sid, "seq": rec["seq"], "ts": rec.get("ts"), "payload": rec,
                })
            if dev and sid in dev.screens:
                await cl.send(dev.screens[sid])
            return

        if name == "session.detach":
            cl.sessions.pop(payload.get("sid", ""), None)
            await cl.send(res(True, {"sid": payload.get("sid")}))
            return

        # 需要落到设备侧的请求（fire and forget）
        if name in ("session.input", "session.snapshot", "session.resize",
                    "session.spawn", "pane.snapshot"):
            dev = self.devices.get(cl.did)
            if not dev:
                await cl.send(res(False, None, {"code": "E_NO_DEVICE", "msg": "设备离线", "retryable": True}))
                return
            fwd = dict(frame)
            fwd["v"] = VERSION
            fwd["type"] = "req"
            fwd["payload"] = {**payload, "_from": cl.sub, "_caps": cl.caps}
            await dev.send(fwd)
            await cl.send(res(True, {"queued": True}))
            return

        # SBP-0.2 作答：必须等 bridge 的结果，否则 UI 收不到 E_PROMPT_STALE
        if name == "session.answer":
            dev = self.devices.get(cl.did)
            if not dev:
                await cl.send(res(False, None, {"code": "E_NO_DEVICE", "msg": "设备离线", "retryable": True}))
                return
            fwd = dict(frame)
            fwd["v"] = VERSION
            fwd["type"] = "req"
            fwd["payload"] = {**payload, "_from": cl.sub}
            await dev.send(fwd)
            # 不自己回：bridge 会回一个同 ref 的 res，由 on_device_frame 转交给发起者
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
            "clients": len(hub.clients),
            "uptime_s": int(time.time() - START),
        })

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
            await dconn.send({
                "v": VERSION, "type": "res", "ref": hello.get("id"), "name": "hello.ok",
                "ok": True, "ts": now_ms(),
                "payload": {"server": {"v": VERSION, "ts": now_ms()}, "device": {"id": dev["id"], "name": dconn.name}},
            })
            await dconn.send({
                "v": VERSION, "type": "evt", "name": "session.index", "ts": now_ms(),
                "payload": {"sessions": store.index(dev["id"])},
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
                log.info("device offline: %s", dev["id"])
            return ws

        if role == "client":
            payload = verify(store.secret, str(hp.get("token", "")))
            if not payload or payload.get("role") != "client":
                await ws.close(code=4401, message=b"bad token")
                return ws
            dev = store.device(payload["did"])
            if not dev or dev.get("revoked"):
                await ws.close(code=4403, message=b"device revoked")
                return ws
            cconn = ClientConn(ws, payload["did"], payload.get("sub", ""), payload.get("cap") or [])
            if not cconn.caps:
                # token 里没写死能力位 -> 继承设备当前能力（绑定早于 hello 的常见情况）
                live = hub.devices.get(payload["did"])
                cconn.caps = list(live.caps) if live else list(dev.get("caps") or [])
            hub.clients.add(cconn)
            await cconn.send({
                "v": VERSION, "type": "res", "ref": hello.get("id"), "name": "hello.ok",
                "ok": True, "ts": now_ms(),
                "payload": {
                    "server": {"v": VERSION, "ts": now_ms()},
                    "device": {"id": dev["id"], "name": dev.get("name"), "online": dev["id"] in hub.devices},
                    "caps": cconn.caps,
                },
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
