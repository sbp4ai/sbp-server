"""SBP core: tokens, device store, session jsonl storage."""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import re
import secrets
import time
from pathlib import Path
from typing import Any, Iterable

BIND_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"  # 去掉 0O1I
CODE_TTL = 600
CLIENT_TOKEN_TTL = 12 * 3600


# ---------------------------------------------------------------- tokens
def _b64e(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


def _b64d(text: str) -> bytes:
    pad = "=" * (-len(text) % 4)
    return base64.urlsafe_b64decode(text + pad)


def sign(secret: bytes, payload: dict) -> str:
    body = _b64e(json.dumps(payload, separators=(",", ":"), sort_keys=True).encode())
    mac = hmac.new(secret, body.encode(), hashlib.sha256).digest()
    return f"{body}.{_b64e(mac)}"


def verify(secret: bytes, token: str) -> dict | None:
    try:
        body, mac = token.split(".", 1)
    except ValueError:
        return None
    expect = _b64e(hmac.new(secret, body.encode(), hashlib.sha256).digest())
    if not hmac.compare_digest(expect, mac):
        return None
    try:
        payload = json.loads(_b64d(body))
    except Exception:
        return None
    if int(payload.get("exp", 0)) < int(time.time()):
        return None
    return payload


# ---------------------------------------------------------------- helpers
def new_device_id() -> str:
    return "dev_" + secrets.token_hex(6)


def new_sid() -> str:
    return "s_" + secrets.token_hex(4)


def new_sid_code() -> str:
    raw = "".join(secrets.choice(BIND_ALPHABET) for _ in range(8))
    return f"{raw[:4]}-{raw[4:]}"


def now_ms() -> int:
    return int(time.time() * 1000)


class Store:
    """File-backed store. 单进程使用，读写用一把锁即可。"""

    def __init__(self, root: str | Path):
        self.root = Path(root).expanduser()
        (self.root / "sessions").mkdir(parents=True, exist_ok=True)
        self.devices_path = self.root / "devices.json"
        self.secret_path = self.root / "secret.key"
        self._devices: dict[str, dict] = {}
        self._codes: dict[str, dict] = {}
        self._index: dict[str, dict[str, dict]] = {}
        self._load()

    # -- persistence
    def _load(self) -> None:
        if self.secret_path.exists():
            self.secret = self.secret_path.read_bytes()
        else:
            self.secret = secrets.token_bytes(32)
            self.secret_path.write_bytes(self.secret)
            os.chmod(self.secret_path, 0o600)
        if self.devices_path.exists():
            self._devices = json.loads(self.devices_path.read_text())
        for did in list(self._devices):
            idx = self.root / "sessions" / did / "index.json"
            if idx.exists():
                self._index[did] = json.loads(idx.read_text())

    def _flush_devices(self) -> None:
        self.devices_path.write_text(json.dumps(self._devices, indent=2, ensure_ascii=False))
        os.chmod(self.devices_path, 0o600)

    def _flush_index(self, did: str) -> None:
        d = self.root / "sessions" / did
        d.mkdir(parents=True, exist_ok=True)
        (d / "index.json").write_text(json.dumps(self._index.get(did, {}), indent=1, ensure_ascii=False))

    # -- devices
    def register(self, name: str, agent: str, fingerprint: str) -> dict:
        for dev in self._devices.values():
            if fingerprint and dev.get("fingerprint") == fingerprint:
                dev["secret"] = secrets.token_hex(24)
                dev["name"] = name or dev["name"]
                dev["agent"] = agent or dev["agent"]
                code = self.issue_code(dev["id"])
                self._flush_devices()
                return {"device": dev, "bind_code": code}
        did = new_device_id()
        dev = {
            "id": did,
            "name": name or "unnamed-device",
            "agent": agent or "generic",
            "fingerprint": fingerprint,
            "secret": secrets.token_hex(24),
            "created_at": now_ms(),
            "bindings": [],
            "revoked": False,
        }
        self._devices[did] = dev
        self._index.setdefault(did, {})
        self._flush_devices()
        return {"device": dev, "bind_code": self.issue_code(did)}

    def device(self, did: str) -> dict | None:
        return self._devices.get(did)

    def issue_code(self, did: str) -> str:
        for code, meta in list(self._codes.items()):
            if meta["device_id"] == did:
                self._codes.pop(code, None)
        code = new_sid_code()
        self._codes[code] = {"device_id": did, "exp": time.time() + CODE_TTL}
        return code

    def code_ttl(self, code: str) -> int:
        meta = self._codes.get(code)
        if not meta:
            return 0
        return max(0, int(meta["exp"] - time.time()))

    def consume_code(self, code: str, im_user: str, channel: str) -> dict | None:
        code = code.strip().upper().replace(" ", "")
        if "-" not in code and len(code) == 8:
            code = f"{code[:4]}-{code[4:]}"
        meta = self._codes.get(code)
        if not meta or meta["exp"] < time.time():
            return None
        self._codes.pop(code, None)  # 一次性
        dev = self._devices.get(meta["device_id"])
        if not dev or dev.get("revoked"):
            return None
        binding = {"im_user": im_user, "channel": channel, "at": now_ms()}
        dev.setdefault("bindings", []).append(binding)
        self._flush_devices()
        return dev

    def revoke(self, did: str) -> bool:
        dev = self._devices.get(did)
        if not dev:
            return False
        dev["revoked"] = True
        dev["bindings"] = []
        self._flush_devices()
        return True

    def client_token(self, dev: dict, sub: str, caps: list[str] | None = None) -> str:
        payload = {
            "sub": sub,
            "role": "client",
            "did": dev["id"],
            "cap": caps if caps is not None else dev.get("caps", []),
            "exp": int(time.time()) + CLIENT_TOKEN_TTL,
            "jti": secrets.token_hex(8),
        }
        return sign(self.secret, payload)

    # -- sessions
    def index(self, did: str) -> list[dict]:
        return sorted(
            self._index.get(did, {}).values(),
            key=lambda s: s.get("updated_at", 0),
            reverse=True,
        )

    def upsert_session(self, did: str, sid: str, patch: dict) -> dict:
        bucket = self._index.setdefault(did, {})
        entry = bucket.get(sid, {"sid": sid, "last_seq": 0, "msg_count": 0, "status": "idle"})
        for k, v in patch.items():
            if v is None or k in ("last_seq", "msg_count"):  # seq 由 server 独占
                continue
            entry[k] = v
        bucket[sid] = entry
        self._flush_index(did)
        return entry

    def _jsonl(self, did: str, sid: str) -> Path:
        d = self.root / "sessions" / did
        d.mkdir(parents=True, exist_ok=True)
        return d / f"{sid}.jsonl"

    def append_message(self, did: str, sid: str, msg: dict) -> dict:
        bucket = self._index.setdefault(did, {})
        entry = bucket.get(sid) or self.upsert_session(did, sid, {"title": sid})
        seq = int(entry.get("last_seq", 0)) + 1
        rec = {"seq": seq, **msg}
        with self._jsonl(did, sid).open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
        entry["last_seq"] = seq
        entry["msg_count"] = int(entry.get("msg_count", 0)) + 1
        entry["updated_at"] = rec.get("ts") or now_ms()
        if msg.get("role") == "agent" and msg.get("text"):
            entry["preview"] = re.sub(r"\s+", " ", msg["text"])[:120]
        self._flush_index(did)
        return rec

    def history(self, did: str, sid: str, before_seq: int | None, limit: int = 20) -> dict:
        path = self._jsonl(did, sid)
        if not path.exists():
            return {"items": [], "has_more": False, "next_before_seq": None}
        limit = max(1, min(int(limit or 20), 100))
        upper = int(before_seq) if before_seq else 10**12
        picked: list[dict] = []
        has_more = False
        # 倒序读过整个文件（单文件 < 数万行，v0.1 够用；v0.2 换 mmap 索引）
        lines = path.read_text(encoding="utf-8").splitlines()
        for line in reversed(lines):
            if not line.strip():
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue
            if int(rec.get("seq", 0)) >= upper:
                continue
            if len(picked) < limit:
                picked.append(rec)
            else:
                has_more = True
                break
        picked.reverse()
        return {
            "items": picked,
            "has_more": has_more,
            "next_before_seq": picked[0]["seq"] if picked else None,
        }

    def messages_after(self, did: str, sid: str, after_seq: int, limit: int = 200) -> list[dict]:
        path = self._jsonl(did, sid)
        if not path.exists():
            return []
        out = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            rec = json.loads(line)
            if int(rec.get("seq", 0)) > after_seq:
                out.append(rec)
                if len(out) >= limit:
                    break
        return out


def iter_lines(text: str) -> Iterable[str]:
    return text.splitlines()
