"""房间列表游标分页与同步核心模块。

修复目标：
1. 稳定排序键：游标基于 (created_at, room_id) 复合键，删除不改变相对顺序；
2. 广播序号与幂等：每频道单调递增 seq，客户端按 seq 去重、拒绝旧状态覆盖新状态；
3. 跨区去重：同一 room_id 跨区只保留一条（版本最新者）；
4. 断线重连全量同步：重连后下发全量快照 + 版本号，客户端整体替换缓存；
5. 游标失效处理：锚点房间被删或游标过期时显式失效，回退全量刷新；
6. 列表与缓存版本号：每次变更 bump 版本，合并时高版本优先。
"""

from __future__ import annotations

import threading
import time

# ---------------------------------------------------------------------------
# 模块级状态（序号、版本、已应用集合、审计、指标）
# ---------------------------------------------------------------------------

_lock = threading.Lock()
_seq_counters: dict = {}        # channel -> 当前广播序号
_version_counters: dict = {}    # scope -> 当前版本号
_applied: dict = {}             # channel -> 已应用的最大 seq（拒绝乱序/重复）
_audit_log: list = []
_metrics: dict = {
    "pages_served": 0,
    "cursor_invalidated": 0,
    "duplicates_dropped": 0,
    "stale_broadcasts_dropped": 0,
    "full_syncs": 0,
}

VALID_STATUS = {"open", "full", "playing", "closed"}
CURSOR_TTL_SECONDS = 300


# ---------------------------------------------------------------------------
# 1. 稳定排序键
# ---------------------------------------------------------------------------

def stable_key(room):
    """返回房间的稳定排序键 (created_at, room_id)。

    键只依赖房间创建时确定、此后不变的字段，因此任意房间被删除
    都不会改变其余房间的相对顺序，游标分页不会跳页或重复。
    """
    if isinstance(room, dict):
        return (room.get("created_at", 0), room.get("room_id", room.get("id", 0)))
    if isinstance(room, (tuple, list)) and len(room) >= 2:
        return (room[0], room[1])
    return (0, room)


def sort_rooms(rooms):
    """按稳定键排序房间列表。"""
    return sorted(rooms, key=stable_key)


# ---------------------------------------------------------------------------
# 2. 广播序号与幂等
# ---------------------------------------------------------------------------

def broadcast_seq(channel=0):
    """为指定频道分配下一个单调递增的广播序号。"""
    with _lock:
        nxt = _seq_counters.get(channel, 0) + 1
        _seq_counters[channel] = nxt
    return nxt


def idempotent(channel, seq=None):
    """判断一条广播是否应当被应用（幂等 + 乱序拒绝）。

    每个频道只接受严格大于已应用序号的广播；重复或乱序到达的
    旧广播被丢弃，避免客户端用旧房间状态覆盖新状态。
    返回 True 表示应应用，False 表示应丢弃。
    """
    if seq is None:  # 兼容单参数调用：idempotent(seq)
        channel, seq = 0, channel
    with _lock:
        last = _applied.get(channel, 0)
        if seq <= last:
            _metrics["stale_broadcasts_dropped"] += 1
            return False
        _applied[channel] = seq
    return True


# ---------------------------------------------------------------------------
# 3. 跨区去重
# ---------------------------------------------------------------------------

def cross_region_dedupe(rooms):
    """跨区匹配房间去重：同一 room_id 只保留一条。

    保留 version 最高的一条；version 相同则保留 region 字典序最小者，
    保证结果与各区返回顺序无关（确定性）。
    """
    if not isinstance(rooms, (list, tuple)):
        return [rooms]
    best = {}
    for r in rooms:
        if isinstance(r, dict):
            rid = r.get("room_id", r.get("id"))
            cur = best.get(rid)
            if cur is None or (r.get("version", 0), ) >= (cur.get("version", 0), ):
                if cur is None or r.get("version", 0) > cur.get("version", 0) \
                        or str(r.get("region", "")) < str(cur.get("region", "")):
                    best[rid] = r
                    if cur is not None:
                        _metrics["duplicates_dropped"] += 1
        else:
            best.setdefault(r, r)
    return sort_rooms(list(best.values()))


# ---------------------------------------------------------------------------
# 4. 断线重连全量同步
# ---------------------------------------------------------------------------

def reconnect_sync(server_rooms=None, client_version=None):
    """断线重连全量同步。

    不做增量补偿（断线期间可能错过任意多条广播），直接返回服务端
    全量快照与当前版本号；客户端整体替换本地缓存，保证一致。
    """
    if server_rooms is None:
        rooms = []
    elif isinstance(server_rooms, (list, tuple)):
        rooms = cross_region_dedupe(list(server_rooms))
    else:
        rooms = [server_rooms]
    version = current_version("roomlist")
    _metrics["full_syncs"] += 1
    return {
        "type": "full_sync",
        "rooms": rooms,
        "version": version,
        "server_time": int(time.time()),
        "replaced": True,  # 客户端必须整体替换缓存，而非合并
    }


# ---------------------------------------------------------------------------
# 5. 游标失效处理
# ---------------------------------------------------------------------------

def cursor_invalidate(cursor, exists=None, now=None):
    """校验游标，返回规范化游标；失效时返回 None（调用方应走 fallback）。

    失效条件：
    - 锚点房间已被删除（exists(anchor) 为 False）；
    - 游标过期（签发时间距现在超过 CURSOR_TTL_SECONDS）。
    """
    if cursor is None:
        return None
    if isinstance(cursor, dict):
        anchor = cursor.get("room_id", cursor.get("id"))
        created_at = cursor.get("created_at", 0)
        issued_at = cursor.get("issued_at", now if now is not None else time.time())
    else:
        anchor, created_at, issued_at = cursor, 0, (now if now is not None else time.time())
    now = time.time() if now is None else now
    if now - issued_at > CURSOR_TTL_SECONDS:
        _metrics["cursor_invalidated"] += 1
        return None
    if exists is not None and not exists(anchor):
        _metrics["cursor_invalidated"] += 1
        return None
    return {"created_at": created_at, "room_id": anchor, "issued_at": issued_at}


def fallback(reason="cursor_invalid"):
    """游标失效时的回退策略：放弃游标分页，从头全量刷新。"""
    return {
        "action": "full_refresh",
        "cursor": None,
        "reason": reason,
        "version": current_version("roomlist"),
    }


def delete_gap(prev_tail_key=None, next_head_key=None, deleted_ids=None):
    """检测翻页之间由删除造成的空洞。

    返回空洞信息：若下一页首条与上一页尾条之间存在被删除的锚点，
    客户端应意识到中间有房间消失（属正常现象，稳定键保证不会因此
    漏拉未删除的房间）。
    """
    gap_ids = sorted(deleted_ids) if deleted_ids else []
    return {
        "has_gap": bool(gap_ids),
        "deleted_ids": gap_ids,
        "prev_tail": prev_tail_key,
        "next_head": next_head_key,
        "safe": True,  # 稳定键下删除不影响剩余房间的遍历完整性
    }


# ---------------------------------------------------------------------------
# 6. 列表与缓存版本号
# ---------------------------------------------------------------------------

def version_stamp(scope="roomlist"):
    """为指定作用域 bump 并返回新版本号（单调递增）。"""
    with _lock:
        nxt = _version_counters.get(scope, 0) + 1
        _version_counters[scope] = nxt
    return nxt


def current_version(scope="roomlist"):
    """读取当前版本号（不 bump）。"""
    with _lock:
        return _version_counters.get(scope, 0)


def cache_merge(server_rooms, cache_rooms=None, server_version=None, cache_version=None):
    """按版本号合并服务端列表与本地缓存。

    - 服务端版本 >= 缓存版本：以服务端为准（缓存整体替换）；
    - 服务端版本 < 缓存版本：说明收到的是过期快照，保留缓存；
    - 逐房间合并时，同一 room_id 取 version 较高者。
    """
    server_rooms = list(server_rooms) if isinstance(server_rooms, (list, tuple)) else [server_rooms]
    cache_rooms = list(cache_rooms) if isinstance(cache_rooms, (list, tuple)) else ([] if cache_rooms is None else [cache_rooms])
    if server_version is not None and cache_version is not None:
        if server_version < cache_version:
            return {"rooms": sort_rooms(cache_rooms), "version": cache_version, "source": "cache"}
        if not cache_rooms:
            return {"rooms": sort_rooms(server_rooms), "version": server_version, "source": "server"}
    merged = {}
    for r in cache_rooms + server_rooms:
        rid = r.get("room_id", r.get("id")) if isinstance(r, dict) else r
        cur = merged.get(rid)
        if cur is None:
            merged[rid] = r
        else:
            rv = r.get("version", 0) if isinstance(r, dict) else 0
            cv = cur.get("version", 0) if isinstance(cur, dict) else 0
            if rv >= cv:
                merged[rid] = r
    version = max(v for v in (server_version, cache_version) if v is not None) \
        if (server_version is not None or cache_version is not None) else current_version("roomlist")
    return {"rooms": sort_rooms(list(merged.values())), "version": version, "source": "merged"}


# ---------------------------------------------------------------------------
# 分页与拼装
# ---------------------------------------------------------------------------

def page_join(pages):
    """拼装多页结果：按稳定键排序并按 room_id 去重，保证翻页边界不重复。"""
    if not isinstance(pages, (list, tuple)):
        pages = [pages]
    rooms = []
    for page in pages:
        if isinstance(page, dict):
            rooms.extend(page.get("rooms", []))
        elif isinstance(page, (list, tuple)):
            rooms.extend(page)
        else:
            rooms.append(page)
    _metrics["pages_served"] += 1
    return cross_region_dedupe(rooms)


# ---------------------------------------------------------------------------
# 校验、审计、指标、确定性
# ---------------------------------------------------------------------------

def validation(room):
    """校验房间记录字段完整性，返回规范化后的房间。

    缺必要字段或状态非法时抛出 ValueError。
    """
    if not isinstance(room, dict):
        return {"room_id": room, "created_at": 0, "region": "default",
                "status": "open", "version": 0}
    missing = [k for k in ("room_id", "created_at") if k not in room]
    if missing:
        raise ValueError(f"room missing fields: {missing}")
    status = room.get("status", "open")
    if status not in VALID_STATUS:
        raise ValueError(f"invalid room status: {status}")
    normalized = dict(room)
    normalized.setdefault("region", "default")
    normalized.setdefault("version", 0)
    normalized["status"] = status
    return normalized


def audit(event, detail=None):
    """记录一条审计日志并返回该条目。"""
    entry = {
        "seq": len(_audit_log) + 1,
        "event": event,
        "detail": detail,
        "ts": int(time.time()),
    }
    _audit_log.append(entry)
    return entry


def metrics(name=None, delta=1):
    """读取指标快照；传入 name 时对该指标累加 delta 后返回快照。"""
    if name is not None:
        with _lock:
            _metrics[name] = _metrics.get(name, 0) + delta
    with _lock:
        return dict(_metrics)


def determinism(rooms):
    """返回房间列表的确定性规范形式（排序后的 room_id 序列）。

    相同输入集合无论到达顺序、来自哪个区，输出完全一致，
    可用于对账与测试断言。
    """
    if not isinstance(rooms, (list, tuple)):
        rooms = [rooms]
    deduped = cross_region_dedupe(list(rooms))
    return [r.get("room_id", r.get("id")) if isinstance(r, dict) else r for r in deduped]
