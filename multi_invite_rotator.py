#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Multi-session inviter (Pyrogram, sync) with:
- One active session at a time (no "all clients started")
- Robust stop/start lifecycle (context manager)
- Persistent state (state.json): session pointer, cooldowns, daily counter, pending username
- In-memory cache of processed usernames (bootstrapped once from log)
- Pending username: do not advance queue until current username is finalized
- PeerFlood handling: global pause + session cooldown (NO switching to another session for same username)
"""

import csv
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo

from pyrogram import Client, errors

# =========================
# CONFIG (ENV overridable)
# =========================

def _env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v is not None and str(v).strip() != "" else default

def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return default
    try:
        return int(v)
    except ValueError:
        return default

# Base dirs
DATA_DIR = Path(_env_str("DATA_DIR", ".")).expanduser().resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)

def _resolve_path(p: str, base: Path) -> Path:
    """
    If p is relative, interpret it relative to base (DATA_DIR),
    not relative to current working directory.
    """
    pp = Path(p).expanduser()
    if not pp.is_absolute():
        pp = (base / pp).resolve()
    return pp

# Required values
GROUP = _env_str("GROUP", "@advocate_ua_1")
APP_TZ = _env_str("APP_TZ", "Europe/Berlin")
TZ = ZoneInfo(APP_TZ)

# Files
USERNAME_CSV = _resolve_path(_env_str("CSV_PATH", str(DATA_DIR / "invite_unique_usernames_clean.csv")), DATA_DIR)
LOG_CSV = _resolve_path(_env_str("LOG_CSV", str(DATA_DIR / "multi_invite_log.csv")), DATA_DIR)
SESSIONS_JSON = _resolve_path(_env_str("SESSIONS_JSON", str(DATA_DIR / "sessions.json")), DATA_DIR)
STATE_JSON = _resolve_path(_env_str("STATE_JSON", str(DATA_DIR / "state.json")), DATA_DIR)

# Inviting policy knobs (your existing defaults)
BATCH_PER_SESSION = _env_int("BATCH_PER_SESSION", 5)

DELAY_BETWEEN_USERNAMES_SEC = _env_int("DELAY_BETWEEN_USERNAMES_SEC", 5 * 60)
DELAY_BETWEEN_SESSIONS_SEC = _env_int("DELAY_BETWEEN_SESSIONS_SEC", 5 * 60)

MAX_DAILY_ADDED = _env_int("MAX_DAILY_ADDED", 100)
FAST_SKIP_SLEEP_SEC = _env_int("FAST_SKIP_SLEEP_SEC", 1)

# For PeerFlood we set a conservative cooldown (seconds)
PEERFLOOD_COOLDOWN_SEC = _env_int("PEERFLOOD_COOLDOWN_SEC", 3600)  # 1 hour default

# Optional: verify access to GROUP each time a session is used
PREFLIGHT_CHECK = _env_int("PREFLIGHT_CHECK", 1) == 1


# =========================
# DATA STRUCTURES
# =========================

@dataclass(frozen=True)
class SessionCfg:
    session_name: str
    api_id: int
    api_hash: str
    session_string: Optional[str] = None


# =========================
# TIME HELPERS
# =========================

def now_dt() -> datetime:
    return datetime.now(TZ)

def now_ts() -> str:
    return now_dt().strftime("%Y-%m-%d %H:%M:%S")

def today_prefix() -> str:
    return now_dt().strftime("%Y-%m-%d")

def seconds_until_next_midnight() -> int:
    now = now_dt()
    tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return max(1, int((tomorrow - now).total_seconds()))


# =========================
# LOGGING (CSV)
# =========================

LOG_FIELDS = ["timestamp", "session", "username", "status", "reason"]

TERMINAL_NOT_ADDED_REASONS: Set[str] = {
    "UsernameNotOccupied",
    "UsernameInvalid",
    "UserPrivacyRestricted",
    "UserNotMutualContact",
    "UserBannedInChannel",
    "UserChannelsTooMuch",
}

FAST_SLEEP_REASONS: Set[str] = set(TERMINAL_NOT_ADDED_REASONS) | {
    "UserAlreadyParticipant",
    "already_in_group",
}

def ensure_log_header(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.stat().st_size > 0:
        return
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=LOG_FIELDS)
        w.writeheader()

def append_log(path: Path, row: Dict[str, str]) -> None:
    ensure_log_header(path)
    safe_row = {k: row.get(k, "") for k in LOG_FIELDS}
    with path.open("a", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=LOG_FIELDS)
        w.writerow(safe_row)
        f.flush()
        try:
            os.fsync(f.fileno())
        except Exception:
            pass


# =========================
# STATE (JSON)
# =========================

def load_state(path: Path) -> Dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        # if corrupted, do not crash; start fresh
        return {}

def save_state(path: Path, state: Dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)

def state_get_int(state: Dict, key: str, default: int = 0) -> int:
    v = state.get(key, default)
    try:
        return int(v)
    except Exception:
        return default

def state_get_str(state: Dict, key: str, default: str = "") -> str:
    v = state.get(key, default)
    return str(v) if v is not None else default


# =========================
# USERNAME IO
# =========================

def sanitize_username(u: str) -> str:
    u = (u or "").strip()
    if u.startswith("@"):
        u = u[1:]
    return u

def load_usernames(path: Path) -> List[str]:
    if not path.exists():
        raise FileNotFoundError(
            f"CSV не найден: {path}\n"
            f"Проверьте CSV_PATH (лучше абсолютный путь, например /var/data/...)."
        )

    for enc in ("utf-8-sig", "utf-8"):
        try:
            with path.open("r", newline="", encoding=enc) as f:
                rows = list(csv.reader(f))
            break
        except UnicodeDecodeError:
            continue
    else:
        with path.open("r", newline="", encoding="latin-1") as f:
            rows = list(csv.reader(f))

    if not rows:
        return []

    header = [c.strip().lower() for c in rows[0]]
    data_rows = rows
    col_idx = 0

    if any(h in ("username", "@username", "user", "nickname") for h in header):
        for i, h in enumerate(header):
            if h in ("username", "@username", "user", "nickname"):
                col_idx = i
                break
        data_rows = rows[1:]

    usernames: List[str] = []
    for r in data_rows:
        if not r:
            continue
        if col_idx >= len(r):
            continue
        u = sanitize_username(r[col_idx])
        if u:
            usernames.append(u)

    # unique while preserving order
    seen: Set[str] = set()
    out: List[str] = []
    for u in usernames:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


# =========================
# BOOTSTRAP FROM LOG
# =========================

def bootstrap_processed_and_daily(log_path: Path, day_prefix: str) -> Tuple[Set[str], int]:
    processed: Set[str] = set()
    daily_added = 0

    if not log_path.exists():
        return processed, daily_added

    with log_path.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            username = sanitize_username(row.get("username") or "")
            if not username:
                continue
            status = (row.get("status") or "").strip().lower()
            reason = (row.get("reason") or "").strip()

            ts = (row.get("timestamp") or "").strip()
            if ts.startswith(day_prefix) and status == "added":
                daily_added += 1

            if status in ("added", "already_in_group"):
                processed.add(username)
                continue

            if status == "not_added" and reason in TERMINAL_NOT_ADDED_REASONS:
                processed.add(username)
                continue

    return processed, daily_added


# =========================
# SESSIONS
# =========================

def load_sessions_from_json(path: Path) -> List[SessionCfg]:
    if not path.exists():
        raise FileNotFoundError(
            f"Sessions config not found: {path}\n"
            f"Создайте sessions.json (на Render удобно как Secret File)."
        )

    data = json.loads(path.read_text(encoding="utf-8"))
    sessions: List[SessionCfg] = []
    for item in data:
        sessions.append(
            SessionCfg(
                session_name=str(item["session_name"]),
                api_id=int(item["api_id"]),
                api_hash=str(item["api_hash"]),
                session_string=str(item["session_string"]) if item.get("session_string") else None,
            )
        )

    if not sessions:
        raise ValueError("sessions.json пустой — добавьте хотя бы одну сессию.")

    return sessions

def build_client(s: SessionCfg) -> Client:
    kwargs = {
        "name": s.session_name,
        "api_id": s.api_id,
        "api_hash": s.api_hash,
        "no_updates": True,
        "workdir": str(DATA_DIR),
    }
    if s.session_string:
        kwargs["session_string"] = s.session_string
    return Client(**kwargs)


# =========================
# INVITE CORE
# =========================

def invite_once(client: Client, session_name: str, group: str, username: str) -> Tuple[Dict[str, str], Optional[int], str]:
    """
    Returns: (row_for_log, extra_sleep_sec, class_tag)
      class_tag in {"ok", "terminal_user", "user_already", "session_throttle", "config_error", "unknown_error"}
    """
    ts = now_ts()
    u = sanitize_username(username)

    if not u:
        return (
            {"timestamp": ts, "session": session_name, "username": username, "status": "skipped", "reason": "EmptyUsername"},
            None,
            "terminal_user",
        )

    try:
        client.add_chat_members(chat_id=group, user_ids=[u])
        return (
            {"timestamp": ts, "session": session_name, "username": u, "status": "added", "reason": "OK"},
            None,
            "ok",
        )

    except errors.UserAlreadyParticipant:
        return (
            {"timestamp": ts, "session": session_name, "username": u, "status": "already_in_group", "reason": "UserAlreadyParticipant"},
            None,
            "user_already",
        )

    except errors.UsernameInvalid:
        return (
            {"timestamp": ts, "session": session_name, "username": u, "status": "not_added", "reason": "UsernameInvalid"},
            None,
            "terminal_user",
        )

    except errors.UsernameNotOccupied:
        return (
            {"timestamp": ts, "session": session_name, "username": u, "status": "not_added", "reason": "UsernameNotOccupied"},
            None,
            "terminal_user",
        )

    except errors.UserPrivacyRestricted:
        return (
            {"timestamp": ts, "session": session_name, "username": u, "status": "not_added", "reason": "UserPrivacyRestricted"},
            None,
            "terminal_user",
        )

    except errors.UserNotMutualContact:
        return (
            {"timestamp": ts, "session": session_name, "username": u, "status": "not_added", "reason": "UserNotMutualContact"},
            None,
            "terminal_user",
        )

    except errors.UserBannedInChannel:
        return (
            {"timestamp": ts, "session": session_name, "username": u, "status": "not_added", "reason": "UserBannedInChannel"},
            None,
            "terminal_user",
        )

    except errors.UserChannelsTooMuch:
        return (
            {"timestamp": ts, "session": session_name, "username": u, "status": "not_added", "reason": "UserChannelsTooMuch"},
            None,
            "terminal_user",
        )

    except errors.ChatAdminRequired:
        # This is not a username problem; this is group/session permission issue.
        return (
            {"timestamp": ts, "session": session_name, "username": u, "status": "not_added", "reason": "ChatAdminRequired"},
            None,
            "config_error",
        )

    except errors.PeerFlood:
        # Anti-spam restriction. We treat it as a session throttle: cooldown + global pause.
        extra = max(PEERFLOOD_COOLDOWN_SEC, int(DELAY_BETWEEN_SESSIONS_SEC))
        return (
            {"timestamp": ts, "session": session_name, "username": u, "status": "not_added", "reason": "PeerFlood"},
            extra,
            "session_throttle",
        )

    except errors.FloodWait as e:
        wait_sec = int(getattr(e, "value", 0) or 0)
        wait_sec = max(1, wait_sec)
        return (
            {"timestamp": ts, "session": session_name, "username": u, "status": "not_added", "reason": f"FloodWait:{wait_sec}"},
            wait_sec + 3,
            "session_throttle",
        )

    except (errors.Unauthorized, errors.AuthKeyUnregistered, errors.SessionRevoked):
        # Session is broken. Treat as config/session error.
        return (
            {"timestamp": ts, "session": session_name, "username": u, "status": "not_added", "reason": "SessionUnauthorized"},
            None,
            "config_error",
        )

    except Exception as e:
        return (
            {"timestamp": ts, "session": session_name, "username": u, "status": "not_added", "reason": f"Exception:{type(e).__name__}:{e}"},
            None,
            "unknown_error",
        )


def compute_base_sleep(row: Dict[str, str]) -> int:
    status = (row.get("status") or "").strip().lower()
    reason = (row.get("reason") or "").strip()

    if status == "already_in_group":
        return max(1, int(FAST_SKIP_SLEEP_SEC))

    if reason in FAST_SLEEP_REASONS:
        return max(1, int(FAST_SKIP_SLEEP_SEC))

    return max(1, int(DELAY_BETWEEN_USERNAMES_SEC))


# =========================
# SESSION SELECTION
# =========================

def _parse_iso_dt(s: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None

def _dt_to_iso(d: datetime) -> str:
    return d.isoformat()

def pick_next_session(
    sessions: List[SessionCfg],
    cooldowns: Dict[str, str],
    start_idx: int,
) -> Tuple[Optional[int], Optional[SessionCfg], Optional[int]]:
    """
    Returns:
      (idx, session, sleep_until_ready_sec)
    If all in cooldown, idx/session are None and sleep is the min time until next ready.
    """
    n = len(sessions)
    now = now_dt()

    best_sleep: Optional[int] = None

    for k in range(n):
        i = (start_idx + k) % n
        s = sessions[i]
        cd_iso = cooldowns.get(s.session_name)
        if not cd_iso:
            return i, s, None

        cd_dt = _parse_iso_dt(cd_iso)
        if not cd_dt or cd_dt <= now:
            return i, s, None

        sleep_sec = int((cd_dt - now).total_seconds())
        if best_sleep is None or sleep_sec < best_sleep:
            best_sleep = sleep_sec

    return None, None, max(1, best_sleep or 60)


# =========================
# PREFLIGHT
# =========================

def preflight(client: Client, session_name: str) -> bool:
    """
    Basic checks:
    - can access GROUP (get_chat)
    - can read own membership (optional)
    """
    if not PREFLIGHT_CHECK:
        return True
    try:
        client.get_chat(GROUP)
    except Exception as e:
        print(f"[{now_ts()}] [{session_name}] [PREFLIGHT_FAIL] get_chat({GROUP}): {type(e).__name__}: {e}")
        return False

    try:
        client.get_chat_member(GROUP, "me")
    except Exception:
        # Not fatal; some chats may restrict member queries.
        pass

    return True


# =========================
# MAIN LOOP
# =========================

def run() -> None:
    print(f"[{now_ts()}] Boot. TZ={APP_TZ}. DATA_DIR={DATA_DIR}")
    print(f"[{now_ts()}] GROUP={GROUP}")
    print(f"[{now_ts()}] CSV_PATH={USERNAME_CSV}")
    print(f"[{now_ts()}] LOG_CSV={LOG_CSV}")
    print(f"[{now_ts()}] SESSIONS_JSON={SESSIONS_JSON}")
    print(f"[{now_ts()}] STATE_JSON={STATE_JSON}")

    sessions = load_sessions_from_json(SESSIONS_JSON)
    ensure_log_header(LOG_CSV)

    # Load state
    state = load_state(STATE_JSON)
    session_idx = state_get_int(state, "session_idx", 0)

    # Cooldowns per session_name: iso datetime
    cooldowns: Dict[str, str] = state.get("cooldowns", {}) if isinstance(state.get("cooldowns"), dict) else {}

    # Global pause until (iso dt) - used on PeerFlood etc.
    global_pause_until_iso = state_get_str(state, "global_pause_until", "")
    global_pause_until = _parse_iso_dt(global_pause_until_iso) if global_pause_until_iso else None

    # Pending username (do not advance queue until done)
    pending_username = state_get_str(state, "pending_username", "")
    pending_username = sanitize_username(pending_username)

    # Daily counter in state
    state_day = state_get_str(state, "daily_day", "")
    state_daily_added = state_get_int(state, "daily_added", 0)

    # Bootstrap from log (once) to build processed cache and today's count
    day = today_prefix()
    processed, log_daily_added = bootstrap_processed_and_daily(LOG_CSV, day)

    # Prefer state counter if state_day == today, else use log bootstrap
    if state_day == day:
        daily_added = max(state_daily_added, log_daily_added)
    else:
        daily_added = log_daily_added
        state_day = day

    print(f"[{now_ts()}] Loaded sessions={len(sessions)}. processed={len(processed)}. daily_added={daily_added}/{MAX_DAILY_ADDED}")

    def persist_state() -> None:
        nonlocal state, session_idx, cooldowns, global_pause_until, pending_username, state_day, daily_added
        st = {
            "session_idx": int(session_idx),
            "cooldowns": dict(cooldowns),
            "global_pause_until": _dt_to_iso(global_pause_until) if global_pause_until else "",
            "pending_username": pending_username or "",
            "daily_day": state_day,
            "daily_added": int(daily_added),
            "updated_at": _dt_to_iso(now_dt()),
        }
        save_state(STATE_JSON, st)

    while True:
        # Refresh day (timezone-aware)
        day = today_prefix()
        if day != state_day:
            state_day = day
            daily_added = 0
            # Optional: also clear pending (you can keep it; I keep it)
            print(f"[{now_ts()}] New day {state_day}. Daily counter reset.")
            persist_state()

        # Daily limit
        if daily_added >= MAX_DAILY_ADDED:
            sleep_sec = seconds_until_next_midnight()
            print(f"[{now_ts()}] Daily limit reached: {daily_added}/{MAX_DAILY_ADDED}. Sleep {sleep_sec}s until midnight ({APP_TZ}).")
            time.sleep(sleep_sec)
            continue

        # Global pause (e.g. after PeerFlood)
        if global_pause_until and global_pause_until > now_dt():
            sleep_sec = max(1, int((global_pause_until - now_dt()).total_seconds()))
            print(f"[{now_ts()}] Global pause active. Sleep {sleep_sec}s until {_dt_to_iso(global_pause_until)}")
            time.sleep(sleep_sec)
            continue
        else:
            global_pause_until = None

        # Load usernames and compute queue
        usernames_all = load_usernames(USERNAME_CSV)
        if not usernames_all:
            print(f"[{now_ts()}] CSV empty. Recheck in 1 hour.")
            time.sleep(3600)
            continue

        # If pending is set, try it first; else pick next from queue
        if pending_username:
            target = pending_username
        else:
            queue = [u for u in usernames_all if u not in processed]
            if not queue:
                print(f"[{now_ts()}] Queue empty (all processed). Recheck in 1 hour.")
                time.sleep(3600)
                continue
            target = queue[0]
            pending_username = target
            persist_state()

        # Pick next available session
        idx, sess, sleep_ready = pick_next_session(sessions, cooldowns, session_idx)
        if sess is None:
            print(f"[{now_ts()}] All sessions in cooldown. Sleep {sleep_ready}s.")
            time.sleep(sleep_ready or 60)
            continue

        # Run one attempt (single username) with chosen session
        print(f"[{now_ts()}] === SESSION {idx+1}/{len(sessions)}: {sess.session_name}. DailyAdded={daily_added}/{MAX_DAILY_ADDED}. Target=@{target} ===")

        try:
            with build_client(sess) as app:
                print(f"[{now_ts()}] [SESSION_START] {sess.session_name}")
                if not preflight(app, sess.session_name):
                    # config/group access issue; cooldown this session briefly and do not advance target
                    cooldowns[sess.session_name] = _dt_to_iso(now_dt() + timedelta(minutes=10))
                    print(f"[{now_ts()}] [{sess.session_name}] Preflight failed. Cooldown 10 min.")
                    persist_state()
                    time.sleep(5)
                    continue

                row, extra_sleep, class_tag = invite_once(app, sess.session_name, GROUP, target)
                append_log(LOG_CSV, row)
                print(f"[{now_ts()}] [{sess.session_name}] Result: {row['status']} / {row['reason']}")

        except Exception as e:
            # catastrophic client-level failure
            print(f"[{now_ts()}] [{sess.session_name}] [CLIENT_FAIL] {type(e).__name__}: {e}")
            cooldowns[sess.session_name] = _dt_to_iso(now_dt() + timedelta(minutes=30))
            persist_state()
            time.sleep(10)
            continue

        # Update rotation pointer (only after an attempt)
        session_idx = (idx + 1) % len(sessions)

        # Process result: update in-memory processed and daily counter
        status = (row.get("status") or "").strip().lower()
        reason = (row.get("reason") or "").strip()
        u_clean = sanitize_username(row.get("username") or target)

        # Decide whether this username is "finalized" (so we can clear pending)
        finalized = False

        if status == "added":
            daily_added += 1
            processed.add(u_clean)
            finalized = True

        elif status == "already_in_group":
            processed.add(u_clean)
            finalized = True

        elif status == "not_added" and reason in TERMINAL_NOT_ADDED_REASONS:
            processed.add(u_clean)
            finalized = True

        elif status == "not_added" and reason == "ChatAdminRequired":
            # This is not a user issue — configuration/permissions
            # Stop the whole loop to avoid burning attempts.
            persist_state()
            raise RuntimeError(
                f"ChatAdminRequired for group={GROUP}. "
                f"У аккаунта {sess.session_name} нет прав добавлять участников или группа недоступна."
            )

        # Handle throttles
        if status == "not_added" and reason == "PeerFlood":
            # IMPORTANT: we keep pending_username (do not move forward).
            # We also apply:
            # - session cooldown
            # - global pause (so we do not switch to other sessions immediately)
            cd = now_dt() + timedelta(seconds=max(1, extra_sleep or PEERFLOOD_COOLDOWN_SEC))
            cooldowns[sess.session_name] = _dt_to_iso(cd)
            global_pause_until = cd
            print(f"[{now_ts()}] [{sess.session_name}] PeerFlood -> cooldown until {_dt_to_iso(cd)} and GLOBAL PAUSE.")
            persist_state()
            time.sleep(max(1, int(extra_sleep or PEERFLOOD_COOLDOWN_SEC)))
            continue

        if status == "not_added" and reason.startswith("FloodWait:"):
            # Keep pending_username and wait the mandatory time.
            if extra_sleep:
                print(f"[{now_ts()}] Mandatory sleep {extra_sleep}s (FloodWait). Pending username stays: @{pending_username}")
                persist_state()
                time.sleep(max(1, int(extra_sleep)))
                continue

        # If username finalized, clear pending
        if finalized:
            pending_username = ""

        # Persist state
        persist_state()

        # Base sleep between attempts (fast for terminal)
        base_sleep = compute_base_sleep(row)
        print(f"[{now_ts()}] Sleep {base_sleep}s before next action.")
        time.sleep(base_sleep)

        # Sleep between sessions (optional; since we do 1 username per iteration, this is your "session pacing")
        # Keep it, but short-circuit if pending cleared? We keep it always to match your original pacing.
        print(f"[{now_ts()}] Session pacing sleep {DELAY_BETWEEN_SESSIONS_SEC}s.")
        time.sleep(max(1, int(DELAY_BETWEEN_SESSIONS_SEC)))


if __name__ == "__main__":
    run()
