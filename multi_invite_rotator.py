# -*- coding: utf-8 -*-
import csv
import os
import time
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from zoneinfo import ZoneInfo

from pyrogram import Client, errors


# =========================
# CONFIG (через ENV можно переопределять)
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


GROUP = _env_str("GROUP", "@advocate_ua_1")

DATA_DIR = Path(_env_str("DATA_DIR", ".")).expanduser().resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)

USERNAME_CSV = Path(_env_str("CSV_PATH", str(DATA_DIR / "invite_unique_usernames_clean.csv"))).expanduser()
LOG_CSV = Path(_env_str("LOG_CSV", str(DATA_DIR / "multi_invite_log.csv"))).expanduser()
SESSIONS_JSON = Path(_env_str("SESSIONS_JSON", str(DATA_DIR / "sessions.json"))).expanduser()
STATE_JSON = Path(_env_str("STATE_JSON", str(DATA_DIR / "state.json"))).expanduser()

BATCH_PER_SESSION = _env_int("BATCH_PER_SESSION", 5)
DELAY_BETWEEN_USERNAMES_SEC = _env_int("DELAY_BETWEEN_USERNAMES_SEC", 5 * 60)   # 300 сек
DELAY_BETWEEN_SESSIONS_SEC = _env_int("DELAY_BETWEEN_SESSIONS_SEC", 5 * 60)     # 300 сек
MAX_DAILY_ADDED = _env_int("MAX_DAILY_ADDED", 100)
FAST_SKIP_SLEEP_SEC = _env_int("FAST_SKIP_SLEEP_SEC", 1)                         # 1 сек

RECONNECT_BETWEEN_SESSIONS = _env_int("RECONNECT_BETWEEN_SESSIONS", 1) == 1

APP_TZ = _env_str("APP_TZ", "Europe/Berlin")
TZ = ZoneInfo(APP_TZ)

# Задержки для подтверждения фактического вступления
CONFIRM_CHECK_1 = _env_int("CONFIRM_CHECK_1", 2)
CONFIRM_CHECK_2 = _env_int("CONFIRM_CHECK_2", 4)
CONFIRM_CHECK_3 = _env_int("CONFIRM_CHECK_3", 6)
CONFIRM_DELAYS = (CONFIRM_CHECK_1, CONFIRM_CHECK_2, CONFIRM_CHECK_3)


# =========================
# DATA STRUCTURES
# =========================
@dataclass
class SessionCfg:
    session_name: str
    api_id: int
    api_hash: str
    session_string: Optional[str] = None


# =========================
# HELPERS: TIME
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
# HELPERS: IO
# =========================
LOG_FIELDS = ["timestamp", "session", "username", "status", "reason"]

FAST_SKIP_REASONS = {
    "UsernameNotOccupied",
    "UsernameInvalid",
    "UserPrivacyRestricted",
    "UserNotMutualContact",
    "UserBannedInChannel",
    "UserChannelsTooMuch",
    "UserAlreadyParticipant",
    "AddedNotConfirmed",
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


def sanitize_username(u: str) -> str:
    u = (u or "").strip()
    if u.startswith("@"):
        u = u[1:]
    return u


def load_usernames(path: Path) -> List[str]:
    if not path.exists():
        raise FileNotFoundError(
            f"CSV не найден: {path}\n"
            f"Укажите корректный путь через ENV CSV_PATH или положите файл рядом."
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

    # уникализируем с сохранением порядка
    seen = set()
    out: List[str] = []
    for u in usernames:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def load_daily_added_count(log_path: Path, day_prefix: str) -> int:
    if not log_path.exists():
        return 0

    cnt = 0
    with log_path.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ts = (row.get("timestamp") or "").strip()
            if not ts.startswith(day_prefix):
                continue
            if (row.get("status") or "").strip().lower() == "added":
                cnt += 1
    return cnt


# =========================
# STATE
# =========================
def load_state(path: Path) -> Dict[str, int]:
    if not path.exists():
        return {"cursor": 0, "session_idx": 0}

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        cursor = int(data.get("cursor", 0))
        session_idx = int(data.get("session_idx", 0))
        return {
            "cursor": max(0, cursor),
            "session_idx": max(0, session_idx),
        }
    except Exception:
        return {"cursor": 0, "session_idx": 0}


def save_state(path: Path, state: Dict[str, int]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


# =========================
# SESSIONS
# =========================
def load_sessions_from_json(path: Path) -> List[SessionCfg]:
    if not path.exists():
        raise FileNotFoundError(
            f"Sessions config not found: {path}\n"
            f"Создайте sessions.json (лучше как Secret File на Render)."
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


# =========================
# CLIENT LIFECYCLE
# =========================
def safe_start(client: Client, session_name: str) -> bool:
    try:
        client.start()
        print(f"[{now_ts()}] [SESSION_START] {session_name}")
        return True
    except Exception as e:
        print(f"[{now_ts()}] [START_FAIL] [{session_name}] {type(e).__name__}: {e}")
        return False


def safe_stop(client: Client, session_name: str) -> None:
    try:
        client.stop()
        print(f"[{now_ts()}] [SESSION_STOP] {session_name}")
    except Exception as e:
        print(f"[{now_ts()}] [STOP_FAIL] [{session_name}] {type(e).__name__}: {e}")


def restart_client(client: Client, session_name: str, sleep_sec: int = 10) -> bool:
    safe_stop(client, session_name)
    time.sleep(max(1, int(sleep_sec)))
    return safe_start(client, session_name)


# =========================
# INVITE CORE
# =========================
def _confirm_member_status(client: Client, group: str, user_id: int) -> Tuple[bool, Optional[str]]:
    """
    Проверяет, действительно ли пользователь состоит в группе.
    """
    last_error = None

    for delay in CONFIRM_DELAYS:
        time.sleep(max(1, int(delay)))
        try:
            member = client.get_chat_member(group, user_id)
            status_str = str(getattr(member, "status", ""))
            # Подтверждаем любое "живое" участие в группе
            if any(x in status_str for x in ("MEMBER", "ADMINISTRATOR", "OWNER", "RESTRICTED")):
                return True, status_str
            last_error = f"unexpected_status:{status_str}"
        except Exception as e:
            last_error = f"{type(e).__name__}:{e}"

    return False, last_error


def invite_once(
    client: Client,
    session_name: str,
    group: str,
    username: str,
) -> Tuple[Dict[str, str], Optional[int]]:
    """
    Возвращает:
      - row для лога
      - extra_sleep_sec
    """
    ts = now_ts()
    username_clean = sanitize_username(username)

    if not username_clean:
        return (
            {"timestamp": ts, "session": session_name, "username": username, "status": "skipped", "reason": "Empty username"},
            None,
        )

    try:
        # 1) Резолвим username в user_id
        user_obj = client.get_users(username_clean)
        user_id = user_obj.id

        # 2) Пытаемся добавить по user_id
        client.add_chat_members(chat_id=group, user_ids=[user_id])

        # 3) Подтверждаем фактическое членство
        confirmed, check_info = _confirm_member_status(client, group, user_id)

        if confirmed:
            return (
                {
                    "timestamp": ts,
                    "session": session_name,
                    "username": username_clean,
                    "status": "added",
                    "reason": f"CONFIRMED:user_id={user_id};status={check_info}",
                },
                None,
            )

        return (
            {
                "timestamp": ts,
                "session": session_name,
                "username": username_clean,
                "status": "not_added",
                "reason": f"AddedNotConfirmed:user_id={user_id};check={check_info or 'unknown'}",
            },
            None,
        )

    except errors.UserAlreadyParticipant:
        return (
            {
                "timestamp": ts,
                "session": session_name,
                "username": username_clean,
                "status": "already_in_group",
                "reason": "UserAlreadyParticipant",
            },
            None,
        )

    except errors.UsernameInvalid:
        return (
            {
                "timestamp": ts,
                "session": session_name,
                "username": username_clean,
                "status": "not_added",
                "reason": "UsernameInvalid",
            },
            None,
        )

    except errors.UsernameNotOccupied:
        return (
            {
                "timestamp": ts,
                "session": session_name,
                "username": username_clean,
                "status": "not_added",
                "reason": "UsernameNotOccupied",
            },
            None,
        )

    except errors.UserPrivacyRestricted:
        return (
            {
                "timestamp": ts,
                "session": session_name,
                "username": username_clean,
                "status": "not_added",
                "reason": "UserPrivacyRestricted",
            },
            None,
        )

    except errors.UserNotMutualContact:
        return (
            {
                "timestamp": ts,
                "session": session_name,
                "username": username_clean,
                "status": "not_added",
                "reason": "UserNotMutualContact",
            },
            None,
        )

    except errors.UserBannedInChannel:
        return (
            {
                "timestamp": ts,
                "session": session_name,
                "username": username_clean,
                "status": "not_added",
                "reason": "UserBannedInChannel",
            },
            None,
        )

    except errors.UserChannelsTooMuch:
        return (
            {
                "timestamp": ts,
                "session": session_name,
                "username": username_clean,
                "status": "not_added",
                "reason": "UserChannelsTooMuch",
            },
            None,
        )

    except errors.ChatAdminRequired:
        return (
            {
                "timestamp": ts,
                "session": session_name,
                "username": username_clean,
                "status": "not_added",
                "reason": "ChatAdminRequired",
            },
            None,
        )

    except errors.PeerFlood:
        return (
            {
                "timestamp": ts,
                "session": session_name,
                "username": username_clean,
                "status": "not_added",
                "reason": "PeerFlood",
            },
            max(3600, int(DELAY_BETWEEN_SESSIONS_SEC)),
        )

    except errors.FloodWait as e:
        wait_sec = int(getattr(e, "value", 0) or 0)
        wait_sec = max(1, wait_sec)
        return (
            {
                "timestamp": ts,
                "session": session_name,
                "username": username_clean,
                "status": "not_added",
                "reason": f"FloodWait:{wait_sec}",
            },
            wait_sec + 3,
        )

    except errors.RPCError as e:
        return (
            {
                "timestamp": ts,
                "session": session_name,
                "username": username_clean,
                "status": "not_added",
                "reason": f"RPCError:{type(e).__name__}",
            },
            None,
        )

    except Exception as e:
        return (
            {
                "timestamp": ts,
                "session": session_name,
                "username": username_clean,
                "status": "not_added",
                "reason": f"Exception:{type(e).__name__}:{e}",
            },
            None,
        )


def compute_base_sleep(row: Dict[str, str]) -> int:
    status = (row.get("status") or "").strip().lower()
    reason = (row.get("reason") or "").strip()

    if status == "already_in_group":
        return max(1, int(FAST_SKIP_SLEEP_SEC))

    if reason in FAST_SKIP_REASONS:
        return max(1, int(FAST_SKIP_SLEEP_SEC))

    if reason.startswith("AddedNotConfirmed:"):
        return max(1, int(FAST_SKIP_SLEEP_SEC))

    return max(1, int(DELAY_BETWEEN_USERNAMES_SEC))


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

    sessions_all = load_sessions_from_json(SESSIONS_JSON)

    active_sessions: List[Tuple[SessionCfg, Client]] = []
    for s in sessions_all:
        kwargs = {
            "name": s.session_name,
            "api_id": s.api_id,
            "api_hash": s.api_hash,
            "no_updates": True,
            "workdir": str(DATA_DIR),
        }
        if s.session_string:
            kwargs["session_string"] = s.session_string

        c = Client(**kwargs)
        if safe_start(c, s.session_name):
            active_sessions.append((s, c))
        else:
            try:
                c.stop()
            except Exception:
                pass

    if not active_sessions:
        raise RuntimeError("Не удалось стартовать ни одну сессию. Проверьте sessions.json / session_string / API_ID/API_HASH.")

    ensure_log_header(LOG_CSV)

    state = load_state(STATE_JSON)
    state["session_idx"] = state["session_idx"] % len(active_sessions)
    save_state(STATE_JSON, state)

    try:
        while True:
            usernames_all = load_usernames(USERNAME_CSV)

            if not usernames_all:
                print(f"[{now_ts()}] В CSV нет usernames. Повторная проверка через 1 час.")
                time.sleep(3600)
                continue

            # если курсор уже в конце файла
            if state["cursor"] >= len(usernames_all):
                print(f"[{now_ts()}] Дошли до конца CSV (cursor={state['cursor']}, total={len(usernames_all)}). Жду 1 час.")
                time.sleep(3600)
                usernames_all = load_usernames(USERNAME_CSV)
                if state["cursor"] > len(usernames_all):
                    state["cursor"] = len(usernames_all)
                    save_state(STATE_JSON, state)
                continue

            day = today_prefix()
            daily_added = load_daily_added_count(LOG_CSV, day)
            if daily_added >= MAX_DAILY_ADDED:
                sleep_sec = seconds_until_next_midnight()
                print(f"[{now_ts()}] Daily limit reached: {daily_added}/{MAX_DAILY_ADDED}. Sleep {sleep_sec}s until next midnight ({APP_TZ}).")
                time.sleep(sleep_sec)
                continue

            session_idx = state["session_idx"] % len(active_sessions)
            s, c = active_sessions[session_idx]

            batch_end = min(state["cursor"] + BATCH_PER_SESSION, len(usernames_all))
            batch = usernames_all[state["cursor"]:batch_end]

            print(
                f"[{now_ts()}] === SESSION {session_idx + 1}/{len(active_sessions)}: {s.session_name}. "
                f"Batch={len(batch)}. DailyAdded={daily_added}/{MAX_DAILY_ADDED}. "
                f"Cursor={state['cursor']}/{len(usernames_all)} ==="
            )

            for username in batch:
                day = today_prefix()
                daily_added = load_daily_added_count(LOG_CSV, day)
                if daily_added >= MAX_DAILY_ADDED:
                    break

                print(f"[{now_ts()}] [{s.session_name}] Try add: @{username}")

                try:
                    row, extra_sleep = invite_once(c, s.session_name, GROUP, username)
                except Exception as e:
                    row = {
                        "timestamp": now_ts(),
                        "session": s.session_name,
                        "username": sanitize_username(username),
                        "status": "not_added",
                        "reason": f"OuterException:{type(e).__name__}:{e}",
                    }
                    extra_sleep = None

                append_log(LOG_CSV, row)
                print(f"[{now_ts()}] [{s.session_name}] Result: {row['status']} / {row['reason']}")

                # ВАЖНО: двигаем курсор всегда вперёд
                state["cursor"] += 1
                save_state(STATE_JSON, state)

                if row["status"].lower() == "added":
                    daily_added += 1

                if extra_sleep is not None:
                    print(f"[{now_ts()}] [{s.session_name}] Mandatory sleep {extra_sleep}s (Telegram limitation).")
                    time.sleep(max(1, int(extra_sleep)))
                    time.sleep(1)
                    continue

                base_sleep = compute_base_sleep(row)
                print(f"[{now_ts()}] [{s.session_name}] Sleep {base_sleep}s before next username.")
                time.sleep(base_sleep)

            # переключаем на следующую сессию
            state["session_idx"] = (state["session_idx"] + 1) % len(active_sessions)
            save_state(STATE_JSON, state)

            print(f"[{now_ts()}] Switching to next session in {DELAY_BETWEEN_SESSIONS_SEC}s...")
            time.sleep(max(1, int(DELAY_BETWEEN_SESSIONS_SEC)))

            if RECONNECT_BETWEEN_SESSIONS and len(active_sessions) > 1:
                next_idx = state["session_idx"] % len(active_sessions)
                ns, nc = active_sessions[next_idx]
                print(f"[{now_ts()}] Reconnect next session client: {ns.session_name}")
                ok = restart_client(nc, ns.session_name, sleep_sec=10)
                if not ok:
                    print(f"[{now_ts()}] [WARN] Reconnect failed for {ns.session_name}, continue...")

    except KeyboardInterrupt:
        print(f"[{now_ts()}] Stopping by user (Ctrl+C).")
    finally:
        save_state(STATE_JSON, state)
        for s, c in active_sessions:
            safe_stop(c, s.session_name)


if __name__ == "__main__":
    run()
