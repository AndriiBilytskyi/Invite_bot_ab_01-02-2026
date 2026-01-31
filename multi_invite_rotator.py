import csv
import os
import time
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Set, Dict, Tuple, Optional
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

# Требования:
BATCH_PER_SESSION = _env_int("BATCH_PER_SESSION", 5)
DELAY_BETWEEN_USERNAMES_SEC = _env_int("DELAY_BETWEEN_USERNAMES_SEC", 5 * 60)   # 5 минут
DELAY_BETWEEN_SESSIONS_SEC = _env_int("DELAY_BETWEEN_SESSIONS_SEC", 5 * 60)     # 5 минут
MAX_DAILY_ADDED = _env_int("MAX_DAILY_ADDED", 100)

# "мягкая смена сети": перезапускать клиент между сессиями
RECONNECT_BETWEEN_SESSIONS = _env_int("RECONNECT_BETWEEN_SESSIONS", 1) == 1

# Таймзона для дневных лимитов/полуночи (важно на Render, там обычно UTC)
APP_TZ = _env_str("APP_TZ", "Europe/Berlin")
TZ = ZoneInfo(APP_TZ)


# =========================
# DATA STRUCTURES
# =========================
from typing import Optional

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

    # Пробуем utf-8-sig (часто после Excel), если не получится — utf-8
    for enc in ("utf-8-sig", "utf-8"):
        try:
            with path.open("r", newline="", encoding=enc) as f:
                reader = csv.reader(f)
                rows = list(reader)
            break
        except UnicodeDecodeError:
            continue
    else:
        # последний шанс — latin-1, чтобы хотя бы прочитать
        with path.open("r", newline="", encoding="latin-1") as f:
            reader = csv.reader(f)
            rows = list(reader)

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
        data_rows = rows[1:]  # пропускаем заголовок

    usernames: List[str] = []
    for r in data_rows:
        if not r:
            continue
        if col_idx >= len(r):
            continue
        u = sanitize_username(r[col_idx])
        if u:
            usernames.append(u)

    # уникализируем, сохраняя порядок
    seen: Set[str] = set()
    out: List[str] = []
    for u in usernames:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def load_processed_success(log_path: Path) -> Set[str]:
    """
    Возвращает usernames, которые уже успешно обработаны:
      - added
      - already_in_group
    """
    processed: Set[str] = set()
    if not log_path.exists():
        return processed

    with log_path.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            status = (row.get("status") or "").strip().lower()
            username = sanitize_username(row.get("username") or "")
            if username and status in ("added", "already_in_group"):
                processed.add(username)
    return processed


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
def invite_once(
    client: Client,
    session_name: str,
    group: str,
    username: str,
) -> Tuple[Dict[str, str], Optional[int]]:
    """
    Возвращает:
      - row для лога (строго с ключами LOG_FIELDS)
      - sleep_sec: сколько нужно выждать (например FloodWait), иначе None
    """
    ts = now_ts()
    username_clean = sanitize_username(username)

    if not username_clean:
        return (
            {"timestamp": ts, "session": session_name, "username": username, "status": "skipped", "reason": "Empty username"},
            None,
        )

    try:
        client.add_chat_members(chat_id=group, user_ids=[username_clean])
        return (
            {"timestamp": ts, "session": session_name, "username": username_clean, "status": "added", "reason": "OK"},
            None,
        )

    except errors.UserAlreadyParticipant:
        return (
            {"timestamp": ts, "session": session_name, "username": username_clean, "status": "already_in_group", "reason": "UserAlreadyParticipant"},
            None,
        )

    except errors.UsernameInvalid:
        return (
            {"timestamp": ts, "session": session_name, "username": username_clean, "status": "not_added", "reason": "UsernameInvalid"},
            None,
        )

    except errors.UsernameNotOccupied:
        return (
            {"timestamp": ts, "session": session_name, "username": username_clean, "status": "not_added", "reason": "UsernameNotOccupied"},
            None,
        )

    except errors.UserPrivacyRestricted:
        return (
            {"timestamp": ts, "session": session_name, "username": username_clean, "status": "not_added", "reason": "UserPrivacyRestricted"},
            None,
        )

    except errors.UserNotMutualContact:
        return (
            {"timestamp": ts, "session": session_name, "username": username_clean, "status": "not_added", "reason": "UserNotMutualContact"},
            None,
        )

    except errors.ChatAdminRequired:
        return (
            {"timestamp": ts, "session": session_name, "username": username_clean, "status": "not_added", "reason": "ChatAdminRequired (no rights to add members)"},
            None,
        )

    except errors.PeerFlood:
        # Обычно означает, что Telegram подозревает спам. Лучше сделать длинную паузу/сменить аккаунт.
        return (
            {"timestamp": ts, "session": session_name, "username": username_clean, "status": "not_added", "reason": "PeerFlood"},
            max(3600, DELAY_BETWEEN_SESSIONS_SEC),  # минимум час
        )

    except errors.FloodWait as e:
        wait_sec = int(getattr(e, "value", 0) or 0)
        wait_sec = max(1, wait_sec)
        return (
            {"timestamp": ts, "session": session_name, "username": username_clean, "status": "not_added", "reason": f"FloodWait:{wait_sec}"},
            wait_sec + 3,
        )

    except Exception as e:
        return (
            {"timestamp": ts, "session": session_name, "username": username_clean, "status": "not_added", "reason": f"Exception:{type(e).__name__}:{e}"},
            None,
        )


# =========================
# MAIN LOOP
# =========================
def run() -> None:
    print(f"[{now_ts()}] Boot. TZ={APP_TZ}. DATA_DIR={DATA_DIR}")
    print(f"[{now_ts()}] GROUP={GROUP}")
    print(f"[{now_ts()}] CSV_PATH={USERNAME_CSV}")
    print(f"[{now_ts()}] LOG_CSV={LOG_CSV}")
    print(f"[{now_ts()}] SESSIONS_JSON={SESSIONS_JSON}")

    sessions = load_sessions_from_json(SESSIONS_JSON)

    clients: List[Client] = [
        Client(
            s.session_name,
            api_id=s.api_id,
            api_hash=s.api_hash,
            session_string=s.session_string,
            no_updates=True,
            workdir=str(DATA_DIR),
        )
        for s in sessions
    ]

    # Стартуем все клиенты
    for s, c in zip(sessions, clients):
        if not safe_start(c, s.session_name):
            raise RuntimeError(f"Не удалось стартовать сессию {s.session_name}. Проверьте session_string/api_id/api_hash.")

    ensure_log_header(LOG_CSV)

    session_idx = 0

    try:
        while True:
            day = today_prefix()
            daily_added = load_daily_added_count(LOG_CSV, day)
            if daily_added >= MAX_DAILY_ADDED:
                sleep_sec = seconds_until_next_midnight()
                print(f"[{now_ts()}] Daily limit reached: {daily_added}/{MAX_DAILY_ADDED}. Sleep {sleep_sec}s until next midnight ({APP_TZ}).")
                time.sleep(sleep_sec)
                continue

            usernames_all = load_usernames(USERNAME_CSV)
            processed = load_processed_success(LOG_CSV)
            queue = [u for u in usernames_all if u not in processed]

            if not queue:
                print(f"[{now_ts()}] Очередь пуста (все обработаны). Повторная проверка через 1 час.")
                time.sleep(3600)
                continue

            s = sessions[session_idx]
            c = clients[session_idx]
            batch = queue[:BATCH_PER_SESSION]

            print(f"[{now_ts()}] === SESSION {session_idx+1}/{len(sessions)}: {s.session_name}. Batch={len(batch)}. DailyAdded={daily_added}/{MAX_DAILY_ADDED} ===")

            for username in batch:
                # актуализируем daily limit перед каждым действием
                day = today_prefix()
                daily_added = load_daily_added_count(LOG_CSV, day)
                if daily_added >= MAX_DAILY_ADDED:
                    break

                print(f"[{now_ts()}] [{s.session_name}] Try add: @{username}")
                row, extra_sleep = invite_once(c, s.session_name, GROUP, username)
                append_log(LOG_CSV, row)

                print(f"[{now_ts()}] [{s.session_name}] Result: {row['status']} / {row['reason']}")

                if row["status"].lower() == "added":
                    daily_added += 1

                # Если Telegram сказал ждать — ждем столько
                if extra_sleep is not None:
                    print(f"[{now_ts()}] [{s.session_name}] Mandatory sleep {extra_sleep}s (Telegram limitation).")
                    time.sleep(extra_sleep)

                # Базовая пауза между usernames
                base_sleep = max(1, int(DELAY_BETWEEN_USERNAMES_SEC))
                print(f"[{now_ts()}] [{s.session_name}] Sleep {base_sleep}s before next username.")
                time.sleep(base_sleep)

            # Пауза и переключение на следующую сессию
            print(f"[{now_ts()}] Switching to next session in {DELAY_BETWEEN_SESSIONS_SEC}s...")
            time.sleep(max(1, int(DELAY_BETWEEN_SESSIONS_SEC)))

            # Перезапуск клиента между сессиями (если нужно)
            if RECONNECT_BETWEEN_SESSIONS:
                next_idx = (session_idx + 1) % len(sessions)
                ns = sessions[next_idx]
                nc = clients[next_idx]
                print(f"[{now_ts()}] Reconnect next session client: {ns.session_name}")
                restart_client(nc, ns.session_name, sleep_sec=10)

            session_idx = (session_idx + 1) % len(sessions)

    except KeyboardInterrupt:
        print(f"[{now_ts()}] Stopping by user (Ctrl+C).")
    finally:
        for s, c in zip(sessions, clients):
            safe_stop(c, s.session_name)


if __name__ == "__main__":
    run()
