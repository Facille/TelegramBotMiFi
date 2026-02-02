import json
import logging
import re
from io import BytesIO
from datetime import datetime, timezone
from typing import Dict, Any, Tuple, List, Set, Optional

from openpyxl import Workbook
from telegram import Update, Document
from telegram.constants import ChatAction
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

from bs4 import BeautifulSoup 

BOT_TOKEN = "BOT_TOKEN"


logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("chat_export_bot")

MAX_FILES = 10
LIST_THRESHOLD = 50
TELEGRAM_MSG_LIMIT = 4096

MENTION_RE = re.compile(r"(?<!\w)@([A-Za-z0-9_]{5,32})")


def _safe_str(x: Any) -> str:
    if x is None:
        return ""
    return str(x).strip()


def _split_name(full_name: str) -> Tuple[str, str]:
    full_name = _safe_str(full_name)
    if not full_name:
        return "", ""
    parts = full_name.split()
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])


def _is_deleted_account(name: str, user_id: str) -> bool:
    n = _safe_str(name).lower()
    if "deleted account" in n:
        return True
    if "удал" in n and "аккаунт" in n:
        return True
    if n == "deleted":
        return True
    return False


def _make_user_key(user_id: str, username: str, first_name: str, last_name: str) -> str:
    if user_id:
        return f"id:{user_id}"
    if username:
        return f"u:{username.lower()}"
    return f"n:{first_name.lower()}|{last_name.lower()}"


def extract_mentions_from_text(text: str) -> Set[str]:
    return {m.group(1).lower() for m in MENTION_RE.finditer(text or "")}


def extract_from_json(data: Dict[str, Any]) -> Tuple[str, Dict[str, Dict[str, str]], Set[str]]:
    export_date = _safe_str(data.get("export_date")) or datetime.now(timezone.utc).isoformat()
    participants: Dict[str, Dict[str, str]] = {}
    mentions: Set[str] = set()

    messages = data.get("messages")
    if not isinstance(messages, list):
        return export_date, participants, mentions

    for m in messages:
        if not isinstance(m, dict):
            continue

        text_field = m.get("text")
        if isinstance(text_field, str):
            mentions |= extract_mentions_from_text(text_field)
        elif isinstance(text_field, list):
            joined = ""
            for item in text_field:
                if isinstance(item, str):
                    joined += item
                elif isinstance(item, dict):
                    joined += _safe_str(item.get("text"))
            mentions |= extract_mentions_from_text(joined)

        from_name = _safe_str(m.get("from") or m.get("actor") or m.get("sender") or "")
        from_id = _safe_str(m.get("from_id") or m.get("actor_id") or m.get("sender_id") or "")
        username = _safe_str(m.get("username") or m.get("from_username") or "")
        first_name = _safe_str(m.get("first_name"))
        last_name = _safe_str(m.get("last_name"))

        if not first_name and not last_name and from_name:
            fn, ln = _split_name(from_name)
            first_name = first_name or fn
            last_name = last_name or ln

        if not (from_id or username or first_name or last_name or from_name):
            continue

        if _is_deleted_account(from_name, from_id):
            continue

        key = _make_user_key(from_id, username, first_name, last_name)
        participants[key] = {
            "export_date": export_date,
            "username": username,
            "first_name": first_name,
            "last_name": last_name,
            "bio": "N/A",  
        }

    return export_date, participants, mentions


def extract_from_html(html_bytes: bytes) -> Tuple[str, Dict[str, Dict[str, str]], Set[str]]:
    text = html_bytes.decode("utf-8", errors="replace")
    soup = BeautifulSoup(text, "lxml")

    export_date = datetime.now(timezone.utc).isoformat()
    participants: Dict[str, Dict[str, str]] = {}
    mentions: Set[str] = set()

    for name_tag in soup.select(".from_name"):
        from_name = _safe_str(name_tag.get_text())
        if not from_name:
            continue
        if _is_deleted_account(from_name, ""):
            continue
        fn, ln = _split_name(from_name)
        key = _make_user_key("", "", fn, ln)
        participants[key] = {
            "export_date": export_date,
            "username": "",
            "first_name": fn,
            "last_name": ln,
            "bio": "N/A",
        }

    for text_tag in soup.select(".text"):
        msg_text = _safe_str(text_tag.get_text())
        mentions |= extract_mentions_from_text(msg_text)

    return export_date, participants, mentions


def build_excel_bytes(rows: List[Dict[str, str]]) -> bytes:
    wb = Workbook()
    ws = wb.active
    ws.title = "users"

    headers = ["export_date", "username", "first_name", "last_name", "bio"]
    ws.append(headers)

    for r in rows:
        ws.append([
            r.get("export_date", ""),
            r.get("username", ""),
            r.get("first_name", ""),
            r.get("last_name", ""),
            r.get("bio", "N/A"),
        ])

    bio = BytesIO()
    wb.save(bio)
    return bio.getvalue()


def chunk_text(text: str, limit: int = TELEGRAM_MSG_LIMIT) -> List[str]:
    chunks = []
    cur = ""
    for line in text.splitlines(True):
        if len(cur) + len(line) > limit - 50:
            chunks.append(cur)
            cur = ""
        cur += line
    if cur:
        chunks.append(cur)
    return chunks


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data["files"] = []
    await update.message.reply_text(
        "Привет! Я помогу получить список участников чата по истории сообщений.\n\n"
        f"Отправь файлы экспорта истории (JSON или HTML), не более {MAX_FILES} файлов.\n"
        "Когда закончишь — отправь /done\n\n"
        "Команды:\n"
        "/done — обработать присланные файлы\n"
        "/reset — сбросить список файлов"
    )


async def reset(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data["files"] = []
    await update.message.reply_text("Сбросил список файлов. Можешь присылать заново (до 10 файлов).")


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.message
    doc: Document = msg.document
    if not doc:
        return

    files: List[Dict[str, Any]] = context.user_data.get("files", [])
    if len(files) >= MAX_FILES:
        await msg.reply_text(f"Лимит {MAX_FILES} файлов достигнут. Отправь /done или /reset.")
        return

    filename = (doc.file_name or "upload").lower()
    if not (filename.endswith(".json") or filename.endswith(".html")):
        await msg.reply_text("Нужен файл экспорта .json или .html. Этот файл пропускаю.")
        return

    await msg.chat.send_action(action=ChatAction.TYPING)

    try:
        tg_file = await context.bot.get_file(doc.file_id)
        buf = BytesIO()
        await tg_file.download_to_memory(out=buf)
        raw = buf.getvalue()

        if filename.endswith(".json"):
            json.loads(raw.decode("utf-8", errors="replace"))

        files.append({"name": filename, "bytes": raw})
        context.user_data["files"] = files

        await msg.reply_text(f"✅ Принял: {filename} ({len(files)}/{MAX_FILES}). Пришли ещё или /done.")
    except Exception as e:
        logger.exception("Failed to read file")
        await msg.reply_text(f"❌ Не смог прочитать файл: {filename}\nОшибка: {e}")


async def done(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.message
    files: List[Dict[str, Any]] = context.user_data.get("files", [])

    if not files:
        await msg.reply_text("Файлы не получены. Пришли экспорт (JSON/HTML) и потом /done.")
        return

    await msg.chat.send_action(action=ChatAction.TYPING)

    all_participants: Dict[str, Dict[str, str]] = {}
    all_mentions: Set[str] = set()

    processed = 0
    failed = 0

    for f in files:
        name = f["name"]
        raw = f["bytes"]
        try:
            if name.endswith(".json"):
                data = json.loads(raw.decode("utf-8", errors="replace"))
                _, participants, mentions = extract_from_json(data)
            else:
                _, participants, mentions = extract_from_html(raw)

            all_participants.update(participants)
            all_mentions |= mentions
            processed += 1
        except Exception:
            failed += 1

    context.user_data["files"] = []

    total = len(all_participants)

    if total == 0:
        await msg.reply_text(
            f"Готово. Файлов обработано: {processed}, ошибок: {failed}.\n"
            "Не нашёл участников (возможно, формат экспорта отличается)."
        )
        return

    await msg.reply_text(
        f"Готово. Файлов: {processed}, ошибок: {failed}.\n"
        f"Уникальных участников (писали сообщения): {total}\n"
        f"Уникальных @упоминаний в тексте (не участники): {len(all_mentions)}"
    )

    rows = list(all_participants.values())
    rows.sort(key=lambda r: (
        r.get("username", "").lower(),
        r.get("first_name", "").lower(),
        r.get("last_name", "").lower()
    ))

    if total < LIST_THRESHOLD:
        with_username = []
        no_username = []

        for r in rows:
            u = _safe_str(r.get("username"))
            if u:
                with_username.append("@" + u)
            else:
                fn = _safe_str(r.get("first_name"))
                ln = _safe_str(r.get("last_name"))
                no_username.append(" ".join([p for p in [fn, ln] if p]) or "(без имени)")

        text_parts = []
        text_parts.append("Список участников (username):")
        if with_username:
            text_parts.extend([f"{i+1}. {u}" for i, u in enumerate(with_username)])
        else:
            text_parts.append("— (в экспорте не найдено ни одного username)")

        if no_username:
            text_parts.append("\nУчастники без username (по имени из экспорта):")
            text_parts.extend([f"- {n}" for n in no_username])

        out_text = "\n".join(text_parts)
        for chunk in chunk_text(out_text):
            await msg.reply_text(chunk)

    else:
        xlsx_bytes = build_excel_bytes(rows)
        out = BytesIO(xlsx_bytes)
        out.name = "users.xlsx"
        out.seek(0)

        await msg.reply_document(
            document=out,
            filename="users.xlsx",
            caption="Excel со списком участников (export_date, username, first_name, last_name, bio)."
        )


def main() -> None:
    if not BOT_TOKEN or BOT_TOKEN == "PASTE_YOUR_TOKEN_HERE":
        raise SystemExit("В bot.py нужно вставить BOT_TOKEN")

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("reset", reset))
    app.add_handler(CommandHandler("done", done))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))

    logger.info("Bot is starting...")
    print("=== BOT STARTED OK ===")
    app.run_polling()


if __name__ == "__main__":
    main()
