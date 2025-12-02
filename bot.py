#!/usr/bin/env python3

import logging
import os
import io
import tempfile
import pandas as pd
import xml.etree.ElementTree as ET

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputFile,
)
from telegram.ext import (
    Updater,
    CommandHandler,
    MessageHandler,
    Filters,
    CallbackQueryHandler,
)


# ===========================
# CONFIG
# ===========================
TOKEN = "7977886967:AAEWIQ4cdMIQYL50tU61WJWK6b3efbKYvsg"   # <-- এখানে টোকেন বসান

STORAGE = {}

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


# ===========================
# 25MB SAFE TXT PARSER
# ===========================
def parse_txt_large(file_path):
    """
    25MB TXT file → DataFrame converter (optimized)
    Memory-safe reading with streaming.
    """

    # --- Step 1: Detect delimiter from first lines ---
    sample_lines = []
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        for _ in range(20):
            line = f.readline()
            if not line:
                break
            sample_lines.append(line.strip())

    sample = "\n".join(sample_lines)
    delimiters = [",", ";", "\t", "|"]

    # Try CSV / TSV style
    for sep in delimiters:
        if sep in sample:
            try:
                df = pd.read_csv(
                    file_path,
                    sep=sep,
                    engine="python",
                    on_bad_lines="skip",
                    low_memory=True
                )
                if df.shape[1] > 1:
                    return df
            except:
                pass

    # --- Step 2: key:value or key=value detection ---
    kv_data = {}
    kv_mode = True

    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        for ln in f:
            ln = ln.strip()
            if ":" in ln:
                k, v = ln.split(":", 1)
                kv_data[k.strip()] = v.strip()
            elif "=" in ln:
                k, v = ln.split("=", 1)
                kv_data[k.strip()] = v.strip()
            else:
                kv_mode = False
                break

    if kv_mode and len(kv_data) > 0:
        return pd.DataFrame([kv_data])

    # --- Step 3: fallback → single column text loader ---
    rows = []
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        for ln in f:
            ln = ln.strip()
            if ln:
                rows.append(ln)

    return pd.DataFrame({"text": rows})


# ===========================
# Convert DataFrame → XML
# ===========================
def df_to_xml(df):
    root = ET.Element("rows")

    for _, row in df.iterrows():
        item = ET.SubElement(root, "row")
        for col in df.columns:
            c = ET.SubElement(item, str(col))
            val = row[col]
            c.text = "" if pd.isna(val) else str(val)

    return ET.tostring(root, encoding="utf-8")


# ===========================
# Telegram Handlers
# ===========================
def start(update, context):
    update.message.reply_text("হ্যালো! আমাকে একটি TXT ফাইল পাঠান (সর্বোচ্চ 25MB)।")


def handle_file(update, context):
    doc = update.message.document

    if not doc.file_name.lower().endswith(".txt"):
        return update.message.reply_text("শুধুমাত্র .txt ফাইল সাপোর্ট করি।")

    if doc.file_size > 25 * 1024 * 1024:
        return update.message.reply_text("২৫MB এর বেশি TXT ফাইল সাপোর্ট করি না।")

    update.message.reply_text("ফাইল পড়া হচ্ছে... অপেক্ষা করুন...")

    file = context.bot.get_file(doc.file_id)

    fd, temp = tempfile.mkstemp(suffix=".txt")
    os.close(fd)
    file.download(temp)

    df = parse_txt_large(temp)
    os.remove(temp)

    STORAGE[update.effective_chat.id] = df

    buttons = [
        [
            InlineKeyboardButton("📘 XLSX", callback_data="xlsx"),
            InlineKeyboardButton("📄 CSV", callback_data="csv"),
        ],
        [
            InlineKeyboardButton("🟦 JSON", callback_data="json"),
            InlineKeyboardButton("🟧 XML", callback_data="xml"),
        ],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel")],
    ]

    update.message.reply_text(
        f"TXT লোড হয়েছে ✔️\nRows: {df.shape[0]} | Columns: {df.shape[1]}\n\nএকটি ফরম্যাট নির্বাচন করুন:",
        reply_markup=InlineKeyboardMarkup(buttons),
    )


def convert_format(update, context):
    query = update.callback_query
    query.answer()

    chat_id = query.message.chat.id

    if chat_id not in STORAGE:
        return query.edit_message_text("প্রথমে একটি TXT ফাইল পাঠান।")

    df = STORAGE[chat_id]
    bio = io.BytesIO()

    if query.data == "cancel":
        del STORAGE[chat_id]
        return query.edit_message_text("বাতিল করা হয়েছে।")

    if query.data == "csv":
        df.to_csv(bio, index=False)
        filename = "converted.csv"

    elif query.data == "xlsx":
        with pd.ExcelWriter(bio, engine="openpyxl") as writer:
            df.to_excel(writer, index=False)
        filename = "converted.xlsx"

    elif query.data == "json":
        txt = df.to_json(orient="records", force_ascii=False)
        bio.write(txt.encode("utf-8"))
        filename = "converted.json"

    elif query.data == "xml":
        xml_bytes = df_to_xml(df)
        bio.write(xml_bytes)
        filename = "converted.xml"

    bio.seek(0)

    query.message.reply_document(InputFile(bio, filename))
    query.edit_message_text("✔️ কনভার্ট সম্পন্ন!")

    del STORAGE[chat_id]


# ===========================
# MAIN
# ===========================
def main():
    updater = Updater(TOKEN, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(MessageHandler(Filters.document, handle_file))
    dp.add_handler(CallbackQueryHandler(convert_format))

    updater.start_polling()
    updater.idle()


if __name__ == "__main__":
    main()
