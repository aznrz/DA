import os
import httpx
import logging
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, CommandHandler, filters, ContextTypes

# Загрузка переменных из файла .env
load_dotenv()

TOKEN = os.getenv("TELEGRAM_TOKEN")
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434/api/chat")
MODEL_NAME = os.getenv("MODEL_NAME", "mistral")

# Настройка логирования
logging.basicConfig(level=logging.INFO)

context_storage = {}

SYSTEM_PROMPT = {
    "role": "system", 
    "content": (
        "Ты — лаконичный помощник по Python и Excel. Отвечай строго на русском. "
        "Давай короткие ответы, минимум теории, только суть и код/формулы."
    )
}

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    context_storage[chat_id] = [SYSTEM_PROMPT]
    await update.message.reply_text("Привет! Я твой краткий ментор по Python и Excel. Спрашивай!")

async def clear_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    context_storage[chat_id] = [SYSTEM_PROMPT]
    await update.message.reply_text("🧼 Память очищена.")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_text = update.message.text

    if chat_id not in context_storage:
        context_storage[chat_id] = [SYSTEM_PROMPT]

    context_storage[chat_id].append({"role": "user", "content": user_text})

    # Ограничение памяти (System Prompt + 4 последних сообщения)
    if len(context_storage[chat_id]) > 5:
        context_storage[chat_id] = [SYSTEM_PROMPT] + context_storage[chat_id][-4:]

    await context.bot.send_chat_action(chat_id=chat_id, action="typing")

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                OLLAMA_URL,
                json={"model": MODEL_NAME, "messages": context_storage[chat_id], "stream": False},
                timeout=None
            )
            result = response.json()
            answer = result.get("message", {}).get("content", "⚠️ Пустой ответ")
            context_storage[chat_id].append({"role": "assistant", "content": answer})
    except Exception as e:
        answer = f"❌ Ошибка: {e}"

    await update.message.reply_text(answer)

if __name__ == "__main__":
    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("clear", clear_history))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    print(f"🚀 Бот запущен локально...")
    app.run_polling()
