import os
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from config import ensure_telegram_token
from telegram.ext import ApplicationBuilder, CommandHandler
from bot.command_handler import TelegramCommandHandler


def main() -> None:
    token = ensure_telegram_token()
    os.environ.setdefault("TELEGRAM_BOT_TOKEN", token)

    handler_ref: list = []

    async def post_init(application) -> None:
        if not handler_ref:
            return
        command_handler = handler_ref[0]
        import asyncio

        command_handler._main_loop = asyncio.get_running_loop()

    application = ApplicationBuilder().token(token).post_init(post_init).build()
    command_handler = TelegramCommandHandler(application)
    handler_ref.append(command_handler)

    application.add_handler(CommandHandler("start", command_handler.start))
    application.add_handler(CommandHandler("help", command_handler.help))
    application.add_handler(CommandHandler("hunt", command_handler.hunt))
    application.add_handler(CommandHandler("scrape", command_handler.scrape))
    application.add_handler(CommandHandler("status", command_handler.status))
    application.add_handler(CommandHandler("download", command_handler.download))

    print("🚀 HexaLeads Telegram Bot by HexaCyberLab is starting. Press Ctrl+C to stop.")
    application.run_polling()


if __name__ == "__main__":
    main()
