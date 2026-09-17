"""
Run once (locally or in Colab) to create a reusable session string.
Paste the output into the app's "Use a saved session string" tab.

    pip install telethon
    python make_session.py
"""
from telethon.sync import TelegramClient
from telethon.sessions import StringSession

api_id = int(input("API ID: "))
api_hash = input("API hash: ")
with TelegramClient(StringSession(), api_id, api_hash) as client:
    print("\nSession string (treat like a password):\n")
    print(client.session.save())
