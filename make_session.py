"""
Run once (locally or in Colab) to create a reusable session string.
Paste the output into the app's "Use a saved session string" tab.

You need an API ID and API hash first. Get them at
https://my.telegram.org/auth?to=apps (see the app's first-time guide or the README).

    pip install telethon
    python make_session.py
"""
from telethon.sync import TelegramClient
from telethon.sessions import StringSession

print("Get your API ID and API hash at https://my.telegram.org/auth?to=apps")
print("(API development tools > App api_id and App api_hash)\n")
api_id = int(input("API ID (a number): ").strip())
api_hash = input("API hash (32 letters and numbers): ").strip()
print("\nNext, enter your phone number with country code, such as +65 9123 4567.")
print("Telegram sends the login code as a message in your Telegram app.\n")
with TelegramClient(StringSession(), api_id, api_hash) as client:
    print("\nSession string (treat like a password):\n")
    print(client.session.save())
