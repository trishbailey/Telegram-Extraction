# Telegram Channel Collector

A web app for collecting keyword-matched posts from public Telegram channels. Each user signs in with their own Telegram API credentials and account, enters target channels, keywords and a date range, and downloads the results as CSV files. The app also maps forwarding sources, channel mentions, linked domains and cross-channel reposting (cascades) within the collected posts.

Multiple users can run collections at the same time. Every browser session keeps its own Telegram connection, settings and results in memory. Nothing is written to the server's disk.

## Output

Each run produces one Excel file, `<prefix>_<date>.xlsx`, with one sheet and one row per post. Every column describes that post.

| Column | Contents |
|---|---|
| channel, date, text, link | Where and when the post appeared, its full text and a direct link |
| views, forwards, replies | Engagement counts at collection time |
| is_forward, forwarded_from, original_fwd_date | Whether the post was forwarded, from which account or channel, and when the original was posted |
| found_by | `keyword search`, `full scan` or `both` |
| search_term | The search keyword that surfaced the post |
| matched_keywords | Search keywords that appear word for word in the text |
| full_scan_keywords | Full-scan keywords that appear in the text |
| mentioned_channels | @handles and t.me links in the text |
| domains, urls | Linked websites, and the full links (one per line) |
| cascade_id | A shared number for posts with near-identical text in two or more channels |
| cascade_channel_count | How many channels carried that text |
| cascade_first_channel, cascade_first_date | Where and when that text appeared first |
| hours_after_first | Hours between the first appearance and this post |
| media_type, channel_title, message_id, edit_date, post_author, reply_to_msg_id, grouped_id | Post metadata. `grouped_id` links posts sent together as an album. |

The header row and the first three columns stay in place while scrolling, and every column has a filter. Filter `channel` to see one channel's posts, or filter `cascade_id` to see one piece of content across channels. For ranked counts, such as the most-mentioned channels or most-linked domains, use a pivot table or the result tabs in the app.

Media files are not downloaded. Excel cells hold up to 32,767 characters, so longer text is cut at that length.

## Files in this repository

| File | Purpose |
|---|---|
| `app.py` | The Streamlit app |
| `requirements.txt` | Python dependencies with pinned versions |
| `make_session.py` | Creates a reusable Telegram session string so users can skip the login code in the app |
| `.gitignore` | Keeps session files and secrets out of the repository |

## Deploying on Streamlit Community Cloud

1. Create a GitHub repository (public or private) and upload the four files above to its root folder.
2. Go to https://share.streamlit.io and sign in with GitHub. Grant access to the repository if prompted.
3. Click **Create app**, then choose the repository, the branch (usually `main`) and `app.py` as the main file.
4. Open **Advanced settings** and select Python 3.12. Leave Secrets empty. Users enter their own credentials in the app.
5. Click **Deploy**. The first build takes a few minutes.
6. Before sharing the link, sign in with your own account and complete one full collection.

Pushing a commit to the repository redeploys the app and disconnects every active user. Do not push changes while people are using it.

## Setup for each user

Complete these steps before the session. The Telegram developer site is unreliable, and account logins from a shared server can be throttled.

### 1. Get API credentials

1. Sign in at https://my.telegram.org with the phone number on your Telegram account. The login code arrives in the Telegram app.
2. Open **API development tools**.
3. Fill in an app title and short name (any values work), choose a platform, and submit.
4. Copy the **api_id** and **api_hash**.

If the form returns a generic "ERROR", turn off any VPN, wait a few minutes and try again from a different browser or network.

### 2. Create a session string (recommended)

A session string lets you connect to the app without requesting a login code on the day.

```
pip install telethon
python make_session.py
```

Enter your API ID, API hash, phone number, login code and two-step verification password if your account has one. The script prints a long string. Save it somewhere private. Anyone who holds it can access your Telegram account. To revoke it, open Telegram, go to **Settings > Devices** and end the session.

The script also runs in Google Colab: paste its contents into a cell, add `!pip install telethon` above it and run.

## Using the app

1. **Connect.** Enter your API ID and API hash. Then either paste your session string on the **Use a saved session string** tab, or use **Log in with phone** to receive a code in the Telegram app. The sidebar shows the connected account and offers a session string download for future use.
2. **Configure.**
   - **Target channels:** one per line. `@handle`, `handle` and `https://t.me/handle` all work.
   - **Search keywords:** one per line. The app uses Telegram's server-side search, which is fast.
   - **Full-scan keywords (optional):** the app reads every post in the date range and flags exact text matches. This is much slower on busy channels.
   - **Dates:** start and end dates are inclusive and use UTC.
   - **Max characters per message:** 0 keeps the full text.
   - **File name prefix:** added to every output file.
3. **Run.** Click **Start collection**. The progress bar and log update every two seconds. **Cancel collection** stops the run.
4. **Download.** When the run finishes, review the result tabs and click **Download results (.xlsx)**. Download promptly: results exist only in the browser session and disappear if the app restarts or you disconnect.
5. **Disconnect** from the sidebar when finished.

## Operating notes

**Search behavior.** Telegram's search matches words and some word variants, so a post can appear in results without an exact keyword match. The `search_term` column shows which term surfaced each post. The `matched_keywords` column lists exact text matches only and may be empty for these posts.

**Rate limits.** Telegram limits requests per account. The app waits out rate limits automatically and resumes where it stopped, which the log reports. Users on separate accounts do not slow each other down. Long date ranges with full-scan keywords produce the longest waits.

**Unavailable channels.** Private channels, channels the account cannot access, and mistyped handles are skipped and listed in a warning above the results.

**Capacity.** The app is designed for about 10 to 20 simultaneous users. On Community Cloud, memory is the constraint. If the app exceeds its allocation it restarts, and every user loses their connection and results.

**Idle connections.** Telegram connections left idle for two hours after a browser closes are disconnected automatically.

## Running locally

```
pip install -r requirements.txt
streamlit run app.py
```

The app opens at http://localhost:8501.
