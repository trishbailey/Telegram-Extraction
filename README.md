# Telegram Channel Collector

A web app for collecting posts from public Telegram channels with Boolean queries and mapping how those channels refer their audiences to one another. Each user signs in with their own Telegram account and API credentials, enters target channels, a query and a date range, and downloads the results as an Excel file. Each post row includes forwarding sources, Telegram mentions, social media accounts, linked websites and cross-channel reposting (cascades). A second page maps referrals in labeled diagrams: which collected channel sends its audience where, side by side, and who sends audiences to any single channel.

Multiple users can run collections at the same time. Every browser session keeps its own Telegram connection, settings and results in memory. Nothing is written to the server's disk.

## Output

Each run produces one Excel file, `<prefix>_<date>.xlsx`, with one sheet and one row per post. Every column describes that post.

| Column | Contents |
|---|---|
| channel, date, text, link | Where and when the post appeared, its full text and a direct link |
| views, forwards, replies | Engagement counts at collection time |
| is_forward, forwarded_from, original_fwd_date | Whether the post was forwarded, from which account or channel, and when the original was posted |
| query_matches | The query terms found in the post. Terms under NOT are not listed. |
| search_term | The term Telegram's search returned the post for (fast method only) |
| mentioned_channels | Other Telegram accounts the post names by @handle or t.me link. The posting channel's own handle is left out. Empty for forwards. |
| social_accounts | Social media accounts the post links to, such as `YouTube: @handle; X: @user`. Links that do not name an account appear as, for example, `YouTube: channel not identified`. Empty for forwards. |
| domains | Websites the post links to, excluding Telegram and social media platforms. Empty for forwards. |
| forwarded_content_refs | For forwards only: the Telegram accounts, social media accounts and websites inside the forwarded post. These belong to the original author, so they are kept apart from the forwarding channel's own referrals. |
| urls | Every link in the post, one per line |
| resolved_links | Links the app looked up, with the account each one led to, such as `https://youtu.be/abc → YouTube: @handle` |
| cascade_id | A shared number for posts with near-identical text in two or more channels |
| cascade_channel_count | How many channels carried that text |
| cascade_first_channel, cascade_first_date | Where and when that text appeared first |
| hours_after_first | Hours between the first appearance and this post |
| media_type, channel_title, message_id, edit_date, post_author, reply_to_msg_id, grouped_id | Post metadata. `grouped_id` links posts sent together as an album. |

The header row and the first three columns stay in place while scrolling, and every column has a filter. Filter `channel` to see one channel's posts, or filter `cascade_id` to see one piece of content across channels. For ranked counts, such as the most-mentioned channels or most-linked domains, use a pivot table or the result tabs in the app.

Media files are not downloaded. Excel cells hold up to 32,767 characters, so longer text is cut at that length.

## Search queries

The query box takes Boolean expressions. The same syntax works in the optional filter on the Referral map page, and the app shows an in-app guide.

| Syntax | Meaning | Example |
|---|---|---|
| `a b` or `a AND b` | Both terms | `aukus submarine` |
| `a OR b` | Either term | `aukus OR аукус` |
| `NOT a` or `-a` | Excludes a term | `aukus -"virginia class"` |
| `( )` | Groups terms | `(aukus OR аукус) AND (submarine OR подлодка)` |
| `"..."` | Exact phrase. Spacing and punctuation between the words can vary. | `"pillar ii"` |
| `*` | Any number of letters | `submarin*`, `*marine` |
| `?` | One letter | `organi?ation` |
| `a NEAR/n b` | Within n words of each other, in either order. `NEAR` alone means 10. | `aukus NEAR/5 cancel*` |
| `/.../` | Regular expression, ignoring case | `/ssn[- ]?aukus/` |
| `channel:` | The posting channel's handle or title | `channel:rybar` |
| `from:` | The forward source | `from:@dva_majors` |
| `mention:` | Telegram accounts named in the post | `mention:@geopolitics_prime` |
| `domain:` | Linked websites, including subdomains | `domain:ria.ru` |

Matching rules:

- Terms match whole words and ignore case. `aukus` matches "AUKUS" and "#AUKUS" but not "aukusfile". Use `aukus*` for longer forms.
- The letters ё and е are treated as the same letter.
- Operators must be capitalized: AND, OR, NOT, NEAR. The symbols `&`, `|` and `!` also work. Lowercase "and" and "or" are searched as words.
- Without parentheses, NOT applies first, then NEAR, then AND, then OR. `a OR b c` means `a OR (b AND c)`.
- NEAR works between terms, phrases and OR groups of terms in the post text.
- Field names must be one of the five above. To search for text that contains a colon, such as `Re:news`, put it in quotes.
- `mention:` ignores the posting channel's own handle.

As you type, the app shows how it read the query and flags mistakes such as an unclosed parenthesis. **Test the query on sample text** checks a pasted post against the query.

### Collection methods

| Method | How it works | Use it when |
|---|---|---|
| Fast | The app picks the fewest plain words that every match must contain, asks Telegram's search for posts with those words, then applies the full query. The app shows which words it will search for. | Most topic searches |
| Thorough | The app reads every post in the date range and applies the query to each one | Completeness matters, or the query has no required plain word |

Telegram's search matches loosely and does not support Boolean operators, wildcards or phrases, so the fast method can miss posts that the thorough method finds. How many depends on the query:

- Distinctive plain words, such as `aukus OR аукус`, usually come close to the thorough result.
- Wildcards miss the most, because Telegram searches only the fixed letters (`submarin*` is searched as `submarin`) and does not expand them.
- Inflected words and transliterations also cause misses, especially in Russian.

A query built only from NOT terms, field filters or regular expressions has nothing to search for and requires the thorough method.

**Measuring the difference.** **Compare fast and thorough on a sample** runs both methods on up to three channels over a short period, reports the share of thorough results the fast method found, and lists each missed post with the reason: a wildcard Telegram cannot expand, a word Telegram's search did not return, or a phrase, field or regular expression match.

**Time.** The thorough method reads about 3,000 to 5,000 posts a minute, because Telegram returns 100 posts per request and Telethon pauses about a second between requests. **Estimate collection size** checks each channel's post volume in the date range and shows the expected time for both methods. The app warns before thorough runs of more than 20,000 posts, and flags runs over 100,000 posts, which can take hours. Without an estimate, it warns when a thorough run covers more than three channels. During a thorough run, the log shows each channel's approximate post count.

### Social media accounts and link lookups

Links to social media platforms are recorded by account instead of by domain. Links to news and other websites are recorded by domain.

| Platform | Account read from the link | Needs a lookup |
|---|---|---|
| YouTube | `youtube.com/@handle`, `/channel/…`, `/c/…`, `/user/…` | `youtu.be/…`, `/watch`, `/shorts/…`, `/live/…` |
| X | `x.com/user/…`, `twitter.com/user/…` | `t.co/…` |
| VK | `vk.com/name`, `/wall-123_…` (community 123), `/id123` | `vk.cc/…` |
| TikTok | `tiktok.com/@user` | `vm.tiktok.com/…`, `vt.tiktok.com/…` |
| Rumble | `rumble.com/c/…`, `/user/…` | video pages |
| Telegraph | | every article (the author comes from Telegraph's public API) |
| Facebook | page names, `profile.php?id=`, `groups/…` | `fb.watch/…` |
| Instagram, Truth Social, Threads, Bluesky, Gab, Gettr, Substack, Reddit, Odysee, BitChute, Rutube, Odnoklassniki, Dzen, LinkedIn, Twitch | profile and channel links | short links where the platform has them |
| Any site | | general short links such as `bit.ly`, `tinyurl.com`, `ow.ly`, `clck.ru` |

**Identify the accounts behind video and short links** (on by default) looks these links up after collection, up to 3,000 per run. The app's server sends each request to YouTube or Rumble, Telegraph's API, or the link shortener. Link-shortening services log these requests. A short link that leads to a news site is recorded under that site's domain.

Some links cannot be identified without a logged-in account: Instagram and Facebook posts, and links whose lookup fails. These are grouped per platform as "account not identified". The Referral map page offers the same lookup for uploaded files, including files from earlier versions.

## Files in this repository

| File | Purpose |
|---|---|
| `app.py` | The Streamlit app |
| `requirements.txt` | Python dependencies with pinned versions |
| `make_session.py` | Creates a reusable Telegram session string so users can skip the login code in the app |
| `packages.txt` | System fonts used when saving diagrams as PNG (Community Cloud installs these automatically) |
| `.gitignore` | Keeps session files and secrets out of the repository |

## Deploying on Streamlit Community Cloud

1. Create a GitHub repository (public or private) and upload the files above to its root folder.
2. Go to https://share.streamlit.io and sign in with GitHub. Grant access to the repository if prompted.
3. Click **Create app**, then choose the repository, the branch (usually `main`) and `app.py` as the main file.
4. Open **Advanced settings** and select Python 3.12. Leave Secrets empty. Users enter their own credentials in the app.
5. Click **Deploy**. The first build takes a few minutes.
6. Before sharing the link, sign in with your own account and complete one full collection.

Pushing a commit to the repository redeploys the app and disconnects every active user. Do not push changes while people are using it.

## Setup for each user

Complete these steps before the session. The Telegram developer site is unreliable, and account logins from a shared server can be throttled.

### 1. Get an API ID and API hash

Every user needs their own. The app's connect screen has the same guide under **First time? Get your API ID and API hash**, with a button that opens the site.

You need a Telegram account first. If you do not have one, install Telegram on your phone and sign up.

1. Go to **https://my.telegram.org/auth?to=apps**.
2. Enter the phone number of your Telegram account with the country code, such as `+65 9123 4567`, and click **Next**.
3. Telegram sends a confirmation code as a message in your Telegram app, not by SMS. Enter it and click **Sign In**.
4. Click **API development tools**.
5. Fill in **Create new application**:
   - **App title:** any name, such as *OSINT course*
   - **Short name:** 5 to 32 letters and numbers with no spaces, such as *osintcourse2026*
   - **URL** and **Description:** leave empty
   - **Platform:** any option
6. Click **Create application**.
7. Copy **App api_id** (a number) and **App api_hash** (32 letters and numbers).

You create the application once. To see the values again, sign in to my.telegram.org and open **API development tools**. Keep the hash private; Telegram does not let you replace it.

The app checks the format as you paste. The ID must be digits only, and the hash exactly 32 characters of numbers and the letters a to f.

**Troubleshooting**

| Problem | What to try |
|---|---|
| "ERROR" after **Create application** | Use a short name of 5 to 32 letters and numbers only. Turn off VPNs, proxies, ad blockers and privacy extensions. Try another browser or mobile data. Wait 10 to 15 minutes between attempts. Recently created Telegram accounts are sometimes refused for a few days. |
| The code does not arrive | Look in the Telegram app for a message from *Telegram* with a blue check mark. The site does not send SMS. Check the country code. |
| The site does not load | Some networks block telegram.org. Try another network. |
| "Too many tries" | Telegram has paused sign-ins for the number, sometimes for several hours. Wait and try again. |

Because the form often fails on the first attempt, have students complete this step before the session.

### 2. Create a session string (recommended)

A session string lets you connect to the app without requesting a login code on the day.

```
pip install telethon
python make_session.py
```

Enter your API ID, API hash, phone number, login code and two-step verification password if your account has one. The script prints a long string. Save it somewhere private. Anyone who holds it can access your Telegram account. To revoke it, open Telegram, go to **Settings > Devices** and end the session.

The script also runs in Google Colab: paste its contents into a cell, add `!pip install telethon` above it and run.

## Using the app

1. **Connect.** Enter your API ID and API hash. First-time users can open **First time? Get your API ID and API hash** for the steps and a link to my.telegram.org. Then either paste your session string on the **Use a saved session string** tab, or use **Log in with phone** to receive a code in the Telegram app. The sidebar shows the connected account and offers a session string download for future use.
2. **Configure.**
   - **Target channels:** one per line. `@handle`, `handle` and `https://t.me/handle` all work.
   - **Search query:** a Boolean query. See [Search queries](#search-queries).
   - **Collection method:** fast or thorough. See [Collection methods](#collection-methods). Use **Estimate collection size** to see how long a run will take, and **Compare fast and thorough on a sample** to measure what fast misses for your query.
   - **Identify the accounts behind video and short links:** see [Social media accounts and link lookups](#social-media-accounts-and-link-lookups).
   - **Dates:** start and end dates are inclusive and use UTC.
   - **Max characters per message:** 0 keeps the full text.
   - **File name prefix:** added to every output file.
3. **Run.** Click **Start collection**. The progress bar and log update every two seconds. **Cancel** stops the run. Link lookups run after the posts are collected and have their own progress bar.
4. **Download.** When the run finishes, review the result tabs and click **Download results (.xlsx)**. Download promptly: results exist only in the browser session and disappear if the app restarts or you disconnect.
5. **Disconnect** from the sidebar when finished.

## Referral map

Open **Referral map** in the sidebar. The page uses the results from the current session, or an uploaded `.xlsx` or `.csv` file from the collector. Files from earlier versions work too, because the page reads referrals directly from the `text` and `forwarded_from` columns.

### What counts as a referral

A referral is a post that points its audience somewhere else.

| Type | Evidence | Direction | Color |
|---|---|---|---|
| Telegram mention | The channel's own post names another Telegram account by @handle or t.me link | Posting channel to the named account | Purple |
| Telegram forward | The channel reposts another account's post with Telegram's Forward button; readers can click through to the original | Forwarding channel to the original source | Orange |
| Social media link | The post links to a social media account | Posting channel to that account | YouTube red, X black, VK blue, all other platforms magenta |
| Website link | The post links to a website | Posting channel to the website's domain | Green |

A mention is the channel's own writing: a citation, recommendation, cross-promotion, advertisement or attack. A forward carries the original author's words unchanged, with provenance. A channel that copies another's text without forwarding or naming it creates neither; the cascade columns catch that.

**Forwards.** A forward counts only as a referral to its source. The accounts and links inside the forwarded post belong to the original author, so they are not credited to the forwarding channel. **Show references inside forwarded posts** adds them to the diagrams as faint dashed lines and to the counterparty tables as a `via_forwards` column. They never count toward the totals.

**Counting rules.**

- A post that references the same destination twice counts once.
- A channel referencing or forwarding itself is left out.
- Forward sources that appear only as a channel title are matched to a collected channel with that title where possible.
- Sources Telegram would not resolve appear as "Unresolved account" with their numeric ID.

### Controls

- **Query filter:** limits the map to posts matching a Boolean query, using the syntax in [Search queries](#search-queries). For example, `channel:rybar OR channel:dva_majors` maps only those two channels, and `aukus -"virginia class"` maps only posts on that topic.
- **Referral types and date range:** filter which referrals appear.
- **Line width and order:** number of posts, or the views of those posts.
- **Identify accounts:** appears when video or short links have not been looked up yet.

### Views

Both views use labeled cards joined by curved lines whose thickness shows volume. Every card shows its name, so the diagrams stay readable at any size.

**Compare channels.** The default when the data covers more than one channel. Every referral stays attributed to the channel that made it.

- **Left:** the channels being compared, each with its own color. A channel's card grows taller with the number of lines it sends, and cards are sorted from most to fewest referrals.
- **Right:** the destinations those channels refer their audience to, in three sections (Telegram channels, social media accounts, websites), each sorted from most to least referred.
- **Lines:** each line runs from one channel to one destination. Thickness shows volume on one scale for every channel, so widths compare directly.
- **Destination cards:** a colored dot and count for each channel that refers to the destination, such as ● 12 ● 5 ● 2. Hover over a card for the channel names, posts and views, or over a line for that channel's figures.
- **Controls:**
  - **Channels to compare:** up to 10 at a time, listed by how many referrals each makes. The six most active are selected at first.
  - **Only destinations shared by at least this many of these channels:** keeps destinations several of the selected channels refer to, which can point to common sourcing or coordination.
  - **Line color:** by source channel, or by referral type and platform (the platform colors used elsewhere). In referral-type mode, each channel sends one line per type.
  - **Show top** (see [Detail level](#detail-level)).
- **Referrals by channel table:** one row per destination and one column per channel with its number of posts (or views), plus the total, how many channels refer to the destination, and the referral types. **Include every collected channel in the table** adds a column for every collected channel, beyond the ten in the diagram. The table downloads as `.xlsx`.

**One channel.** Pick a channel, social media account or website. It sits in the center.

- **Left:** the channels that refer their audience to it.
- **Right:** where it refers its audience, in the same three sections.
- **Order:** within each section, the most frequent partner is at the top. Accounts that could not be identified follow the identified ones, and an "Other" card collects anything beyond the display limit.
- **Cards:** social media cards show a platform badge (YT, X, VK, TT and so on) and the platform name. Hover over a card or line for exact figures.
- **Totals and tables:** totals count distinct channels, identified social media accounts, websites and posts. Tables below the diagram list every counterparty with counts by type, views, first and last dates, and an example post link.

A website, a social media account or an uncollected channel shows incoming referrals only, and the diagram says why.

### Detail level

Every section of both views is sorted by prevalence: the channel, account or website with the most referrals is at the top, and the rest follow in descending order. The sort follows **Line width and order**, so it ranks by number of posts or by views. Accounts that could not be identified follow the identified ones.

**Show top** sets how many cards each section shows: 5, 10, 15, 20, 25, 30, 40, 50, 75, 100 or **All**. Everything beyond the limit is combined into an "Other" card at the bottom of its section, so the totals stay complete. Use a low setting to see only the heaviest sources and destinations, and **All** to see every account.

**Set each section separately** replaces the single control with one slider per section, each running from 0 to the number of accounts available. Set a section to 0 to hide it.

A line under the controls reports what is shown, for example "10 of 36 Telegram channels referred to; all 5 websites". Diagrams with more than 150 cards are long; the app notes this so you can lower **Show top** or scroll.

### Exports

- **Diagrams:** both views download as PNG or SVG. The SVG scales cleanly in PowerPoint and Word.
- **Referral table (.xlsx):** one row per channel pair, referral type and platform, with a column marking references inside forwarded posts.
- **Referrals by channel (.xlsx):** the comparison table from **Compare channels**.
- **Other tables:** the counterparty tables can be downloaded from the table toolbar (hover over a table).

### Coverage

The map covers only the collected posts.

- **Outgoing referrals** are complete for collected channels within the chosen query and dates.
- **Incoming referrals** count only references from other collected channels, so a channel's full incoming picture across Telegram is not visible here.
- **Uncollected accounts** show incoming referrals only.

## Operating notes

**Search behavior.** Every saved post matches the full query, whichever method found it. The `query_matches` column lists the query terms each post contains.

**Rate limits.** Telegram limits requests per account. The app waits out rate limits automatically and resumes where it stopped, which the log reports. Users on separate accounts do not slow each other down. The thorough method over long date ranges produces the longest waits.

**Unavailable channels.** Private channels, channels the account cannot access, and mistyped handles are skipped and listed in a warning above the results.

**Capacity.** The app is designed for about 10 to 20 simultaneous users. On Community Cloud, memory is the constraint. If the app exceeds its allocation it restarts, and every user loses their connection and results.

**Idle connections.** Telegram connections left idle for two hours after a browser closes are disconnected automatically.

## Running locally

```
pip install -r requirements.txt
streamlit run app.py
```

The app opens at http://localhost:8501.
