import httpx
import asyncio
from bs4 import BeautifulSoup
import re
import json
import argparse
from datetime import datetime
import sys
import random

BASE_URL = 'https://togetter.com/'
PAGES_TO_SCRAPE = 5

DISCORD_WEBHOOK_URLS = []

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
}

DISCORD_MSG_LIMIT = 1990

fetch_sem = asyncio.Semaphore(4)
discord_sem = asyncio.Semaphore(2)
translate_sem = asyncio.Semaphore(2)

_translation_cache = {}


# ==================== JSONL / TTS 連携用 ====================

def filter_records_by_keywords(records, keywords):
    """キーワードフィルタ（大文字小文字区別なし）。返すレコードは通常の辞書"""
    if not keywords:
        return records
    filtered = []
    for rec in records:
        content = f"{rec['title']} {rec['description']}"
        if any(kw.lower() in content.lower() for kw in keywords):
            filtered.append(rec)
    return filtered


def write_jsonl(records, filepath):
    """TTS スクリプトが期待する最小形式で JSONL を書き出す"""
    count = 0
    with open(filepath, "w", encoding="utf-8") as f:
        for rec in records:
            text_parts = [rec['title']]
            if rec.get('description'):
                text_parts.append(rec['description'])
            entry = {
                "text": "。".join(text_parts),
                "url": rec['link']
            }
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
            count += 1
    print(f"JSONL saved to: {filepath} ({count} records)")


# ==================== 既存の関数 ====================

async def fetch_html(client, url, max_retries=6):
    """セマフォはリクエスト送信のみ保持。sleep/判定は解放後。"""
    for attempt in range(max_retries):
        try:
            async with fetch_sem:
                response = await client.get(url, headers=HEADERS, timeout=30.0)

            # セマフォ解放後
            if response.status_code == 200:
                print(f"{url} succeed! {attempt + 1}")
                return response.text
            print(f"Failed to fetch {url} (Status: {response.status_code})")
            if attempt < max_retries - 1:
                await asyncio.sleep(2)
            else:
                return None
        except Exception as e:
            print(f"Error fetching {url} {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(2)
            else:
                return None
    return None


def parse_page(html):
    soup = BeautifulSoup(html, 'html.parser')
    urllist = []

    for atag in soup.find_all('a', href=True):
        href = atag['href']
        if re.search(r'/li/\d+', href):
            urllist.append(href)

    if not urllist:
        print("DEBUG: No /li/ links found. Sampling <a> tags:")
        for atag in list(soup.find_all('a'))[:10]:
            print(" ", str(atag)[:200])
        print("END DEBUG")

    result = []
    for href in set(urllist):
        if href.startswith('/'):
            result.append(BASE_URL.rstrip('/') + href)
        elif href.startswith('http'):
            result.append(href)
        else:
            result.append(BASE_URL + href)
    return result


def parse_page2(html):
    soup = BeautifulSoup(html, 'html.parser')
    h1 = soup.find('h1', class_="entry_title")
    if not h1:
        return None, ""
    title = h1.get_text(strip=True)
    desc_box = soup.find('div', class_="description_box")
    description = desc_box.get_text(strip=True) if desc_box else ""
    return title, description


async def translate_text(client, text, target_lang, source_lang="auto", max_retries=5):
    if not text or not target_lang:
        return ""

    cache_key = (text, target_lang, source_lang)
    if cache_key in _translation_cache:
        return _translation_cache[cache_key]

    from urllib.parse import quote
    q_text = quote(text, safe='')
    gt_url = f"https://translate.googleapis.com/translate_a/single?client=gtx&sl={source_lang}&tl={target_lang}&dt=t&q={q_text}"

    async with translate_sem:
        for attempt in range(max_retries):
            try:
                response = await client.get(gt_url, timeout=15.0)
                response.raise_for_status()
                data = response.json()
                translated = "".join(item[0] for item in data[0] if item)
                _translation_cache[cache_key] = translated
                await asyncio.sleep(0.5)
                return translated

            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    wait = (2 ** attempt) + random.uniform(0, 1)
                    print(f"Translation 429 (attempt {attempt + 1}), waiting {wait:.1f}s...")
                    await asyncio.sleep(wait)
                else:
                    print(f"Translation HTTP error {e.response.status_code} for '{text[:50]}...': {e}")
                    break
            except (httpx.RequestError, json.JSONDecodeError) as e:
                print(f"Translation attempt {attempt + 1} failed for '{text[:50]}...': {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                else:
                    print(f"Max retries reached for translation of '{text[:50]}...'")
                    return ""
            except Exception as e:
                print(f"Unexpected error during translation for '{text[:50]}...': {e}")
                return ""

    return ""


def truncate_discord(msg, limit=DISCORD_MSG_LIMIT):
    if len(msg) <= limit:
        return msg
    return msg[:limit] + "\n…"


async def send_discord_message(client, message, webhook_urls):
    if not webhook_urls:
        print("Discord webhook URLs not set. Skipping Discord message.")
        return

    payload = {"content": message}
    for webhook_url in webhook_urls:
        retries = 5
        delay = 1.0
        success = False
        for _ in range(retries):
            try:
                async with discord_sem:
                    response = await client.post(webhook_url, json=payload, timeout=30.0)
                response.raise_for_status()
                print(f"Message sent to Discord successfully via {webhook_url}.")
                success = True
                break
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    retry_after = float(e.response.headers.get("Retry-After", delay))
                    print(f"Rate limited. Retrying in {retry_after} seconds...")
                    await asyncio.sleep(retry_after)
                    delay *= 2
                else:
                    print(f"Error sending message to Discord via {webhook_url}: {e}")
                    break
            except httpx.RequestError as e:
                print(f"Error sending message to Discord via {webhook_url}: {e}")
                break
        if not success:
            print(f"Failed to send message to {webhook_url} after {retries} retries.")


def pack_batches(messages, limit=DISCORD_MSG_LIMIT):
    batches = []
    batch = []
    batch_len = 0
    for m in messages:
        extra = len(m) + (1 if batch else 0)
        if batch and batch_len + extra > limit:
            batches.append("\n".join(batch))
            batch = [m]
            batch_len = len(m)
        else:
            batch.append(m)
            batch_len += extra
    if batch:
        batches.append("\n".join(batch))
    return batches


async def process_link(client, link, translate, target_lang, item_number):
    html = await fetch_html(client, link)
    if not html:
        print(f"Failed to fetch detail page: {link}")
        return None, f"Failed to fetch detail page: {link}\n", None

    title, description = parse_page2(html)
    if not title:
        return None, "", None

    await asyncio.sleep(5)

    translated_title = ""
    translated_description = ""
    if translate:
        translated_title = await translate_text(client, title, target_lang)
        if description:
            translated_description = await translate_text(client, description, target_lang)

    console_output = f"**{item_number}. {title}**\n🔗 <{link}>\n"
    if translated_title:
        console_output += f"> {translated_title}\n\n"
    else:
        console_output += "\n\n"

    discord_msg = f"{title}\n"
    if translated_title:
        discord_msg += f"\n**TITLE**\n{translated_title}\n"
    if description:
        discord_msg += f"**Description:** {description}\n"
    if translated_description:
        discord_msg += f"**Translated Description:** {translated_description}\n"
    discord_msg += f":link:\n<{link}>\n----\n\n"
    discord_msg = truncate_discord(discord_msg)

    record = {
        "item_number": item_number,
        "title": title,
        "link": link,
        "description": description,
        "translated_title": translated_title,
        "translated_description": translated_description,
    }

    return record, console_output, discord_msg


async def process_category(client, cat):
    urllist = []
    for x in range(PAGES_TO_SCRAPE):
        page = PAGES_TO_SCRAPE - x
        target_url = f"{BASE_URL}{cat}?page={page}"
        html = await fetch_html(client, target_url)
        if html:
            urllist.extend(parse_page(html))
        else:
            print(f"Failed to fetch: {target_url}")
        await asyncio.sleep(1.0)
    return list(set(urllist))


def generate_markdown(records, generated_at):
    lines = [
        "# Togetter Scraping Results\n",
        f"Generated: {generated_at}\n",
        f"Total items: {len(records)}\n",
        "---\n",
    ]

    for rec in records:
        lines.append(f"\n## {rec['item_number']}. {rec['title']}\n\n")
        lines.append(f"- **Link:** {rec['link']}\n")
        if rec['translated_title']:
            lines.append(f"- **Translated Title:** {rec['translated_title']}\n")
        if rec['description']:
            lines.append(f"- **Description:** {rec['description']}\n")
        if rec['translated_description']:
            lines.append(f"- **Translated Description:** {rec['translated_description']}\n")
        lines.append("\n---\n")

    return "".join(lines)


async def main(args):
    webhook_urls = args.webhook_urls if args.webhook_urls else DISCORD_WEBHOOK_URLS

    async with httpx.AsyncClient(
        timeout=30.0,
        headers=HEADERS,
        follow_redirects=True,
    ) as client:
        if args.url:
            print(f"Processing single URL: {args.url}")
            record, console, discord_msg = await process_link(
                client, args.url, args.translate, args.lang, 1
            )
            print(console, end="")

            if args.discord and discord_msg:
                await send_discord_message(client, discord_msg, webhook_urls)

            if args.output_md:
                if record:
                    md = generate_markdown([record], datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                    with open(args.output_md, "w", encoding="utf-8") as f:
                        f.write(md)
                    print(f"Markdown saved to: {args.output_md}")
                else:
                    print("Warning: No record generated. Markdown file not written.")

            if args.output_jsonl:
                if record:
                    filtered = filter_records_by_keywords([record], args.keywords)
                    if filtered or not args.keywords:
                        write_jsonl(filtered if args.keywords else [record], args.output_jsonl)
                    else:
                        print("Warning: Record did not match keywords. JSONL not written.")
                else:
                    print("Warning: No record generated. JSONL file not written.")
            return

        tasks = [
            asyncio.create_task(process_category(client, cat))
            for cat in args.categories
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_links = []
        for res in results:
            if isinstance(res, Exception):
                print(f"Category error: {res}")
                continue
            all_links.extend(res)

        all_links = list(set(all_links))
        print(f"Total links found: {len(all_links)}")

        link_tasks = [
            asyncio.create_task(
                process_link(client, link, args.translate, args.lang, i + 1)
            )
            for i, link in enumerate(all_links) if link
        ]
        link_results = await asyncio.gather(*link_tasks, return_exceptions=True)

        records = []
        console_outputs = []
        discord_messages = []

        for res in link_results:
            if isinstance(res, Exception):
                print(f"Link error: {res}")
                continue
            record, console_str, discord_str = res
            if record:
                records.append(record)
            if console_str:
                console_outputs.append(console_str)
            if discord_str:
                discord_messages.append(discord_str)

        print(f"Total records collected: {len(records)}")

        for out in console_outputs:
            print(out, end="")

        if args.discord:
            batches = pack_batches(discord_messages)
            for batch in batches:
                await send_discord_message(client, batch, webhook_urls)

        if args.keywords:
            records = filter_records_by_keywords(records, args.keywords)
            print(f"Records after keyword filter: {len(records)}")

        if args.output_md:
            if records:
                md = generate_markdown(records, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                with open(args.output_md, "w", encoding="utf-8") as f:
                    f.write(md)
                print(f"Markdown saved to: {args.output_md}")
            else:
                print("Warning: No records collected. Markdown file not written.")

        if args.output_jsonl:
            if records:
                write_jsonl(records, args.output_jsonl)
            else:
                print("Warning: No records collected. JSONL file not written.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape Togetter and optionally send to Discord.")
    parser.add_argument("--url", help="Scrape a single URL instead of a category.")
    parser.add_argument("-t", "--translate", action="store_true", help="Translate titles and descriptions.")
    parser.add_argument("-l", "--lang", default="en", help="Target language for translation.")
    parser.add_argument("-d", "--discord", action="store_true", help="Send results to Discord.")
    parser.add_argument("--webhook-urls", nargs="+", help="Discord webhook URLs to send messages to.")
    parser.add_argument("-o", "--output-md", metavar="FILE", help="Write results to a Markdown file (e.g. result.md).")
    parser.add_argument("--output-jsonl", metavar="FILE", help="Write results to a JSONL file for TTS (e.g. togetter.jsonl).")
    parser.add_argument("--keywords", nargs="+", help="Filter posts containing any of these keywords (case-insensitive).")
    parser.add_argument("--categories", nargs="+", default=["recentpopular"], help="Categories to scrape (e.g. hot recentpopular recent review).")

    # Jupyter 等で -f が渡される場合の対処（必要に応じて）
    argv = [arg for arg in sys.argv[1:] if not arg.startswith("-f")]
    args = parser.parse_args(argv)

    asyncio.run(main(args))
