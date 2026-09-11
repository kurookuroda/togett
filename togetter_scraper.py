import argparse
import asyncio
import json
import random
import re
from datetime import datetime
from typing import List, Optional

import httpx
from bs4 import BeautifulSoup

# Configuration
BASE_URL = "https://togetter.com/"
PAGES_TO_SCRAPE = 5
DISCORD_MSG_LIMIT = 1990

# Semaphores for concurrency
fetch_sem = asyncio.Semaphore(4)
discord_sem = asyncio.Semaphore(2)
translate_sem = asyncio.Semaphore(2)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/114.0.0.0 Safari/537.36"
    )
}


async def fetch_html(client: httpx.AsyncClient, url: str, max_retries: int = 6) -> Optional[str]:
    """Fetch HTML with retries and concurrency limit."""
    for attempt in range(max_retries):
        try:
            async with fetch_sem:
                response = await client.get(url, headers=HEADERS, timeout=30.0)
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


def parse_page(html: str) -> List[str]:
    """Extract /li/ links from listing page."""
    soup = BeautifulSoup(html, "html.parser")
    urllist = []
    for link in soup.find_all("a", href=re.compile(r"/li/\d+")):
        href = link.get("href")
        if href:
            urllist.append(href)

    if not urllist:
        print("DEBUG: No /li/ links found. Sampling <a> tags:")
        for link in soup.find_all("a", href=True)[:10]:
            print(f"  {link['href'][:200]}")
        print("END DEBUG")

    result = []
    for href in set(urllist):
        if href.startswith("/"):
            result.append(f"{BASE_URL.rstrip('/')}{href}")
        elif href.startswith("http"):
            result.append(href)
        else:
            result.append(f"{BASE_URL}{href}")
    return result


def parse_page2(html: str):
    """Extract title and description from detail page."""
    soup = BeautifulSoup(html, "html.parser")
    title_tag = soup.find("h1", class_="entry_title")
    desc_tag = soup.find("div", class_="description_box")

    title = title_tag.get_text(strip=True) if title_tag else None
    description = desc_tag.get_text(strip=True) if desc_tag else ""
    return title, description


# Translation cache
_translation_cache = {}


async def translate_text(
    client: httpx.AsyncClient,
    text: str,
    target_lang: str,
    source_lang: str = "auto",
) -> str:
    """Translate text using Google Translate API."""
    if not text or not target_lang:
        return ""

    cache_key = (text, target_lang, source_lang)
    if cache_key in _translation_cache:
        return _translation_cache[cache_key]

    async with translate_sem:
        for attempt in range(5):
            try:
                response = await client.get(
                    "https://translate.googleapis.com/translate_a/single",
                    params={
                        "client": "gtx",
                        "sl": source_lang,
                        "tl": target_lang,
                        "dt": "t",
                        "q": text,
                    },
                    timeout=15.0,
                )
                if response.status_code == 429:
                    wait = (2 ** attempt) + random.random()
                    print(f"Translation 429 (attempt {attempt + 1}), waiting {wait:.1f}s...")
                    await asyncio.sleep(wait)
                    continue
                response.raise_for_status()
                data = response.json()
                translated = "".join(item[0] for item in data[0] if item and item[0])
                _translation_cache[cache_key] = translated
                await asyncio.sleep(0.5)
                return translated
            except httpx.HTTPStatusError as e:
                print(f"Translation HTTP {e.response.status_code} for '{text[:50]}...' (no retry)")
                return ""
            except Exception as e:
                print(f"Translation attempt {attempt + 1} failed for '{text[:50]}...': {e}")
                if attempt < 4:
                    await asyncio.sleep(2 ** attempt)
                else:
                    return ""
    return ""


def truncate_discord(msg: str, limit: int = DISCORD_MSG_LIMIT) -> str:
    """Truncate message to fit Discord limit."""
    if len(msg) <= limit:
        return msg
    return msg[:limit] + "\n…"


async def send_discord_message(
    client: httpx.AsyncClient,
    message: str,
    webhook_urls: List[str],
):
    """Send message to Discord webhooks with rate limit handling."""
    if not webhook_urls:
        print("Discord webhook URLs not set. Skipping Discord message.")
        return

    payload = {"content": message}
    for webhook_url in webhook_urls:
        delay = 1
        success = False
        for _ in range(5):
            try:
                async with discord_sem:
                    response = await client.post(webhook_url, json=payload)
                if response.status_code == 204:
                    print(f"Message sent to Discord successfully via {webhook_url}.")
                    success = True
                    break
                elif response.status_code == 429:
                    retry_after = float(response.headers.get("retry-after", delay))
                    print(f"Rate limited. Retrying in {retry_after} seconds...")
                    await asyncio.sleep(retry_after)
                    delay *= 2
                else:
                    print(f"Error sending message to Discord via {webhook_url}: {response.status_code}")
                    break
            except Exception as e:
                print(f"Error sending message to Discord via {webhook_url}: {e}")
                break
        if not success:
            print(f"Failed to send message to {webhook_url} after 5 retries.")


def pack_batches(messages: List[str], limit: int = DISCORD_MSG_LIMIT) -> List[str]:
    """Pack messages into batches respecting Discord character limit."""
    batches = []
    batch = []
    batch_len = 0
    for msg in messages:
        extra = len(msg) + (1 if batch else 0)
        if batch and batch_len + extra > limit:
            batches.append("\n".join(batch))
            batch = [msg]
            batch_len = len(msg)
        else:
            batch.append(msg)
            batch_len += extra
    if batch:
        batches.append("\n".join(batch))
    return batches


async def process_link(
    client: httpx.AsyncClient,
    link: str,
    translate: bool,
    target_lang: str,
    item_number: int,
):
    """Process a single Togetter link."""
    html = await fetch_html(client, link)
    if html is None:
        return None, f"Failed to fetch detail page: {link}\n", None

    title, description = parse_page2(html)
    if not title:
        return None, "", None

    await asyncio.sleep(5)

    if translate:
        translated_title = await translate_text(client, title, target_lang)
        translated_description = await translate_text(client, description, target_lang) if description else ""
    else:
        translated_title = ""
        translated_description = ""

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


async def process_category(client: httpx.AsyncClient, category: str) -> List[str]:
    """Process a category and return list of links."""
    urllist = []
    for x in range(PAGES_TO_SCRAPE):
        page = PAGES_TO_SCRAPE - x
        target_url = f"{BASE_URL}{category}?page={page}"
        html = await fetch_html(client, target_url)
        if html:
            urllist.extend(parse_page(html))
        else:
            print(f"Failed to fetch: {target_url}")
        await asyncio.sleep(1)
    return list(set(urllist))


def filter_records_by_keywords(records: List[dict], keywords: List[str]) -> List[dict]:
    """Filter records by keywords (case-insensitive OR match)."""
    if not keywords:
        return records
    filtered = []
    for rec in records:
        content = f"{rec['title']} {rec['description']}".lower()
        if any(kw.lower() in content for kw in keywords):
            filtered.append(rec)
    return filtered


def write_jsonl(records: List[dict], filepath: str):
    """Write records to JSONL file."""
    count = 0
    with open(filepath, "w", encoding="utf-8") as f:
        for rec in records:
            text_parts = [rec["title"]]
            if rec["description"]:
                text_parts.append(rec["description"])
            entry = {
                "text": "。".join(text_parts),
                "url": rec["link"],
            }
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
            count += 1
    print(f"JSONL saved to: {filepath} ({count} records)")


def generate_markdown(records: List[dict], generated_at: str) -> str:
    """Generate Markdown content from records."""
    lines = [
        "# Togetter Scraping Results\n",
        f"Generated: {generated_at}\n",
        f"Total items: {len(records)}\n",
        "---\n",
    ]
    for rec in records:
        lines.append(f"\n## {rec['item_number']}. {rec['title']}\n\n")
        lines.append(f"- **Link:** {rec['link']}\n")
        if rec["translated_title"]:
            lines.append(f"- **Translated Title:** {rec['translated_title']}\n")
        if rec["description"]:
            lines.append(f"- **Description:** {rec['description']}\n")
        if rec["translated_description"]:
            lines.append(f"- **Translated Description:** {rec['translated_description']}\n")
        lines.append("\n---\n")
    return "".join(lines)


async def main():
    parser = argparse.ArgumentParser(description="Scrape Togetter and optionally send to Discord.")
    parser.add_argument("--url", type=str, help="Scrape a single URL instead of a category.")
    parser.add_argument("-t", "--translate", action="store_true", help="Translate titles and descriptions.")
    parser.add_argument("-l", "--lang", type=str, default="en", help="Target language for translation.")
    parser.add_argument("-d", "--discord", action="store_true", help="Send results to Discord.")
    parser.add_argument("--webhook-urls", nargs="+", default=[], help="Discord webhook URLs to send messages to.")
    parser.add_argument("-o", "--output-md", type=str, help="Write results to a Markdown file (e.g. result.md).")
    parser.add_argument("--output-jsonl", type=str, help="Write results to a JSONL file for TTS (e.g. togetter.jsonl).")
    parser.add_argument("-k", "--keywords", nargs="+", default=[], help="Filter posts containing any of these keywords (case-insensitive).")
    parser.add_argument("--categories", nargs="+", default=["recentpopular"], help="Categories to scrape (e.g. hot recentpopular recent review).")
    args = parser.parse_args()

    async with httpx.AsyncClient(
        follow_redirects=True,
        timeout=httpx.Timeout(30.0),
        headers=HEADERS,
    ) as client:
        if args.url:
            print(f"Processing single URL: {args.url}")
            record, console, discord_msg = await process_link(
                client, args.url, args.translate, args.lang, 1
            )
            if console:
                print(console, end="")
            if args.discord and discord_msg:
                await send_discord_message(client, discord_msg, args.webhook_urls)

            if record:
                if args.output_md:
                    md = generate_markdown(
                        [record],
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    )
                    with open(args.output_md, "w", encoding="utf-8") as f:
                        f.write(md)
                    print(f"Markdown saved to: {args.output_md}")

                if args.output_jsonl:
                    filtered = filter_records_by_keywords([record], args.keywords)
                    if filtered:
                        write_jsonl(filtered, args.output_jsonl)
                    else:
                        print("Warning: Record did not match keywords. JSONL not written.")
            else:
                if args.output_md:
                    print("Warning: No record generated. Markdown file not written.")
                if args.output_jsonl:
                    print("Warning: No record generated. JSONL file not written.")
            return

        # Multi-category scraping
        tasks = [asyncio.create_task(process_category(client, cat)) for cat in args.categories]
        category_results = await asyncio.gather(*tasks, return_exceptions=True)

        all_links = []
        for result in category_results:
            if isinstance(result, list):
                all_links.extend(result)
            else:
                print(f"Category task error: {result}")

        unique_links = list(set(all_links))
        print(f"Total links found: {len(unique_links)}")

        records = []
        console_outputs = []
        discord_messages = []

        link_tasks = [
            asyncio.create_task(
                process_link(client, link, args.translate, args.lang, i + 1)
            )
            for i, link in enumerate(unique_links)
        ]
        link_results = await asyncio.gather(*link_tasks, return_exceptions=True)

        for result in link_results:
            if isinstance(result, tuple):
                record, console, discord_msg = result
                if record:
                    records.append(record)
                if console:
                    console_outputs.append(console)
                if discord_msg:
                    discord_messages.append(discord_msg)
            else:
                print(f"Link error: {result}")

        print(f"Total records collected: {len(records)}")

        for out in console_outputs:
            print(out, end="")

        if args.discord:
            batches = pack_batches(discord_messages)
            for batch in batches:
                await send_discord_message(client, batch, args.webhook_urls)

        filtered_records = filter_records_by_keywords(records, args.keywords)
        if args.keywords:
            print(f"Records after keyword filter: {len(filtered_records)}")

        if args.output_md:
            if filtered_records:
                md = generate_markdown(
                    filtered_records,
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                )
                with open(args.output_md, "w", encoding="utf-8") as f:
                    f.write(md)
                print(f"Markdown saved to: {args.output_md}")
            else:
                print("Warning: No records collected. Markdown file not written.")

        if args.output_jsonl:
            if filtered_records:
                write_jsonl(filtered_records, args.output_jsonl)
            else:
                print("Warning: No records collected. JSONL file not written.")


if __name__ == "__main__":
    asyncio.run(main())