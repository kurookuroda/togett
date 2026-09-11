use anyhow::Result;
use chrono::Local;
use clap::Parser;
use futures::future::join_all;
use regex::Regex;
use scraper::{Html, Selector};
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;
use tokio::sync::Semaphore;

const BASE_URL: &str = "https://togetter.com/";
const PAGES_TO_SCRAPE: usize = 5;
const DISCORD_MSG_LIMIT: usize = 1990;

static LI_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"/li/\d+").unwrap());
static A_SEL: LazyLock<Selector> = LazyLock::new(|| Selector::parse("a[href]").unwrap());

#[derive(Parser, Debug)]
#[command(name = "togetter_scraper")]
#[command(about = "Scrape Togetter and optionally send to Discord.")]
struct Args {
    #[arg(long, help = "Scrape a single URL instead of a category.")]
    url: Option<String>,

    #[arg(short, long, help = "Translate titles and descriptions.")]
    translate: bool,

    #[arg(short, long, default_value = "en", help = "Target language for translation.")]
    lang: String,

    #[arg(short, long, help = "Send results to Discord.")]
    discord: bool,

    #[arg(long, value_delimiter = ' ', num_args = 1.., help = "Discord webhook URLs to send messages to.")]
    webhook_urls: Vec<String>,

    #[arg(short = 'o', long = "output-md", help = "Write results to a Markdown file (e.g. result.md).")]
    output_md: Option<String>,

    #[arg(long, help = "Write results to a JSONL file for TTS (e.g. togetter.jsonl).")]
    output_jsonl: Option<String>,

    #[arg(long, value_delimiter = ' ', num_args = 1.., help = "Filter posts containing any of these keywords (case-insensitive).")]
    keywords: Vec<String>,

    #[arg(long, value_delimiter = ' ', num_args = 1.., default_value = "recentpopular", help = "Categories to scrape (e.g. hot recentpopular recent review).")]
    categories: Vec<String>,
}

#[derive(Debug, Clone)]
struct Record {
    item_number: usize,
    title: String,
    link: String,
    description: String,
    translated_title: String,
    translated_description: String,
}

#[derive(Clone)]
struct AppState {
    client: reqwest::Client,
    fetch_sem: Arc<Semaphore>,
    discord_sem: Arc<Semaphore>,
    translate_sem: Arc<Semaphore>,
    translation_cache: Arc<Mutex<HashMap<(String, String, String), String>>>,
}

fn trunc(s: &str, n: usize) -> String {
    s.chars().take(n).collect()
}

fn filter_records_by_keywords(records: &[Record], keywords: &[String]) -> Vec<Record> {
    if keywords.is_empty() {
        return records.to_vec();
    }
    records
        .iter()
        .filter(|rec| {
            let content = format!("{} {}", rec.title, rec.description).to_lowercase();
            keywords.iter().any(|kw| content.contains(&kw.to_lowercase()))
        })
        .cloned()
        .collect()
}

fn write_jsonl(records: &[Record], filepath: &str) -> Result<()> {
    let mut count = 0;
    let mut file = std::fs::File::create(filepath)?;
    for rec in records {
        let mut text_parts = vec![rec.title.clone()];
        if !rec.description.is_empty() {
            text_parts.push(rec.description.clone());
        }
        let entry = json!({
            "text": text_parts.join("。"),
            "url": rec.link,
        });
        writeln!(file, "{}", serde_json::to_string(&entry)?)?;
        count += 1;
    }
    println!("JSONL saved to: {} ({} records)", filepath, count);
    Ok(())
}

async fn fetch_html(state: &AppState, url: &str, max_retries: usize) -> Option<String> {
    for attempt in 0..max_retries {
        let outcome = {
            let _permit = state.fetch_sem.acquire().await.ok()?;
            match state.client.get(url).send().await {
                Ok(resp) => {
                    let status = resp.status();
                    resp.text().await.map(|body| (status, Some(body))).map_err(|e| e.to_string())
                }
                Err(e) => Err(e.to_string()),
            }
        };

        match outcome {
            Ok((status, Some(body))) if status.is_success() => {
                println!("{} succeed! {}", url, attempt + 1);
                return Some(body);
            }
            Ok((status, _)) => {
                eprintln!("Failed to fetch {} (Status: {})", url, status);
                if attempt < max_retries - 1 {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
            Err(e) => {
                eprintln!("Error fetching {} {}: {}", url, attempt + 1, e);
                if attempt < max_retries - 1 {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
        }
    }
    None
}

fn parse_page(html: &str) -> Vec<String> {
    let document = Html::parse_document(html);
    let mut urllist = Vec::new();

    for element in document.select(&A_SEL) {
        if let Some(href) = element.value().attr("href") {
            if LI_RE.is_match(href) {
                urllist.push(href.to_string());
            }
        }
    }

    if urllist.is_empty() {
        eprintln!("DEBUG: No /li/ links found. Sampling <a> tags:");
        for element in document.select(&A_SEL).take(10) {
            if let Some(href) = element.value().attr("href") {
                eprintln!("  {}", trunc(href, 200));
            }
        }
        eprintln!("END DEBUG");
    }

    let base = BASE_URL.trim_end_matches('/');
    let mut result = Vec::new();
    for href in urllist.into_iter().collect::<HashSet<_>>().into_iter() {
        if href.starts_with('/') {
            result.push(format!("{}{}", base, href));
        } else if href.starts_with("http") {
            result.push(href);
        } else {
            result.push(format!("{}{}", BASE_URL, href));
        }
    }
    result
}

fn parse_page2(html: &str) -> (Option<String>, String) {
    let document = Html::parse_document(html);
    let h1_sel = Selector::parse("h1.entry_title").unwrap();
    let desc_sel = Selector::parse("div.description_box").unwrap();

    let title = document
        .select(&h1_sel)
        .next()
        .map(|e| e.text().collect::<Vec<_>>().concat().trim().to_string());
    let description = document
        .select(&desc_sel)
        .next()
        .map(|e| e.text().collect::<Vec<_>>().concat().trim().to_string())
        .unwrap_or_default();

    (title, description)
}

async fn translate_text(
    state: &AppState,
    text: &str,
    target_lang: &str,
    source_lang: &str,
) -> Result<String> {
    if text.is_empty() || target_lang.is_empty() {
        return Ok("".to_string());
    }

    let cache_key = (text.to_string(), target_lang.to_string(), source_lang.to_string());
    {
        let cache = state.translation_cache.lock().unwrap();
        if let Some(v) = cache.get(&cache_key) {
            return Ok(v.clone());
        }
    }

    let q_text = urlencoding::encode(text);
    let gt_url = format!(
        "https://translate.googleapis.com/translate_a/single?client=gtx&sl={}&tl={}&dt=t&q={}",
        source_lang, target_lang, q_text
    );

    let _permit = state.translate_sem.acquire().await?;

    for attempt in 0..5 {
        let outcome = state.client.get(&gt_url).timeout(Duration::from_secs(15)).send().await;
        match outcome {
            Ok(resp) => {
                if resp.status() == 429 {
                    let wait = (2u64.pow(attempt) as f64) + rand::random::<f64>();
                    eprintln!("Translation 429 (attempt {}), waiting {:.1}s...", attempt + 1, wait);
                    tokio::time::sleep(Duration::from_secs_f64(wait)).await;
                    continue;
                }
                match resp.error_for_status() {
                    Ok(resp) => {
                        match resp.json::<serde_json::Value>().await {
                            Ok(data) => {
                                let mut translated = String::new();
                                if let Some(arr) = data.get(0).and_then(|v| v.as_array()) {
                                    for item in arr {
                                        if let Some(s) = item.get(0).and_then(|v| v.as_str()) {
                                            translated.push_str(s);
                                        }
                                    }
                                }
                                state.translation_cache.lock().unwrap().insert(cache_key.clone(), translated.clone());
                                tokio::time::sleep(Duration::from_secs_f64(0.5)).await;
                                return Ok(translated);
                            }
                            Err(e) => {
                                eprintln!("Translation JSON decode error for '{}...': {}", trunc(text, 50), e);
                                if attempt < 4 {
                                    tokio::time::sleep(Duration::from_secs(1 << attempt)).await;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Translation HTTP error for '{}...': {}", trunc(text, 50), e);
                        break;
                    }
                }
            }
            Err(e) => {
                eprintln!("Translation attempt {} failed for '{}...': {}", attempt + 1, trunc(text, 50), e);
                if attempt < 4 {
                    tokio::time::sleep(Duration::from_secs(1 << attempt)).await;
                } else {
                    return Ok("".to_string());
                }
            }
        }
    }
    Ok("".to_string())
}

fn truncate_discord(msg: &str, limit: usize) -> String {
    let count = msg.chars().count();
    if count <= limit {
        msg.to_string()
    } else {
        let trimmed: String = msg.chars().take(limit).collect();
        format!("{}\n…", trimmed)
    }
}

async fn send_discord_message(state: &AppState, message: &str, webhook_urls: &[String]) {
    if webhook_urls.is_empty() {
        println!("Discord webhook URLs not set. Skipping Discord message.");
        return;
    }

    let payload = json!({ "content": message });
    for webhook_url in webhook_urls {
        let mut delay = Duration::from_secs(1);
        let mut success = false;
        for _ in 0..5 {
            let outcome = {
                let _permit = state.discord_sem.acquire().await.unwrap();
                state.client.post(webhook_url).json(&payload).send().await
            };
            match outcome {
                Ok(resp) => {
                    if resp.status().is_success() {
                        println!("Message sent to Discord successfully via {}.", webhook_url);
                        success = true;
                        break;
                    } else if resp.status() == 429 {
                        let retry_after = resp
                            .headers()
                            .get("retry-after")
                            .and_then(|v| v.to_str().ok())
                            .and_then(|v| v.parse::<f64>().ok())
                            .unwrap_or(delay.as_secs_f64());
                        println!("Rate limited. Retrying in {} seconds...", retry_after);
                        tokio::time::sleep(Duration::from_secs_f64(retry_after)).await;
                        delay *= 2;
                    } else {
                        eprintln!("Error sending message to Discord via {}: {}", webhook_url, resp.status());
                        break;
                    }
                }
                Err(e) => {
                    eprintln!("Error sending message to Discord via {}: {}", webhook_url, e);
                    break;
                }
            }
        }
        if !success {
            println!("Failed to send message to {} after 5 retries.", webhook_url);
        }
    }
}

fn pack_batches(messages: &[String], limit: usize) -> Vec<String> {
    let mut batches = Vec::new();
    let mut batch = Vec::new();
    let mut batch_len = 0usize;

    for m in messages {
        let extra = m.chars().count() + if batch.is_empty() { 0 } else { 1 };
        if !batch.is_empty() && batch_len + extra > limit {
            batches.push(batch.join("\n"));
            batch = vec![m.clone()];
            batch_len = m.chars().count();
        } else {
            batch.push(m.clone());
            batch_len += extra;
        }
    }
    if !batch.is_empty() {
        batches.push(batch.join("\n"));
    }
    batches
}

async fn process_link(
    state: &AppState,
    link: &str,
    translate: bool,
    target_lang: &str,
    item_number: usize,
) -> (Option<Record>, String, Option<String>) {
    let html = match fetch_html(state, link, 6).await {
        Some(h) => h,
        None => {
            let err = format!("Failed to fetch detail page: {}\n", link);
            return (None, err, None);
        }
    };

    let (title_opt, description) = parse_page2(&html);
    let title = match title_opt {
        Some(t) => t,
        None => return (None, "".to_string(), None),
    };

    tokio::time::sleep(Duration::from_secs(5)).await;

    let (translated_title, translated_description) = if translate {
        let tt = translate_text(state, &title, target_lang, "auto").await.ok().unwrap_or_default();
        let td = if description.is_empty() {
            "".to_string()
        } else {
            translate_text(state, &description, target_lang, "auto").await.ok().unwrap_or_default()
        };
        (tt, td)
    } else {
        ("".to_string(), "".to_string())
    };

    let mut console_output = format!("**{}. {}**\n🔗 <{}>\n", item_number, title, link);
    if !translated_title.is_empty() {
        console_output.push_str(&format!("> {}\n\n", translated_title));
    } else {
        console_output.push_str("\n\n");
    }

    let mut discord_msg = format!("{}\n", title);
    if !translated_title.is_empty() {
        discord_msg.push_str(&format!("\n**TITLE**\n{}\n", translated_title));
    }
    if !description.is_empty() {
        discord_msg.push_str(&format!("**Description:** {}\n", description));
    }
    if !translated_description.is_empty() {
        discord_msg.push_str(&format!("**Translated Description:** {}\n", translated_description));
    }
    discord_msg.push_str(&format!(":link:\n<{}>\n----\n\n", link));
    let discord_msg = truncate_discord(&discord_msg, DISCORD_MSG_LIMIT);

    let record = Record {
        item_number,
        title,
        link: link.to_string(),
        description,
        translated_title,
        translated_description,
    };

    (Some(record), console_output, Some(discord_msg))
}

async fn process_category(state: &AppState, cat: &str) -> Vec<String> {
    let mut urllist = Vec::new();
    for x in 0..PAGES_TO_SCRAPE {
        let page = PAGES_TO_SCRAPE - x;
        let target_url = format!("{}{}?page={}", BASE_URL, cat, page);
        if let Some(html) = fetch_html(state, &target_url, 6).await {
            urllist.extend(parse_page(&html));
        } else {
            eprintln!("Failed to fetch: {}", target_url);
        }
        tokio::time::sleep(Duration::from_secs_f64(1.0)).await;
    }
    urllist.into_iter().collect::<HashSet<_>>().into_iter().collect()
}

fn generate_markdown(records: &[Record], generated_at: &str) -> String {
    let mut lines = vec![
        "# Togetter Scraping Results\n".to_string(),
        format!("Generated: {}\n", generated_at),
        format!("Total items: {}\n", records.len()),
        "---\n".to_string(),
    ];

    for rec in records {
        lines.push(format!("\n## {}. {}\n\n", rec.item_number, rec.title));
        lines.push(format!("- **Link:** {}\n", rec.link));
        if !rec.translated_title.is_empty() {
            lines.push(format!("- **Translated Title:** {}\n", rec.translated_title));
        }
        if !rec.description.is_empty() {
            lines.push(format!("- **Description:** {}\n", rec.description));
        }
        if !rec.translated_description.is_empty() {
            lines.push(format!("- **Translated Description:** {}\n", rec.translated_description));
        }
        lines.push("\n---\n".to_string());
    }

    lines.concat()
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let webhook_urls: Vec<String> = args.webhook_urls;
    let translate: bool = args.translate;
    let lang: String = args.lang;
    let keywords: Vec<String> = args.keywords;
    let categories: Vec<String> = args.categories;
    let url: Option<String> = args.url;
    let discord: bool = args.discord;
    let output_md: Option<String> = args.output_md;
    let output_jsonl: Option<String> = args.output_jsonl;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::limited(10))
        .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36")
        .build()?;

    let state = AppState {
        client,
        fetch_sem: Arc::new(Semaphore::new(4)),
        discord_sem: Arc::new(Semaphore::new(2)),
        translate_sem: Arc::new(Semaphore::new(2)),
        translation_cache: Arc::new(Mutex::new(HashMap::new())),
    };

    if let Some(url) = url {
        println!("Processing single URL: {}", url);
        let (record, console, discord_msg) = process_link(&state, &url, translate, &lang, 1).await;
        print!("{}", console);

        if discord {
            if let Some(msg) = discord_msg {
                send_discord_message(&state, &msg, &webhook_urls).await;
            }
        }

        // FIX: record.as_ref() で borrow。ムーブしない。
        if let Some(rec) = record.as_ref() {
            if let Some(path) = &output_md {
                let md = generate_markdown(
                    &[rec.clone()],
                    &Local::now().format("%Y-%m-%d %H:%M:%S").to_string(),
                );
                std::fs::write(path, md)?;
                println!("Markdown saved to: {}", path);
            }

            if let Some(path) = &output_jsonl {
                let filtered = filter_records_by_keywords(&[rec.clone()], &keywords);
                if filtered.is_empty() {
                    println!("Warning: Record did not match keywords. JSONL not written.");
                } else {
                    write_jsonl(&filtered, path)?;
                }
            }
        } else {
            if output_md.is_some() {
                println!("Warning: No record generated. Markdown file not written.");
            }
            if output_jsonl.is_some() {
                println!("Warning: No record generated. JSONL file not written.");
            }
        }
        return Ok(());
    } 
    
    let category_list: Vec<String> = if categories.is_empty() {
        vec!["recentpopular".to_string()]
    } else {
        categories
    };

    let cat_tasks: Vec<_> = category_list.into_iter().map(|cat| {
        let state = state.clone();
        tokio::spawn(async move { process_category(&state, &cat).await })
    }).collect();

    let mut all_links = Vec::new();
    for res in join_all(cat_tasks).await {
        match res {
            Ok(links) => all_links.extend(links),
            Err(e) => eprintln!("Category task error: {}", e),
        }
    }

    let unique_links: Vec<String> = all_links.into_iter().collect::<HashSet<_>>().into_iter().collect();
    println!("Total links found: {}", unique_links.len());

    let mut records = Vec::new();
    let mut console_outputs = Vec::new();
    let mut discord_messages = Vec::new();

    let mut tasks = Vec::new();
    for (i, link) in unique_links.iter().enumerate() {
        let state_clone = state.clone();
        let link = link.clone();
        let lang = lang.clone();
        let task = tokio::spawn(async move {
            process_link(&state_clone, &link, translate, &lang, i + 1).await
        });
        tasks.push(task);
    }

    for res in join_all(tasks).await {
        match res {
            Ok((record, console, discord_msg)) => {
                if let Some(r) = record {
                    records.push(r);
                }
                if !console.is_empty() {
                    console_outputs.push(console);
                }
                if let Some(m) = discord_msg {
                    discord_messages.push(m);
                }
            }
            Err(e) => eprintln!("Link error: {}", e),
        }
    }

    println!("Total records collected: {}", records.len());

    for out in console_outputs {
        print!("{}", out);
    }

    if discord {
        let batches = pack_batches(&discord_messages, DISCORD_MSG_LIMIT);
        for batch in batches {
            send_discord_message(&state, &batch, &webhook_urls).await;
        }
    }

    let filtered_records = if !keywords.is_empty() {
        let filtered = filter_records_by_keywords(&records, &keywords);
        println!("Records after keyword filter: {}", filtered.len());
        filtered
    } else {
        records
    };

    if let Some(path) = output_md {
        if !filtered_records.is_empty() {
            let md = generate_markdown(&filtered_records, &Local::now().format("%Y-%m-%d %H:%M:%S").to_string());
            std::fs::write(&path, md)?;
            println!("Markdown saved to: {}", path);
        } else {
            println!("Warning: No records collected. Markdown file not written.");
        }
    }

    if let Some(path) = output_jsonl {
        if !filtered_records.is_empty() {
            write_jsonl(&filtered_records, &path)?;
        } else {
            println!("Warning: No records collected. JSONL file not written.");
        }
    }

    Ok(())
}