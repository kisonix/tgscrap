#!/usr/bin/env python3    
"""    
TGScraper/main.py    
    
Reads channels from channels.txt, fetches each page (requests),    
parses HTML with BeautifulSoup using the html5lib parser,    
extracts message blocks from section.tgme_channel_history.js-message_history    
(or falls back to searching message wrappers globally),    
and writes a full JSON array to raws/<channel_name>.json (overwrite).    
    
This variant saves messages newest-first by:    
 - sorting by time_iso (descending) when available, otherwise    
 - reversing the scraped list.    
    
Usage:    
  python3 main.py    
  python3 main.py --channels channels.txt --outdir raws --delay 1.5 --verbose    
"""    
    
from __future__ import annotations    
import argparse    
import json    
import logging    
import os    
import re    
import time    
from typing import List, Dict, Any, Optional    
from urllib.parse import urlparse    
    
import requests    
from bs4 import BeautifulSoup    
    
# Config defaults    
DEFAULT_CHANNELS_FILE = "channels.txt"    
DEFAULT_OUT_DIR = "raws"    
DEFAULT_USER_AGENT = (    
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "    
    "(KHTML, like Gecko) Chrome/120 Safari/537.36"    
)    
DEFAULT_DELAY = 3.0  # seconds between requests    
    
    
# ------------------------    
# Utilities    
# ------------------------    
def sane_name_from_url(url: str) -> str:    
    """    
    Convert a channel URL into a safe filename base.    
    Examples:    
      https://t.me/s/dingyue_Center -> dingyue_center    
      https://t.me/NetifyVPN -> netifyvpn    
    """    
    parsed = urlparse(url.strip())    
    path = parsed.path.rstrip("/")    
    if path == "":    
        name = parsed.netloc or "channel"    
    else:    
        name = path.split("/")[-1]    
    name = name.lower()    
    name = re.sub(r"[^\w]+", "_", name)    
    name = re.sub(r"_{2,}", "_", name).strip("_")    
    if name == "":    
        name = "channel"    
    return name    
    
    
def create_session(user_agent: str = DEFAULT_USER_AGENT, timeout: int = 20) -> requests.Session:    
    s = requests.Session()    
    s.headers.update({"User-Agent": user_agent, "Accept-Language": "en-US,en;q=0.9"})    
    s.timeout = timeout    
    return s    
    
    
def parse_reactions(wrap) -> List[Dict[str, Optional[str]]]:    
    out = []    
    for el in wrap.select(".tgme_reaction"):    
        full = el.get_text(separator="", strip=True)    
        emoji_tag = el.select_one("i.emoji b")    
        emoji_char = emoji_tag.get_text(strip=True) if emoji_tag else None    
        count = None    
        if emoji_char:    
            count_text = full.replace(emoji_char, "").strip()    
            if count_text:    
                count = count_text    
        else:    
            m = re.search(r"(\d+)$", full)    
            if m:    
                count = m.group(1)    
        out.append({"emoji": emoji_char, "count": count, "raws": full})    
    return out    
    
    
def safe_text(el) -> str:    
    if el is None:    
        return ""    
    return el.get_text(separator=" ", strip=True)    
    
    
# ------------------------    
# Scraping logic    
# ------------------------    
def scrape_channel(session: requests.Session, url: str, verbose: bool = False) -> List[Dict[str, Any]]:    
    logging.info("Fetching %s", url)    
    try:    
        resp = session.get(url, timeout=getattr(session, "timeout", 20))    
        resp.raise_for_status()    
        html = resp.text    
    except Exception as e:    
        logging.error("Failed to fetch %s : %s", url, e)    
        return []    
    
    soup = BeautifulSoup(html, "html5lib")    
    
    section = soup.select_one("section.tgme_channel_history.js-message_history")    
    if section is None:    
        logging.debug("Parent section not found for %s; falling back to global search", url)    
        wrappers = soup.select("div.tgme_widget_message_wrap.js-widget_message_wrap")    
    else:    
        wrappers = section.select("div.tgme_widget_message_wrap.js-widget_message_wrap")    
    
    results: List[Dict[str, Any]] = []    
    for idx, wrap in enumerate(wrappers, start=1):    
        msg = wrap.select_one("div.tgme_widget_message")    
        post_id = msg.get("data-post") if msg and msg.has_attr("data-post") else None    
        data_view = msg.get("data-view") if msg and msg.has_attr("data-view") else None    
    
        author_name_el = wrap.select_one(".tgme_widget_message_owner_name")    
        author_name = safe_text(author_name_el)    
        author_href = None    
        ah = wrap.select_one(".tgme_widget_message_user a")    
        if ah and ah.has_attr("href"):    
            author_href = ah["href"]    
    
        photo_src = None    
        img = wrap.select_one(".tgme_widget_message_user img")    
        if img and img.has_attr("src"):    
            photo_src = img["src"]    
        else:    
            iel = wrap.select_one(".tgme_widget_message_user .tgme_widget_message_user_photo")    
            if iel and iel.has_attr("data-content"):    
                photo_src = None    
    
        text_block = wrap.select_one(".tgme_widget_message_text")    
        message_text = ""    
        message_html = ""    
        if text_block:    
            message_text = text_block.get_text(separator="\n", strip=True)    
            message_html = text_block.decode_contents()    
    
        views = None    
        views_el = wrap.select_one("span.tgme_widget_message_views")    
        if views_el:    
            vt = safe_text(views_el)    
            try:    
                views = int(re.sub(r"[^\d]", "", vt)) if vt else None    
            except Exception:    
                views = vt    
    
        time_iso = None    
        time_el = wrap.select_one("time")    
        if time_el and time_el.has_attr("datetime"):    
            time_iso = time_el["datetime"]    
    
        reactions = parse_reactions(wrap)    
    
        entry = {    
            "index": idx,    
            "post": post_id,    
            "data_view": data_view,    
            "author": {    
                "name": author_name,    
                "href": author_href,    
                "photo_src": photo_src,    
            },    
            "message_text": message_text,    
            "message_html": message_html,    
            "views": views,    
            "time_iso": time_iso,    
            "reactions": reactions,    
        }    
        results.append(entry)    
        if verbose:    
            logging.debug("Parsed message %d: post=%s author=%s views=%s", idx, post_id, author_name, views)    
    
    logging.info("Found %d message(s) on %s", len(results), url)    
    return results    
    
    
# ------------------------    
# Ordering helper (new)    
# ------------------------    
def order_results_newest_first(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:    
    """    
    Return a new list ordered newest-first.    
    Prefer sorting by 'time_iso' (ISO datetime string) descending when present.    
    Otherwise fallback to reversing the list.    
    Re-indexes the 'index' field starting at 1.    
    """    
    if not results:    
        return results    
    
    # check if any time_iso exists    
    have_time = any(bool(r.get("time_iso")) for r in results)    
    if have_time:    
        # stable sort by time_iso descending; None values go to the end    
        def _key(r):    
            t = r.get("time_iso")    
            return t or ""  # ISO strings sort lexicographically; empty string -> oldest    
        ordered = sorted(results, key=_key, reverse=True)    
        logging.debug("Ordered by time_iso descending (newest first).")    
    else:    
        ordered = list(reversed(results))    
        logging.debug("No time_iso present: simple reversed order applied.")    
    
    # reassign index    
    for i, r in enumerate(ordered, start=1):    
        r["index"] = i    
    return ordered    
    
    
# ------------------------    
# Main CLI    
# ------------------------    
def main():    
    p = argparse.ArgumentParser(description="TGScraper - scrape telegram channel pages and write raws JSON files.")    
    p.add_argument("--channels", "-c", default=DEFAULT_CHANNELS_FILE, help="Channels file (one URL per line)")    
    p.add_argument("--outdir", "-o", default=DEFAULT_OUT_DIR, help="Output directory for raws JSON files")    
    p.add_argument("--delay", "-d", type=float, default=DEFAULT_DELAY, help="Delay between requests (seconds)")    
    p.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")    
    args = p.parse_args()    
    
    logging.basicConfig(    
        level=logging.DEBUG if args.verbose else logging.INFO,    
        format="%(levelname)s: %(message)s",    
    )    
    
    if not os.path.exists(args.outdir):    
        os.makedirs(args.outdir, exist_ok=True)    
    
    if not os.path.exists(args.channels):    
        logging.error("Channels file not found: %s", args.channels)    
        return    
    
    with open(args.channels, "r", encoding="utf-8") as f:    
        lines = [ln.strip() for ln in f.readlines()]    
    channels = [ln for ln in lines if ln and not ln.startswith("#")]    
    
    if not channels:    
        logging.error("No channels found in %s", args.channels)    
        return    
    
    session = create_session()    
    
    for i, url in enumerate(channels, start=1):    
        try:    
            results = scrape_channel(session, url, verbose=args.verbose)    
        except Exception as e:    
            logging.exception("Unhandled error scraping %s", url)    
            results = []    
    
        # --- NEW: order newest-first before writing ---    
        ordered = order_results_newest_first(results)    
    
        fname = sane_name_from_url(url) + ".json"    
        path = os.path.join(args.outdir, fname)    
        try:    
            with open(path, "w", encoding="utf-8") as fw:    
                json.dump(ordered, fw, ensure_ascii=False, indent=2)    
            logging.info("Wrote %d entries to %s (newest-first)", len(ordered), path)    
        except Exception as e:    
            logging.exception("Failed to write %s", path)    
    
        if i != len(channels):    
            time.sleep(args.delay)    
    
    
if __name__ == "__main__":    
    main()
