#!/usr/bin/env python3
"""
TGScraper/main.py

Scrape channels listed in channels.txt, detect v2ray-style URIs inside
message_text while parsing, and save the detected messages directly to
raws/<channel_basename>.json (overwrite). No companion _config files.

Each saved item keeps useful metadata for traceability and later polishing:
{
  "index": 1,
  "post": "dingyue_Center/3701",
  "time_iso": "2025-10-14T14:36:01+00:00",
  "author": {"name": "...", "href": "...", "photo_src": "..."},
  "source": "https://t.me/...",
  "message_text": "...",
  "message_html": "...",
  "views": 567,
  "reactions": [...]
}

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
# Config-detection heuristics (merged)
# ------------------------
PRIMARY_SCHEMES = [
    "vmess",
    "ss",
    "socks",
    "vless",
    "trojan",
    "wireguard",
    "hysteria2",
]

PRIMARY_RX = re.compile(r"(?i)\b(" + "|".join(re.escape(s) for s in PRIMARY_SCHEMES) + r")://")
HTTP_RX = re.compile(r"(?i)\bhttp://[^\s'\"<>]+")
BASE64_LONG_RX = re.compile(r"[A-Za-z0-9+/]{40,}={0,2}")
HTTP_CONTAINS_SCHEME_RX = re.compile(r"(?i)(vmess|vless|trojan|ss|socks|hysteria|wireguard)")


def detect_config_in_text(text: str) -> bool:
    """
    Conservative detection of v2ray-style configs inside message text.
    """
    if not text:
        return False

    if PRIMARY_RX.search(text):
        return True

    for m in HTTP_RX.finditer(text):
        url = m.group(0)
        if HTTP_CONTAINS_SCHEME_RX.search(url):
            return True
        if BASE64_LONG_RX.search(url):
            return True
        span_start, span_end = max(m.start() - 50, 0), min(m.end() + 50, len(text))
        context = text[span_start:span_end]
        if HTTP_CONTAINS_SCHEME_RX.search(context) or BASE64_LONG_RX.search(context):
            return True

    return False


# ------------------------
# Utilities
# ------------------------
def sane_name_from_url(url: str) -> str:
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
# Ordering helper
# ------------------------
def order_results_newest_first(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not results:
        return results

    have_time = any(bool(r.get("time_iso")) for r in results)
    if have_time:
        def _key(r):
            t = r.get("time_iso")
            return t or ""
        ordered = sorted(results, key=_key, reverse=True)
        logging.debug("Ordered by time_iso descending (newest first).")
    else:
        ordered = list(reversed(results))
        logging.debug("No time_iso present: simple reversed order applied.")

    for i, r in enumerate(ordered, start=1):
        r["index"] = i
    return ordered


# ------------------------
# Save filtered results directly to raws/<base>.json
# ------------------------
def write_filtered_file(raws_dir: str, base: str, matched_items: List[Dict[str, Any]]) -> None:
    out_fname = f"{base}.json"
    out_path = os.path.join(raws_dir, out_fname)
    if matched_items:
        try:
            with open(out_path, "w", encoding="utf-8") as fw:
                json.dump(matched_items, fw, ensure_ascii=False, indent=2)
            logging.info("Wrote %d filtered items -> %s", len(matched_items), out_path)
        except Exception as e:
            logging.exception("Failed to write filtered file %s: %s", out_path, e)
    else:
        if os.path.exists(out_path):
            try:
                os.remove(out_path)
                logging.info("Removed existing filtered file %s (no matches found)", out_path)
            except Exception:
                logging.debug("Failed to remove %s", out_path)


# ------------------------
# Main CLI
# ------------------------
def main():
    p = argparse.ArgumentParser(description="TGScraper - scrape and save filtered config messages directly to raws/*.json.")
    p.add_argument("--channels", "-c", default=DEFAULT_CHANNELS_FILE, help="Channels file (one URL per line)")
    p.add_argument("--outdir", "-o", default=DEFAULT_OUT_DIR, help="Output directory for filtered JSON files")
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

        ordered = order_results_newest_first(results)

        # Build matched_items list directly from ordered results (include metadata)
        matched_items: List[Dict[str, Any]] = []
        for entry in ordered:
            msg_text = entry.get("message_text") or entry.get("message") or ""
            if not isinstance(msg_text, str):
                continue
            if detect_config_in_text(msg_text):
                author = entry.get("author") if isinstance(entry.get("author"), dict) else {}
                source = author.get("href") if isinstance(author.get("href"), str) else None

                # keep fields for traceability; message_text remains unmodified (raw)
                matched_items.append({
                    "index": entry.get("index"),
                    "post": entry.get("post"),
                    "time_iso": entry.get("time_iso"),
                    "author": author,
                    "source": source,
                    "message_text": msg_text,
                    "message_html": entry.get("message_html"),
                    "views": entry.get("views"),
                    "reactions": entry.get("reactions"),
                })

        base = sane_name_from_url(url)
        write_filtered_file(args.outdir, base, matched_items)

        if i != len(channels):
            time.sleep(args.delay)


if __name__ == "__main__":
    main()
