#!/usr/bin/env python3
"""
extract_configs.py

Scan JSON files in the `raws/` directory (output of main.py) and extract
messages that contain v2ray-style URIs. Save raw message_text values
(unchanged) together with the author.href as `source` into
`configs/<basename>_config.json` as a JSON array of objects.

Usage:
  python3 extract_configs.py
  python3 extract_configs.py --indir raws --outdir configs --verbose
"""

from __future__ import annotations
import argparse
import json
import logging
import os
import re
from typing import List, Dict, Any, Optional

# schemes to detect (primary direct-scheme detection)
PRIMARY_SCHEMES = [
    "vmess",
    "ss",
    "socks",
    "vless",
    "trojan",
    "wireguard",
    "hysteria2",
]

# compiled regex for direct scheme like "vmess://"
PRIMARY_RX = re.compile(r"(?i)\b(" + "|".join(re.escape(s) for s in PRIMARY_SCHEMES) + r")://")

# http regex (we'll treat it conservatively)
HTTP_RX = re.compile(r"(?i)\bhttp://[^\s'\"<>]+")  # basic http url capture

# heuristic: long base64-like tokens (very common in vmess export)
BASE64_LONG_RX = re.compile(r"[A-Za-z0-9+/]{40,}={0,2}")

# helper: check if http url contains embedded scheme keywords
HTTP_CONTAINS_SCHEME_RX = re.compile(
    r"(?i)(vmess|vless|trojan|ss|socks|hysteria|wireguard)"
)


def detect_config_in_text(text: str) -> bool:
    """
    Return True if `text` likely contains a v2ray-style config.
    Heuristics:
      - direct match of vmess://, ss://, etc. -> True
      - http:// links: only True if the http URL contains one of the scheme keywords
        (e.g. /vmess, #vless) OR contains a long base64-like token -> True
      - otherwise False
    """
    if not text:
        return False

    # Quick primary detection (vmess://, ss://, ...)
    if PRIMARY_RX.search(text):
        return True

    # Check for http links, but conservatively
    for m in HTTP_RX.finditer(text):
        url = m.group(0)
        # if the http URL itself contains scheme-keywords -> treat as config
        if HTTP_CONTAINS_SCHEME_RX.search(url):
            return True
        # if the http URL contains a long base64-like string -> likely a config (e.g. vmess base64)
        if BASE64_LONG_RX.search(url):
            return True
        # if surrounding text contains keywords near the URL (e.g. "vmess" within 50 chars) treat as config
        span_start, span_end = max(m.start() - 50, 0), min(m.end() + 50, len(text))
        context = text[span_start:span_end]
        if HTTP_CONTAINS_SCHEME_RX.search(context) or BASE64_LONG_RX.search(context):
            return True

    # No detection
    return False


def load_json_file(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def write_config_file(out_path: str, items: List[Dict[str, Any]]) -> None:
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)


def process_raw_dir(indir: str, outdir: str, verbose: bool = False) -> None:
    if not os.path.exists(indir):
        logging.error("Input directory not found: %s", indir)
        return

    os.makedirs(outdir, exist_ok=True)

    json_files = sorted(
        [
            fn
            for fn in os.listdir(indir)
            if fn.lower().endswith(".json") and os.path.isfile(os.path.join(indir, fn))
        ]
    )
    if not json_files:
        logging.info("No JSON files found in %s", indir)
        return

    total_matches = 0
    for fname in json_files:
        path = os.path.join(indir, fname)
        base = fname[:-5]  # remove .json
        out_fname = f"{base}_config.json"
        out_path = os.path.join(outdir, out_fname)

        try:
            data = load_json_file(path)
        except Exception as e:
            logging.exception("Failed to load %s: %s", path, e)
            continue

        if not isinstance(data, list):
            logging.warning("File %s does not contain a top-level JSON array; skipping.", path)
            continue

        matched_items: List[Dict[str, Any]] = []
        for entry in data:
            # entry expected to be a dict with "message_text" and maybe "author" dict
            if not isinstance(entry, dict):
                continue
            msg_text = entry.get("message_text") or entry.get("message") or ""
            if not isinstance(msg_text, str):
                continue

            if detect_config_in_text(msg_text):
                # extract author.href if present
                author = entry.get("author") if isinstance(entry.get("author"), dict) else {}
                source = author.get("href") if isinstance(author.get("href"), str) else None

                matched_items.append({"message_text": msg_text, "source": source})

        if matched_items:
            try:
                write_config_file(out_path, matched_items)
                logging.info("Wrote %d config items -> %s", len(matched_items), out_path)
                total_matches += len(matched_items)
            except Exception as e:
                logging.exception("Failed to write %s: %s", out_path, e)
        else:
            # remove any existing config file when nothing matched (keeps directory tidy)
            if os.path.exists(out_path):
                try:
                    os.remove(out_path)
                    logging.info("Removed existing empty config file %s (no matches found)", out_path)
                except Exception:
                    logging.debug("Failed to remove %s", out_path)
            else:
                logging.debug("No matches for %s", fname)

    logging.info("Finished. Total matched messages across files: %d", total_matches)


def main() -> None:
    p = argparse.ArgumentParser(description="Extract v2ray-style configs from raw JSON files.")
    p.add_argument("--indir", default="raws", help="Input directory containing raw JSON files")
    p.add_argument("--outdir", default="configs", help="Output directory for extracted config JSONs")
    p.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")
    args = p.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, format="%(levelname)s: %(message)s")

    process_raw_dir(args.indir, args.outdir, verbose=args.verbose)


if __name__ == "__main__":
    main()
