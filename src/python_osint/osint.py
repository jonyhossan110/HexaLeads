import argparse
import json
import os
import sys
import re
from datetime import datetime

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

import requests

EMAIL_REGEX = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
SOCIAL_PATTERNS = [
    re.compile(r"https?://(?:www\.)?twitter\.com/[A-Za-z0-9_\-]+"),
    re.compile(r"https?://(?:www\.)?linkedin\.com/[A-Za-z0-9_\-/%]+"),
    re.compile(r"https?://(?:www\.)?facebook\.com/[A-Za-z0-9_\-./]+"),
    re.compile(r"https?://(?:www\.)?instagram\.com/[A-Za-z0-9_\-./]+"),
    re.compile(r"https?://(?:www\.)?github\.com/[A-Za-z0-9_\-./]+"),
]


def extract_emails(text):
    return sorted(set(EMAIL_REGEX.findall(text)))


def extract_social_links(text):
    links = set()
    for pattern in SOCIAL_PATTERNS:
        for match in pattern.findall(text):
            links.add(match)
    return sorted(links)


def fetch_url(url):
    headers = {"User-Agent": "HexaLeads-OSINT/1.0"}
    response = requests.get(url, headers=headers, timeout=20)
    response.raise_for_status()
    return response.text


def load_json(path):
    with open(path, "r", encoding="utf-8") as file:
        return json.load(file)


def time_iso():
    return datetime.utcnow().isoformat() + "Z"


def build_osint_record(record):
    website = record.get("website") or record.get("url") or record.get("source") or ""
    result = {
        "name": record.get("name", ""),
        "website": website,
        "phone": record.get("phone", ""),
        "source": record.get("source", ""),
        "searchQuery": record.get("searchQuery", ""),
        "emails": [],
        "social_links": [],
        "scanned_at": time_iso(),
    }

    if not website:
        result["error"] = "no website available"
        return result

    try:
        html = fetch_url(website)
        result["emails"] = extract_emails(html)
        result["social_links"] = extract_social_links(html)
    except Exception as error:
        result["error"] = str(error)

    return result


def ensure_directory(path):
    directory = os.path.dirname(path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)


def main():
    parser = argparse.ArgumentParser(description="Python OSINT extractor for HexaLeads")
    parser.add_argument("--input", default="output/analyzed.json", help="Input analyzed JSON file")
    parser.add_argument("--output", default="output/osint.json", help="Output JSON file")
    parser.add_argument("--url", help="Optional single website URL to scan")
    args = parser.parse_args()

    if args.url:
        records = [{"website": args.url, "source": args.url}]
    else:
        if not os.path.exists(args.input):
            print(f"Input file does not exist: {args.input}")
            return
        records = load_json(args.input)

    ensure_directory(args.output)

    output_data = []
    for record in records:
        osint_record = build_osint_record(record)
        output_data.append(osint_record)
        print(f"Processed OSINT for {osint_record.get('website', 'unknown')}")

    with open(args.output, "w", encoding="utf-8") as file:
        json.dump(output_data, file, indent=2)
    print(f"Saved OSINT output to {args.output}")


if __name__ == "__main__":
    main()
