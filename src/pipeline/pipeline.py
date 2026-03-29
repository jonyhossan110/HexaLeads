import argparse
import json
import os
import subprocess
import sys
from urllib.parse import urlparse

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
OUTPUT_DIR = os.path.abspath(os.path.join(ROOT_DIR, 'output'))
BUSINESSES_FILE = os.path.abspath(os.path.join(OUTPUT_DIR, 'businesses.json'))
MAPS_FILE = os.path.abspath(os.path.join(OUTPUT_DIR, 'maps_businesses.json'))
YELP_FILE = os.path.abspath(os.path.join(OUTPUT_DIR, 'yelp_businesses.json'))
BING_FILE = os.path.abspath(os.path.join(OUTPUT_DIR, 'bing_businesses.json'))
YELLOWPAGES_FILE = os.path.abspath(os.path.join(OUTPUT_DIR, 'yellowpages_businesses.json'))
ANALYZED_FILE = os.path.abspath(os.path.join(OUTPUT_DIR, 'analyzed.json'))
OSINT_FILE = os.path.abspath(os.path.join(OUTPUT_DIR, 'osint.json'))
FINAL_FILE = os.path.abspath(os.path.join(OUTPUT_DIR, 'final_leads.json'))


def ensure_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)


# Subprocess budget for Node/Playwright scrapers (maps can exceed 60s on 8GB RAM).
SCRAPER_SUBPROCESS_TIMEOUT_SEC = 1800


def run_command(command, description):
    print(f'-- {description}')
    print(f'   command: {" ".join(command)}')
    # Inherit env so PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH / NODE options apply on Windows.
    env = os.environ.copy()
    result = subprocess.run(
        command,
        cwd=ROOT_DIR,
        env=env,
        timeout=SCRAPER_SUBPROCESS_TIMEOUT_SEC,
    )
    if result.returncode != 0:
        raise RuntimeError(f'Command failed: {" ".join(command)}')
    print(f'   done: {description}')


def normalize_url(raw_url):
    if not raw_url:
        return ''
    candidate = raw_url.strip()
    if '://' not in candidate:
        candidate = 'http://' + candidate
    parsed = urlparse(candidate)
    if not parsed.netloc:
        return ''
    host = parsed.netloc.lower()
    if host.startswith('www.'):
        host = host[4:]
    path = parsed.path.rstrip('/')
    if path and path != '/':
        return f'{host}{path}'
    return host

AGGREGATOR_DOMAINS = {
    'google.com',
    'maps.google.com',
    'yelp.com',
    'bing.com',
    'yellowpages.com',
}


def get_dedupe_key(record):
    website = normalize_url(record.get('website', '') or '')
    if not website:
        return None
    host = website.split('/')[0]
    if host in AGGREGATOR_DOMAINS:
        return website
    return host


def business_score(record):
    fields = ['name', 'website', 'phone', 'rating', 'source', 'searchQuery', 'scraped_at']
    return sum(bool(record.get(field)) for field in fields)


def load_json(path):
    with open(path, 'r', encoding='utf-8') as file:
        return json.load(file)


def merge_businesses(input_files, output_path, search_query):
    merged = []
    best = {}
    order = []
    now = __import__('datetime').datetime.utcnow().isoformat() + 'Z'

    for path in input_files:
        if not os.path.exists(path):
            continue
        try:
            records = load_json(path)
        except Exception as exc:
            print(f'Warning: unable to load {path}: {exc}')
            continue
        for record in records:
            if not isinstance(record, dict):
                continue
            record.setdefault('searchQuery', search_query)
            record.setdefault('scraped_at', now)
            key = get_dedupe_key(record)
            if key:
                if key in best:
                    existing = best[key]
                    if business_score(record) > business_score(existing):
                        best[key] = record
                else:
                    best[key] = record
                    order.append(key)
            else:
                merged.append(record)

    for key in order:
        merged.append(best[key])

    with open(output_path, 'w', encoding='utf-8') as file:
        json.dump(merged, file, indent=2)
    print(f'Merged {len(merged)} unique businesses to {output_path}')


def run_scraper_script(description, script_name, output_file, city, keyword, limit):
    scraper_path = os.path.abspath(os.path.join(ROOT_DIR, 'src', 'maps_scraper', script_name))
    command = [
        'node',
        scraper_path,
        '--city', city,
        '--keyword', keyword,
        '--limit', str(limit),
        '--output', output_file,
    ]
    try:
        run_command(command, description)
        return True
    except RuntimeError as error:
        print(f'WARNING: {description} failed: {error}')
        return False


def run_scrapers(city, keyword, limit):
    ensure_output_dir()
    run_scraper_script('Google Maps', 'maps_scraper.js', MAPS_FILE, city, keyword, limit)
    run_scraper_script('Yelp', 'yelp_scraper.js', YELP_FILE, city, keyword, limit)
    run_scraper_script('Bing', 'bing_scraper.js', BING_FILE, city, keyword, limit)
    run_scraper_script('YellowPages', 'yellowpages_scraper.js', YELLOWPAGES_FILE, city, keyword, limit)
    merge_businesses([MAPS_FILE, YELP_FILE, BING_FILE, YELLOWPAGES_FILE], BUSINESSES_FILE, f'{keyword} in {city}')
    if os.path.exists(BUSINESSES_FILE) and not load_json(BUSINESSES_FILE):
        print(
            'WARNING: No businesses were merged. '
            'From the project root run: npm install && npx playwright install chromium'
        )


def run_analyzer():
    if not os.path.exists(BUSINESSES_FILE):
        raise FileNotFoundError(f'Missing business file: {BUSINESSES_FILE}')
    analyzer_path = os.path.abspath(os.path.join(ROOT_DIR, 'src', 'go_analyzer', 'main.go'))
    command = [
        'go',
        'run',
        analyzer_path,
        '--input', BUSINESSES_FILE,
        '--output', ANALYZED_FILE,
    ]
    run_command(command, 'Go analyzer')


def run_osint():
    if not os.path.exists(ANALYZED_FILE):
        raise FileNotFoundError(f'Missing analyzed file: {ANALYZED_FILE}')
    osint_path = os.path.abspath(os.path.join(ROOT_DIR, 'src', 'python_osint', 'osint.py'))
    command = [
        sys.executable,
        osint_path,
        '--input', ANALYZED_FILE,
        '--output', OSINT_FILE,
    ]
    run_command(command, 'Python OSINT')


def run_scoring():
    if not os.path.exists(OSINT_FILE):
        raise FileNotFoundError(f'Missing OSINT file: {OSINT_FILE}')
    score_path = os.path.abspath(os.path.join(ROOT_DIR, 'src', 'scoring', 'score.py'))
    command = [
        sys.executable,
        score_path,
        '--input', OSINT_FILE,
        '--output', FINAL_FILE,
    ]
    run_command(command, 'Scoring engine')


def print_summary():
    if os.path.exists(FINAL_FILE):
        leads = load_json(FINAL_FILE)
        print(f'\nPipeline completed. Final leads: {len(leads)}')
        print(f'Output file: {FINAL_FILE}')
    else:
        print('Pipeline completed without final output file.')


def main():
    parser = argparse.ArgumentParser(description='HexaLeads end-to-end pipeline')
    parser.add_argument('--city', help='City to search for in the maps scraper')
    parser.add_argument('--keyword', help='Keyword to search for in the maps scraper')
    parser.add_argument('--limit', type=int, default=5, help='Maximum number of map results to scrape')
    parser.add_argument(
        '--phase',
        choices=('all', 'scrape', 'analyze', 'osint', 'score'),
        default='all',
        help='Run a single phase (for autonomous step execution) or full pipeline.',
    )
    args = parser.parse_args()

    phase = args.phase

    if phase in ('all', 'scrape'):
        if args.city and args.keyword:
            print(f'Starting HexaLeads pipeline for {args.keyword} in {args.city}')
            run_scrapers(args.city, args.keyword, args.limit)
        else:
            if not os.path.exists(BUSINESSES_FILE):
                raise ValueError(
                    'Either provide --city and --keyword, or ensure output/businesses.json already exists'
                )
            print('Skipping scraper because businesses.json already exists')
        if phase == 'scrape':
            print_summary_partial('scrape')
            return

    if phase in ('all', 'analyze'):
        run_analyzer()
        if phase == 'analyze':
            print_summary_partial('analyze')
            return

    if phase in ('all', 'osint'):
        run_osint()
        if phase == 'osint':
            print_summary_partial('osint')
            return

    if phase in ('all', 'score'):
        run_scoring()
        if phase == 'score':
            print_summary_partial('score')
            return

    print_summary()


def print_summary_partial(name):
    """Print a short line after a single-phase run."""
    target = {
        'scrape': BUSINESSES_FILE,
        'analyze': ANALYZED_FILE,
        'osint': OSINT_FILE,
        'score': FINAL_FILE,
    }.get(name)
    if target and os.path.exists(target):
        print(f'Phase {name} completed. Output: {target}')
    else:
        print(f'Phase {name} completed.')


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print(f'ERROR: {error}')
        sys.exit(1)
