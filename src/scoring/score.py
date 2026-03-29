import argparse
import json
import os


def score_lead(record):
    score = 0
    if record.get('website'):
        score += 30
    if record.get('phone'):
        score += 20
    if record.get('emails'):
        score += 20
    if record.get('social_links'):
        score += 15

    missing_headers = record.get('missing_security_headers', [])
    if missing_headers:
        score -= min(len(missing_headers) * 5, 20)

    score = max(0, min(score, 100))
    return score


def priority_label(score):
    if score >= 80:
        return 'HIGH'
    if score >= 50:
        return 'MEDIUM'
    return 'LOW'


def build_final_lead(record):
    score = score_lead(record)
    emails = record.get('emails') or []
    email = emails[0] if isinstance(emails, list) and emails else ''
    return {
        'name': record.get('name', record.get('website', '')),
        'website': record.get('website', ''),
        'email': email,
        'score': score,
        'priority': priority_label(score),
    }


def load_json(path):
    with open(path, 'r', encoding='utf-8') as file:
        return json.load(file)


def ensure_directory(path):
    directory = os.path.dirname(path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)


def main():
    parser = argparse.ArgumentParser(description='Scoring engine for HexaLeads')
    parser.add_argument('--input', required=True, help='JSON input file')
    parser.add_argument('--output', default='output/final_leads.json', help='JSON output file')
    args = parser.parse_args()

    data = load_json(args.input)
    if not isinstance(data, list):
        raise ValueError('Input JSON must be an array of records')

    final_leads = [build_final_lead(record) for record in data]
    ensure_directory(args.output)
    with open(args.output, 'w', encoding='utf-8') as file:
        json.dump(final_leads, file, indent=2)

    print(f'Saved final leads to {args.output}')


if __name__ == '__main__':
    main()
