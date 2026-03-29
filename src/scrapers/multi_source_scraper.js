import fs from 'fs';
import path from 'path';
import { spawnSync } from 'child_process';

function parseArgs() {
  const args = process.argv.slice(2);
  const result = {};
  for (let i = 0; i < args.length; i += 1) {
    if (args[i].startsWith('--')) {
      const key = args[i].slice(2);
      const next = args[i + 1];
      if (next && !next.startsWith('--')) {
        result[key] = next;
        i += 1;
      } else {
        result[key] = 'true';
      }
    }
  }
  return result;
}

function normalizeText(value) {
  return value ? value.trim().replace(/\s+/g, ' ') : '';
}

function ensureDirectory(dirPath) {
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true });
  }
}

function runNodeScript(scriptPath, args, description) {
  console.log(`-- running ${description}`);
  const command = ['node', scriptPath, ...args];
  const result = spawnSync(command[0], command.slice(1), {
    cwd: process.cwd(),
    stdio: ['inherit', 'inherit', 'inherit'],
  });
  if (result.error) {
    throw result.error;
  }
  if (result.status !== 0) {
    throw new Error(`${description} failed with exit code ${result.status}`);
  }
}

function safeLoadJson(filePath) {
  if (!fs.existsSync(filePath)) {
    return [];
  }
  try {
    const raw = fs.readFileSync(filePath, 'utf-8');
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch (error) {
    console.warn(`Warning: failed to parse ${filePath}: ${error.message}`);
    return [];
  }
}

function normalizeWebsite(rawUrl) {
  if (!rawUrl || typeof rawUrl !== 'string') {
    return '';
  }
  let url = rawUrl.trim();
  if (!url) {
    return '';
  }
  if (!/^https?:\/\//i.test(url)) {
    url = `http://${url}`;
  }
  try {
    const parsed = new URL(url);
    let host = parsed.hostname.toLowerCase();
    if (host.startsWith('www.')) {
      host = host.slice(4);
    }
    return host;
  } catch {
    return '';
  }
}

function businessScore(record) {
  const fields = ['name', 'website', 'phone', 'rating', 'source'];
  return fields.reduce((score, field) => score + (record[field] ? 1 : 0), 0);
}

function dedupeByWebsite(records) {
  const unique = new Map();
  const withoutWebsite = [];

  for (const record of records) {
    const normalized = normalizeWebsite(record.website);
    if (!normalized) {
      withoutWebsite.push(record);
      continue;
    }

    if (!unique.has(normalized)) {
      unique.set(normalized, record);
      continue;
    }

    const existing = unique.get(normalized);
    if (businessScore(record) > businessScore(existing)) {
      unique.set(normalized, record);
    }
  }

  return [...unique.values(), ...withoutWebsite];
}

function markSource(records, source) {
  return records.map((record) => ({
    ...record,
    source,
    name: normalizeText(record.name),
    website: normalizeText(record.website),
    phone: normalizeText(record.phone),
    rating: normalizeText(record.rating),
  }));
}

async function main() {
  const args = parseArgs();
  const city = args.city;
  const keyword = args.keyword;
  const limit = Number(args.limit || 10);
  const outputFile = args.output || path.resolve('output', 'businesses.json');

  if (!city || !keyword) {
    console.error('Usage: node src/scrapers/multi_source_scraper.js --city "Austin" --keyword "cybersecurity" [--limit 10] [--output ./output/businesses.json]');
    process.exit(1);
  }

  ensureDirectory(path.dirname(outputFile));

  const scraperDir = path.resolve(__dirname, '..', 'maps_scraper');
  const googleScript = path.resolve(scraperDir, 'maps_scraper.js');
  const yelpScript = path.resolve(scraperDir, 'yelp_scraper.js');
  const bingScript = path.resolve(scraperDir, 'bing_scraper.js');
  const yellowpagesScript = path.resolve(scraperDir, 'yellowpages_scraper.js');

  const tempFiles = {
    google: path.resolve(outputFile.replace(/\.json$/i, '.google.json')),
    yelp: path.resolve(outputFile.replace(/\.json$/i, '.yelp.json')),
    bing: path.resolve(outputFile.replace(/\.json$/i, '.bing.json')),
    yellowpages: path.resolve(outputFile.replace(/\.json$/i, '.yellowpages.json')),
  };

  const results = [];

  try {
    runNodeScript(googleScript, ['--city', city, '--keyword', keyword, '--limit', String(limit), '--output', tempFiles.google], 'Google Maps scraper');
    let googleRecords = safeLoadJson(tempFiles.google);
    googleRecords = markSource(googleRecords, 'google');
    results.push(...googleRecords);

    let uniqueCount = dedupeByWebsite(results).length;
    if (uniqueCount < 5) {
      runNodeScript(yelpScript, ['--city', city, '--keyword', keyword, '--limit', String(limit), '--output', tempFiles.yelp], 'Yelp scraper');
      let yelpRecords = safeLoadJson(tempFiles.yelp);
      yelpRecords = markSource(yelpRecords, 'yelp');
      results.push(...yelpRecords);
      uniqueCount = dedupeByWebsite(results).length;
    }

    if (uniqueCount < 5) {
      runNodeScript(bingScript, ['--city', city, '--keyword', keyword, '--limit', String(limit), '--output', tempFiles.bing], 'Bing scraper');
      let bingRecords = safeLoadJson(tempFiles.bing);
      bingRecords = markSource(bingRecords, 'bing');
      results.push(...bingRecords);
      uniqueCount = dedupeByWebsite(results).length;
    }

    if (uniqueCount < 5 && fs.existsSync(yellowpagesScript)) {
      runNodeScript(yellowpagesScript, ['--city', city, '--keyword', keyword, '--limit', String(limit), '--output', tempFiles.yellowpages], 'YellowPages scraper');
      let yellowRecords = safeLoadJson(tempFiles.yellowpages);
      yellowRecords = markSource(yellowRecords, 'yellowpages');
      results.push(...yellowRecords);
    }

    const merged = dedupeByWebsite(results);
    fs.writeFileSync(outputFile, JSON.stringify(merged, null, 2), 'utf-8');
    console.log(`Saved aggregated businesses to ${outputFile}`);
    console.log(`Total unique businesses: ${merged.length}`);
  } catch (error) {
    console.error('Multi-source scraper failed:', error.message || error);
    process.exit(1);
  }
}

main().catch((error) => {
  console.error('Unexpected error:', error);
  process.exit(1);
});
