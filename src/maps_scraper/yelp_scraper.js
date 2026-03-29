import fs from 'fs';
import path from 'path';
import { chromium } from 'playwright';

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

async function extractYelpResults(page, limit) {
  return page.evaluate((limit) => {
    const normalize = (text) => (text || '').trim().replace(/\s+/g, ' ');
    const anchors = Array.from(document.querySelectorAll('a[href^="/biz/"]'));
    const results = [];
    const seen = new Set();
    const phoneRegex = /\+?\d[\d\s().-]{6,}\d/;
    const ratingRegex = /(\d+(?:\.\d+)?)(?:\s+star|\s*★|\s*stars?)/i;

    for (const anchor of anchors) {
      const href = anchor.href;
      if (!href || seen.has(href)) {
        continue;
      }
      seen.add(href);

      const card = anchor.closest('div[role="article"]') || anchor.closest('li') || anchor.closest('div');
      let name = normalize(anchor.getAttribute('aria-label') || anchor.textContent || '');
      if (!name && card) {
        name = normalize(card.querySelector('h3')?.textContent || card.querySelector('span')?.textContent || '');
      }
      if (!name) {
        continue;
      }

      let website = href;
      let phone = '';
      let rating = '';
      if (card) {
        const cardText = normalize(card.innerText || '');
        const phoneMatch = cardText.match(phoneRegex);
        if (phoneMatch) {
          phone = phoneMatch[0];
        }
        const ratingMatch = cardText.match(ratingRegex);
        if (ratingMatch) {
          rating = ratingMatch[1];
        }
        const directWebsite = Array.from(card.querySelectorAll('a[href^="http"]')).find((link) => !link.href.includes('yelp.com'));
        if (directWebsite && directWebsite.href) {
          website = directWebsite.href;
        }
      }

      results.push({
        name,
        website,
        phone,
        rating,
        source: 'yelp',
      });
      if (results.length >= limit) {
        break;
      }
    }

    return results;
  }, limit);
}

async function main() {
  const args = parseArgs();
  const city = args.city;
  const keyword = args.keyword;
  const limit = Number(args.limit || 5);
  const outputFile = args.output || path.resolve('output', 'yelp_businesses.json');

  if (!city || !keyword) {
    console.error('Usage: node src/maps_scraper/yelp_scraper.js --city "Austin" --keyword "cybersecurity" [--limit 5] [--output ./output/yelp_businesses.json]');
    process.exit(1);
  }

  const searchUrl = `https://www.yelp.com/search?find_desc=${encodeURIComponent(keyword)}&find_loc=${encodeURIComponent(city)}`;
  const outDir = path.dirname(outputFile);
  ensureDirectory(outDir);

  const browser = await chromium.launch({
    headless: false,
    slowMo: 40,
    args: ['--start-maximized', '--disable-blink-features=AutomationControlled'],
  });
  const context = await browser.newContext({ userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' });
  const page = await context.newPage();

  try {
    console.log(`Navigating to ${searchUrl}`);
    await page.goto(searchUrl, { timeout: 120000, waitUntil: 'domcontentloaded' });
    await page.waitForTimeout(5000);
    await page.waitForSelector('a[href^="/biz/"]', { timeout: 30000 });
    const results = await extractYelpResults(page, limit);
    console.log(`Yelp results: ${results.length}`);
    fs.writeFileSync(outputFile, JSON.stringify(results, null, 2), 'utf-8');
    console.log(`Saved Yelp JSON output to ${outputFile}`);
  } catch (error) {
    console.error('Yelp scraper failed:', error.message || error);
    fs.writeFileSync(outputFile, JSON.stringify([], null, 2), 'utf-8');
    process.exit(1);
  } finally {
    await browser.close();
  }
}

main().catch((error) => {
  console.error('Unexpected error:', error);
  process.exit(1);
});
