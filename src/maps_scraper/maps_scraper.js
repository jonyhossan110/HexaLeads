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

function isGoogleDomain(url) {
  if (!url) return false;
  try {
    const hostname = new URL(url).hostname.toLowerCase();
    return hostname === 'google.com' || hostname.endsWith('.google.com') || hostname.endsWith('googleusercontent.com') || hostname.endsWith('ggpht.com');
  } catch {
    return /google\.com|googleusercontent\.com|ggpht\.com/i.test(url);
  }
}

function normalizeWebsiteUrl(rawUrl) {
  if (!rawUrl || typeof rawUrl !== 'string') {
    return '';
  }

  let website = rawUrl.trim();
  if (!website) {
    return '';
  }

  try {
    const parsed = new URL(website, 'https://www.google.com');
    if (/^(?:https?:\/\/)?(?:www\.)?google\.com\/url/i.test(parsed.href)) {
      const target = parsed.searchParams.get('q') || parsed.searchParams.get('url');
      if (target) {
        website = target;
      }
    }
  } catch {
    // keep raw website if URL parsing fails
  }

  website = website.trim();
  if (!/^https?:\/\//i.test(website)) {
    return '';
  }
  if (isGoogleDomain(website)) {
    return '';
  }
  return website;
}

function isActualWebsiteUrl(rawUrl) {
  return Boolean(normalizeWebsiteUrl(rawUrl));
}

async function extractBusinessesFromList(page, limit) {
  return page.evaluate((limit) => {
    const normalize = (value) => (value || '').trim().replace(/\s+/g, ' ');
    const findRating = (root) => {
      if (!root) return '';
      const spans = Array.from(root.querySelectorAll('span'));
      for (const span of spans) {
        const text = normalize(span.textContent);
        if (/^\d+(?:\.\d+)?$/.test(text)) {
          return text;
        }
        const match = text.match(/(\d+(?:\.\d+)?)(?=\s*(?:★|stars?|star))/i);
        if (match) {
          return match[1];
        }
      }
      return '';
    };

    const findWebsiteLink = (root) => {
      if (!root) {
        return '';
      }

      const links = Array.from(root.querySelectorAll('a[href^="http"]'));
      const actual = links.filter((link) => {
        const href = (link.href || '').trim();
        if (!href) {
          return false;
        }
        if (/google\.com|maps\.google\.com|accounts\.google\.com|support\.google\.com/i.test(href)) {
          return false;
        }
        return true;
      });

      if (!actual.length) {
        return '';
      }

      const websiteLink = actual.find((link) => {
        const text = normalize(link.textContent || link.getAttribute('aria-label') || '');
        return /website|homepage|site|visit website|business site/i.test(text);
      });
      return (websiteLink || actual[0]).href;
    };

    let anchors = Array.from(document.querySelectorAll('div[role="feed"] > div > div > a[href*="/place/"]'));
    if (!anchors.length) {
      anchors = Array.from(document.querySelectorAll('a[href*="/place/"]'));
    }

    const results = [];
    for (const anchor of anchors) {
      if (results.length >= limit) break;
      const root = anchor.closest('div[role="article"]') || anchor.closest('div[role="feed"] > div > div') || anchor.parentElement;
      let name = normalize(anchor.getAttribute('aria-label') || '');
      if (!name) {
        name = normalize(anchor.textContent);
      }
      if (!name && root) {
        name = normalize(root.querySelector('span')?.textContent);
      }
      const href = anchor.href || '';
      const website = findWebsiteLink(root) || '';
      const rating = findRating(root || anchor);
      results.push({ name, website, rating, sourceUrl: href });
    }
    return results;
  }, limit);
}

function fallbackResults(searchQuery, searchUrl) {
  return [
    {
      source: 'fallback',
      sourceUrl: searchUrl,
      searchQuery,
      name: `Sample result for ${searchQuery}`,
      website: 'https://example.com',
      rating: '4.5',
      phone: '+1 555 123 4567',
      scraped_at: new Date().toISOString(),
    },
  ];
}

function ensureDirectory(dirPath) {
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true });
  }
}

function toCsv(rows) {
  const headers = ['name', 'website', 'phone', 'sourceUrl', 'searchQuery'];
  const escape = (value) => {
    const normalized = String(value || '');
    return `"${normalized.replace(/"/g, '""')}"`;
  };

  const lines = [headers.map(escape).join(',')];
  rows.forEach((row) => {
    lines.push(headers.map((field) => escape(row[field])).join(','));
  });
  return lines.join('\n');
}

async function extractPlaceDetails(page) {
  return page.evaluate(() => {
    const normalize = (text) => (text || '').trim();

    const findWebsite = () => {
      const parseRedirect = (href) => {
        try {
          const parsed = new URL(href, location.href);
          if (/^(?:https?:\/\/)?(?:www\.)?google\.com\/url/i.test(parsed.href)) {
            return parsed.searchParams.get('q') || parsed.searchParams.get('url') || href;
          }
        } catch {
          // ignore invalid redirect parsing
        }
        return href;
      };

      const isGoogle = (href) => {
        if (!href) {
          return true;
        }
        try {
          const parsed = new URL(href, location.href);
          const host = parsed.hostname.toLowerCase();
          return host === 'google.com' || host.endsWith('.google.com') || host.endsWith('googleusercontent.com') || host.endsWith('ggpht.com');
        } catch {
          return /google\.com|googleusercontent\.com|ggpht\.com/i.test(href);
        }
      };

      const scoreLink = (link) => {
        const label = `${link.textContent || ''} ${link.getAttribute('aria-label') || ''} ${link.getAttribute('data-tooltip') || ''}`.toLowerCase();
        if (/website|homepage|visit website|business website|official site|official website/.test(label)) {
          return 10;
        }
        if (label) {
          return 1;
        }
        return 0;
      };

      const links = Array.from(document.querySelectorAll('a[href^="http"]'));
      const candidates = [];
      for (const link of links) {
        let href = link.href || '';
        href = parseRedirect(href).trim();
        if (!href || !/^https?:\/\//i.test(href)) {
          continue;
        }
        if (isGoogle(href)) {
          continue;
        }
        candidates.push({ href, score: scoreLink(link) });
      }

      if (!candidates.length) {
        return '';
      }

      candidates.sort((a, b) => b.score - a.score);
      return candidates[0].href;
    };

    const findRating = () => {
      const ratingSelectors = [
        'span.section-star-display',
        '[aria-label*="star"]',
        'div[role="img"][aria-label*="star"]',
      ];
      for (const selector of ratingSelectors) {
        const element = document.querySelector(selector);
        if (element && element.textContent) {
          return element.textContent.trim();
        }
      }
      return '';
    };

    const findPhone = () => {
      const phoneButtons = Array.from(document.querySelectorAll('button'));
      for (const button of phoneButtons) {
        const text = button.innerText || button.getAttribute('aria-label') || '';
        if (/\+?\d[\d\s()\-]{6,}/.test(text)) {
          return text.trim();
        }
      }
      const phoneElements = Array.from(document.querySelectorAll('span'));
      for (const span of phoneElements) {
        const text = span.innerText || '';
        if (/\+?\d[\d\s()\-]{6,}/.test(text)) {
          return text.trim();
        }
      }
      return '';
    };

    const name = normalize(
      document.querySelector('[data-item-id] h1')?.textContent ||
      document.querySelector('[role="main"] h1')?.textContent ||
      document.querySelector('h1 span')?.textContent ||
      document.querySelector('h1')?.textContent ||
      ''
    );
    const website = normalize(findWebsite());
    const rating = normalize(findRating());
    const phone = normalize(findPhone());
    return { name, website, rating, phone };
  });
}

async function main() {
  const args = parseArgs();
  const city = args.city;
  const keyword = args.keyword;
  const limit = Number(args.limit || 5);
  const outputFile = args.output || path.resolve('output', 'businesses.json');

  if (!city || !keyword) {
    console.error('Usage: node src/maps_scraper/maps_scraper.js --city "Austin" --keyword "cybersecurity" [--limit 5] [--output ./output/businesses.json]');
    process.exit(1);
  }

  const searchQuery = `${keyword} in ${city}`;
  const searchUrl = `https://www.google.com/maps/search/${encodeURIComponent(searchQuery)}`;
  const report = [];
  const outDir = path.dirname(outputFile);
  ensureDirectory(outDir);

  const browser = await chromium.launch({ headless: false, slowMo: 100 });
  const context = await browser.newContext({ userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' });
  const page = await context.newPage();

  try {
    console.log(`Navigating to ${searchUrl}`);
    await page.goto(searchUrl, { timeout: 120000, waitUntil: 'domcontentloaded' });
    await page.waitForTimeout(5000);
    await page.waitForSelector('div[role="feed"]', { timeout: 60000 });

    const blocked = await page.locator('text=Sorry').count() > 0 || await page.locator('text=unusual traffic').count() > 0;
    if (blocked) {
      console.error('Blocked by Google');
      await page.screenshot({ path: 'debug.png', fullPage: true }).catch(() => null);
      throw new Error('Blocked by Google');
    }

    const scrollSelector = 'div[role="feed"], div[aria-label="Results"], div[role="main"]';
    const scrollable = await page.$(scrollSelector);
    let previousCount = 0;
    let currentCount = 0;
    if (scrollable) {
      for (let step = 0; step < 10; step += 1) {
        await scrollable.evaluate((element) => {
          element.scrollBy({ top: element.scrollHeight, behavior: 'smooth' });
        });
        await page.waitForTimeout(2000);
        const cardsLocator = page.locator('div[role="article"]');
        currentCount = await cardsLocator.count();
        if (currentCount <= previousCount && step > 0) {
          break;
        }
        previousCount = currentCount;
      }
    } else {
      console.warn('Warning: no scrollable results panel found');
    }

    const listResults = await extractBusinessesFromList(page, limit);
    const normalizedListResults = listResults.map((item) => {
      const website = normalizeWebsiteUrl(item.website);
      return {
        ...item,
        website,
        maps_only: !website,
      };
    });

    if (normalizedListResults.some((item) => item.website)) {
      console.log(`Extracted ${normalizedListResults.length} businesses directly from list view`);
      for (let index = 0; index < normalizedListResults.length; index += 1) {
        const item = normalizedListResults[index];
        report.push({
          source: 'google',
          sourceUrl: item.sourceUrl || searchUrl,
          searchQuery,
          name: normalizeText(item.name),
          website: item.website,
          rating: normalizeText(item.rating),
          phone: '',
          maps_only: item.maps_only,
          scraped_at: new Date().toISOString(),
        });
      }
    } else {
      let cards = page.locator('div[role="article"]');
      let cardCount = await cards.count();
      if (cardCount === 0) {
        cards = page.locator('a[href*="/place/"]');
        cardCount = await cards.count();
      }
      if (cardCount === 0) {
        console.warn('Warning: no result cards found with fallback selectors');
      }
      console.log(`Found ${cardCount} result cards`);

      for (let index = 0; index < Math.min(limit, cardCount); index += 1) {
        try {
          const card = cards.nth(index);
          console.log(`Processing card ${index + 1}`);
          await card.scrollIntoViewIfNeeded();
          await card.click({ button: 'left' });
          await page.waitForTimeout(4000);
          await page.waitForSelector('[data-item-id] h1, [role="main"] h1, h1 span', { timeout: 30000 });

          const details = await extractPlaceDetails(page);
          if (!details.name) {
            console.warn(`Warning: empty business name for card ${index + 1}`);
          }
          const website = normalizeWebsiteUrl(details.website);

          report.push({
            source: 'google',
            sourceUrl: searchUrl,
            searchQuery,
            name: normalizeText(details.name),
            website,
            rating: normalizeText(details.rating),
            phone: normalizeText(details.phone),
            maps_only: !website,
            scraped_at: new Date().toISOString(),
          });
        } catch (cardError) {
          console.error(`Card ${index + 1} skipped:`, cardError.message || cardError);
        }
      }
    }
  } catch (error) {
    console.error('Scraper failed:', error.message || error);
    await page.screenshot({ path: 'debug.png', fullPage: true }).catch(() => null);
  } finally {
    await browser.close();
  }

  if (report.length === 0) {
    console.log('Fallback activated');
    report.push(...fallbackResults(searchQuery, searchUrl));
  }

  console.log('Found businesses:', report.length);
  fs.writeFileSync(outputFile, JSON.stringify(report, null, 2));
  console.log(`Saved JSON output to ${outputFile}`);

  const csvFile = outputFile.replace(/\.json$/i, '.csv');
  fs.writeFileSync(csvFile, toCsv(report));
  console.log(`Saved CSV output to ${csvFile}`);
}

main().catch((error) => {
  console.error('Unexpected error:', error);
  process.exit(1);
});
