import fs from 'fs';
import path from 'path';
import { chromium } from 'playwright';

/** Minimum long-timeout budget for navigation / selectors on slower machines (8GB RAM, etc.). */
const PAGE_TIMEOUT_MS = 60000;
/** Total budget for waiting until the results panel appears. */
const MAPS_RESULTS_BUDGET_MS = Math.max(120000, PAGE_TIMEOUT_MS);
/** Playwright may need extra time to spawn Chromium on constrained PCs. */
const BROWSER_LAUNCH_TIMEOUT_MS = 120000;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/** Visible browser only — do not toggle via env (debugging / consent screens). */
const HEADLESS = false;

function resolveChromiumExecutablePath() {
  const candidates = [
    process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH,
    process.env.CHROMIUM_PATH,
    process.env.PW_CHROMIUM_PATH,
  ].filter(Boolean);
  for (const p of candidates) {
    const normalized = path.normalize(p);
    if (fs.existsSync(normalized)) {
      return normalized;
    }
    console.warn(`Chromium path missing (check env): ${normalized}`);
  }
  return undefined;
}

function logExpectedBundledChromiumWindows() {
  if (process.platform !== 'win32') {
    return;
  }
  const local = process.env.LOCALAPPDATA;
  if (!local) return;
  const base = path.join(local, 'ms-playwright');
  if (!fs.existsSync(base)) {
    console.warn(
      `No Playwright browsers under ${base}. From project root run: npx playwright install chromium`,
    );
    return;
  }
  let latest = '';
  try {
    const dirs = fs.readdirSync(base).filter((d) => d.startsWith('chromium-'));
    dirs.sort();
    latest = dirs[dirs.length - 1] || '';
  } catch {
    return;
  }
  if (!latest) return;
  const exe = path.join(base, latest, 'chrome-win', 'chrome.exe');
  if (fs.existsSync(exe)) {
    console.log('Expected bundled Chromium (Playwright):', exe);
    console.log('Override with PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH if you use a custom build.');
  }
}

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

async function dismissCookieConsent(page) {
  const candidates = [
    page.getByRole('button', { name: /accept all|i agree|accept|agree/i }),
    page.locator('button:has-text("Accept all")'),
    page.locator('form[action*="consent"] button').first(),
    page.locator('#L2AGLb'),
  ];
  for (const loc of candidates) {
    try {
      if (await loc.isVisible({ timeout: PAGE_TIMEOUT_MS }).catch(() => false)) {
        await loc.click({ timeout: PAGE_TIMEOUT_MS });
        await sleep(1500);
        return;
      }
    } catch {
      // try next
    }
  }
}

async function waitForMapsResults(page, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  const selectors = [
    'div[role="feed"]',
    'div[role="feed"] a[href*="/maps/place/"]',
    'a[href*="/maps/place/"]',
    'a[href*="/place/"]',
    '[aria-label*="Results for"]',
    'div[role="main"] [role="article"]',
  ];

  while (Date.now() < deadline) {
    const remaining = Math.max(1000, deadline - Date.now());
    const slice = Math.min(PAGE_TIMEOUT_MS, remaining);
    for (const sel of selectors) {
      try {
        const el = await page.waitForSelector(sel, { timeout: slice, state: 'attached' });
        if (el) {
          return true;
        }
      } catch {
        // try next selector
      }
    }
    await sleep(800);
  }
  return false;
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
      const aria = root.querySelector('[aria-label*="star"], [aria-label*="Star"]');
      if (aria) {
        const m = (aria.getAttribute('aria-label') || '').match(/(\d+(?:\.\d+)?)/);
        if (m) return m[1];
      }
      return '';
    };

    const findReviewCount = (root) => {
      if (!root) return '';
      const text = root.innerText || '';
      const m = text.match(/([\d,]+)\s*reviews?\b/i);
      if (m) return m[1].replace(/,/g, '');
      const el = root.querySelector('[aria-label*="review"]');
      if (el) {
        const mm = (el.getAttribute('aria-label') || el.textContent || '').match(/([\d,]+)/);
        if (mm) return mm[1].replace(/,/g, '');
      }
      return '';
    };

    const findCategories = (root) => {
      if (!root) return '';
      const chips = root.querySelectorAll(
        'button[jsaction*="category"], button.DkEaL, span.fontBodySmall, div.W4Efsd span',
      );
      const labels = [];
      for (const el of chips) {
        const t = normalize(el.textContent || '');
        if (t.length < 2 || t.length > 60) continue;
        if (/website|directions|reviews?|photos?|save|share|call|menu|^hours|^open|^closed/i.test(t)) continue;
        if (/^[\d.]+\s*(★|stars?)?$/i.test(t)) continue;
        labels.push(t);
      }
      return [...new Set(labels)].slice(0, 4).join(', ');
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

    const feed = document.querySelector('div[role="feed"]');
    let anchors = [];
    if (feed) {
      anchors = Array.from(feed.querySelectorAll('a[href*="/maps/place/"], a[href*="/place/"]'));
    }
    if (!anchors.length) {
      anchors = Array.from(document.querySelectorAll('a[href*="/maps/place/"], a[href*="/place/"]'));
    }

    const seen = new Set();
    anchors = anchors.filter((a) => {
      const key = (a.href || '').split('&')[0];
      if (!key || seen.has(key)) return false;
      seen.add(key);
      return true;
    });

    const results = [];
    for (const anchor of anchors) {
      if (results.length >= limit) break;
      const root =
        anchor.closest('div[role="article"]') ||
        anchor.closest('[role="article"]') ||
        anchor.closest('div[jsaction]') ||
        anchor.parentElement?.parentElement ||
        anchor.parentElement;

      let name = normalize(anchor.getAttribute('aria-label') || '');
      if (name.includes('·')) {
        name = normalize(name.split('·')[0]);
      }
      if (!name) {
        name = normalize(anchor.textContent);
      }
      if (!name && root) {
        const titleEl = root.querySelector('[class*="fontHeadline"], [class*="qBF1Pd"], span');
        name = normalize(titleEl?.textContent);
      }
      const href = anchor.href || '';
      const website = findWebsiteLink(root) || '';
      const rating = findRating(root || anchor);
      const reviewCount = findReviewCount(root || anchor);
      const categories = findCategories(root || anchor);
      results.push({
        name,
        website,
        rating,
        reviewCount,
        categories,
        sourceUrl: href,
      });
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
      categories: '',
      address: '',
      review_count: '',
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
  const headers = ['name', 'website', 'phone', 'categories', 'address', 'sourceUrl', 'searchQuery'];
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
        'div.F7nice span[aria-hidden="true"]',
        'span[role="img"][aria-label*="star"]',
        '[aria-label*="stars"]',
        'div[role="img"][aria-label*="star"]',
        'span.section-star-display',
      ];
      for (const selector of ratingSelectors) {
        const element = document.querySelector(selector);
        if (element) {
          const label = element.getAttribute('aria-label') || element.textContent || '';
          const m = label.match(/(\d+(?:\.\d+)?)/);
          if (m) return m[1];
          const t = normalize(element.textContent);
          if (/^\d/.test(t)) return t.split(/\s/)[0];
        }
      }
      const bodyText = document.body?.innerText || '';
      const rm = bodyText.match(/(\d+(?:\.\d+)?)\s*(?:★|stars?|out of)/i);
      return rm ? rm[1] : '';
    };

    const findReviewCount = () => {
      const bodyText = document.body?.innerText || '';
      const m = bodyText.match(/([\d,]+)\s+reviews?\b/i);
      if (m) return m[1].replace(/,/g, '');
      const el = document.querySelector('[aria-label*="review"]');
      if (el) {
        const mm = (el.getAttribute('aria-label') || '').match(/([\d,]+)/);
        if (mm) return mm[1].replace(/,/g, '');
      }
      return '';
    };

    const cleanPhone = (raw) => {
      const text = (raw || '').replace(/^[\s\u200b\uFEFF]+/, '').replace(/^[^\d+()\-.\s]+/, '');
      const m = text.match(/\+?\d[\d\s().\-]{6,}\d/);
      return m ? m[0].trim() : '';
    };

    const findPhone = () => {
      const phoneButtons = Array.from(document.querySelectorAll('button[data-item-id*="phone"], button[aria-label*="phone" i]'));
      for (const button of phoneButtons) {
        const text = button.innerText || button.getAttribute('aria-label') || '';
        const cleaned = cleanPhone(text);
        if (cleaned) return cleaned;
      }
      const copyPhone = document.querySelector('button[data-tooltip*="phone" i], [data-item-id*="phone"]');
      if (copyPhone) {
        const text = copyPhone.textContent || copyPhone.getAttribute('aria-label') || '';
        const cleaned = cleanPhone(text);
        if (cleaned) return cleaned;
      }
      const phoneElements = Array.from(document.querySelectorAll('span'));
      for (const span of phoneElements) {
        const cleaned = cleanPhone(span.innerText || '');
        if (cleaned) return cleaned;
      }
      return '';
    };

    const cleanAddress = (raw) => {
      let s = normalize(raw || '');
      const idx = s.search(/[0-9A-Za-z]/);
      if (idx > 0) s = s.slice(idx);
      return s;
    };

    const findAddress = () => {
      const addr = document.querySelector('button[data-item-id="address"]');
      if (addr) {
        return cleanAddress(addr.textContent || addr.getAttribute('aria-label') || '');
      }
      const addr2 = document.querySelector('[data-item-id="address"]');
      if (addr2) {
        return cleanAddress(addr2.textContent || '');
      }
      return '';
    };

    const findCategories = () => {
      const chips = document.querySelectorAll(
        'button[jsaction*="category"], button.DkEaL, button[class*="DkEaL"]',
      );
      const out = [];
      for (const b of chips) {
        const t = normalize(b.textContent || '');
        if (t.length < 2 || t.length > 60) continue;
        if (/website|directions|reviews?|photos?|save|share|call|menu|hours|filter|sign in|map/i.test(t)) continue;
        out.push(t);
      }
      return [...new Set(out)].slice(0, 6).join(', ');
    };

    const nameSelectors = [
      'h1.DUwDvf',
      'h1.qrShPb',
      '[role="main"] h1',
      'h1 span',
      '[data-attrid="title"]',
      'h1',
    ];
    let name = '';
    for (const sel of nameSelectors) {
      const el = document.querySelector(sel);
      if (el && normalize(el.textContent)) {
        name = normalize(el.textContent);
        break;
      }
    }

    const website = normalize(findWebsite());
    const rating = normalize(findRating());
    const phone = normalize(findPhone());
    const address = normalize(findAddress());
    const categories = normalize(findCategories());
    const review_count = normalize(findReviewCount());
    return { name, website, rating, phone, address, categories, review_count };
  });
}

async function main() {
  const args = parseArgs();
  const city = args.city;
  const keyword = args.keyword;
  const limit = Number(args.limit || 5);
  const outputFile = args.output || path.resolve('output', 'businesses.json');
  const allowFallback = args['allow-fallback'] === 'true' || args.allowFallback === 'true';

  if (!city || !keyword) {
    console.error('Usage: node src/maps_scraper/maps_scraper.js --city "Austin" --keyword "cybersecurity" [--limit 5] [--output ./output/businesses.json] [--allow-fallback true]');
    process.exit(1);
  }

  const searchQuery = `${keyword} in ${city}`;
  const searchUrl = `https://www.google.com/maps/search/${encodeURIComponent(searchQuery)}`;
  const report = [];
  const outDir = path.dirname(outputFile);
  ensureDirectory(outDir);

  logExpectedBundledChromiumWindows();
  const customExecutable = resolveChromiumExecutablePath();

  console.log('Browser Launching...');
  const launchOptions = {
    headless: HEADLESS,
    slowMo: 50,
    timeout: BROWSER_LAUNCH_TIMEOUT_MS,
    args: ['--start-maximized', '--disable-blink-features=AutomationControlled'],
  };
  if (customExecutable) {
    launchOptions.executablePath = customExecutable;
    console.log('Using chromium executablePath:', customExecutable);
  } else {
    console.log('Using Playwright-managed Chromium (install: npx playwright install chromium)');
  }

  const browser = await chromium.launch(launchOptions);
  const context = await browser.newContext({
    viewport: { width: 1400, height: 900 },
    userAgent:
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    locale: 'en-US',
    extraHTTPHeaders: {
      'Accept-Language': 'en-US,en;q=0.9',
    },
  });
  const page = await context.newPage();

  try {
    console.log(`Navigating to ${searchUrl}`);
    await page.goto(searchUrl, {
      timeout: Math.max(120000, PAGE_TIMEOUT_MS),
      waitUntil: 'domcontentloaded',
    });
    await sleep(2000);
    await dismissCookieConsent(page);
    await sleep(2000);

    const loaded = await waitForMapsResults(page, MAPS_RESULTS_BUDGET_MS);
    if (!loaded) {
      console.warn('Warning: results panel not detected within timeout; continuing with partial page state');
    }
    await sleep(3000);

    const blocked =
      (await page.getByText(/unusual traffic|sorry/i).count()) > 0;
    if (blocked) {
      console.error('Blocked or challenged by Google');
      await page.screenshot({ path: path.join(outDir, 'maps_debug_blocked.png'), fullPage: true }).catch(() => null);
      throw new Error('Blocked or challenged by Google');
    }

    const scrollSelector = 'div[role="feed"], div[aria-label*="Results"], div[role="main"]';
    const scrollable = await page.$(scrollSelector);
    let previousCount = 0;
    let currentCount = 0;
    if (scrollable) {
      for (let step = 0; step < 12; step += 1) {
        await scrollable.evaluate((element) => {
          element.scrollBy({ top: element.scrollHeight, behavior: 'instant' });
        });
        await sleep(2000);
        const cardsLocator = page.locator('div[role="article"], div[role="feed"] a[href*="/maps/place/"]');
        currentCount = await cardsLocator.count();
        if (currentCount <= previousCount && step > 1) {
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

    const hasListWebsites = normalizedListResults.some((item) => item.website);
    if (hasListWebsites && normalizedListResults.length > 0) {
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
          address: normalizeText(item.address || ''),
          categories: normalizeText(item.categories || ''),
          review_count: normalizeText(item.reviewCount || ''),
          maps_only: item.maps_only,
          scraped_at: new Date().toISOString(),
        });
      }
    } else {
      let cards = page.locator('div[role="article"]');
      let cardCount = await cards.count();
      if (cardCount === 0) {
        cards = page.locator('div[role="feed"] a[href*="/maps/place/"], div[role="feed"] a[href*="/place/"]');
        cardCount = await cards.count();
      }
      if (cardCount === 0) {
        cards = page.locator('a[href*="/maps/place/"]');
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
          await sleep(3500);
          await page
            .waitForSelector('[role="main"] h1, h1.DUwDvf, h1.qrShPb, h1 span, [data-attrid="title"]', {
              timeout: PAGE_TIMEOUT_MS,
            })
            .catch(() => null);

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
            address: normalizeText(details.address || ''),
            categories: normalizeText(details.categories || ''),
            review_count: normalizeText(details.review_count || ''),
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
    await page.screenshot({ path: path.join(outDir, 'maps_debug_error.png'), fullPage: true }).catch(() => null);
  } finally {
    await browser.close();
  }

  if (report.length === 0) {
    if (allowFallback) {
      console.log('No results; fallback sample rows enabled');
      report.push(...fallbackResults(searchQuery, searchUrl));
    } else {
      console.error('No businesses extracted. Check maps_debug_*.png in the output folder, consent/captcha, or selectors.');
      console.error('Install browsers: cd project root && npm install && npx playwright install chromium');
    }
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
