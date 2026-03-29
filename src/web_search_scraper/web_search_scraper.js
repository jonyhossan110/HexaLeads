/**
 * HexaLeads — Direct website scraping (search → real business sites only → home + contact).
 * Search: Google + DuckDuckGo (+ Bing fallback). No Maps listing URLs.
 *
 * Usage: node src/web_search_scraper/web_search_scraper.js --keyword "..." --limit 10 [--output path.csv]
 */
import fs from 'fs';
import path from 'path';
import { chromium } from 'playwright';
import { load } from 'cheerio';

const HEADLESS = false;
const BROWSER_LAUNCH_TIMEOUT_MS = 120000;
/** Slow-site navigation: Playwright uses `networkidle` (closest to Puppeteer’s networkidle2). */
const SITE_GOTO_TIMEOUT_MS = 45000;
const SEARCH_GOTO_TIMEOUT_MS = 60000;
const SITES_PER_BROWSER_BATCH = 10;

/** Aggressive: catches addresses in visible text, mailto:, and inside `<script>` / JSON-LD strings. */
const EMAIL_RE_GLOBAL = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;

/** Domains that are never useful as lead contact emails. */
const BLOCKED_EMAIL_DOMAINS = new Set([
  'sentry.io',
  'sentry.wixpress.com',
  'w3.org',
  'example.com',
  'example.org',
  'test.com',
  'googlegroups.com',
  'google.com',
  'gstatic.com',
  'googleusercontent.com',
  'schema.org',
  'cloudflare.com',
  'amazonaws.com',
  'bootstrapcdn.com',
  'jquery.com',
  'wordpress.org',
  'gravatar.com',
]);

const BLOCKED_LOCALPART_PREFIXES =
  /^(?:image|img_|i\.img|noreply|no-reply|donotreply|do-not-reply|mailer-daemon|postmaster|abuse|bounce|newsletter|notifications?|promo)/i;

/** Reject fake emails tied to assets / bogus TLDs. */
const BLOCKED_EMAIL_PATTERN =
  /(\.(png|jpe?g|gif|webp|svg|ico|woff2?|ttf|eot|css|js|map))@|^[^@]+@[^@]*\.(png|jpe?g|gif|webp|svg)$/i;

const IMAGE_EXTENSION_TLD = /\.(png|jpe?g|gif|webp|svg|ico)$/i;

/** Host hints — skip search/social/maps/aggregators (prioritize direct business domains). */
const SKIP_HOST_SUBSTRINGS = [
  'google.',
  'gstatic.',
  'googleusercontent.',
  'goo.gl',
  'youtube.',
  'youtu.be',
  'facebook.com',
  'fbcdn.',
  'instagram.',
  'twitter.',
  'x.com',
  'linkedin.com',
  'licdn.',
  'bing.',
  'microsoft.',
  'duckduckgo.',
  'wikipedia.',
  'wikimedia.',
  'amazon.',
  'reddit.',
  'pinterest.',
  'yahoo.',
  'yelp.',
  'yellowpages.',
  'tripadvisor.',
  'foursquare.',
  'bbb.org',
  '-mapquest.',
  'mapquest.',
  'openstreetmap.',
  'waze.',
];

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function randomInterSiteDelayMs() {
  return 3000 + Math.random() * 2000;
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

function resolveChromiumExecutablePath() {
  const candidates = [
    process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH,
    process.env.CHROMIUM_PATH,
    process.env.PW_CHROMIUM_PATH,
  ].filter(Boolean);
  for (const p of candidates) {
    const normalized = path.normalize(p);
    if (fs.existsSync(normalized)) return normalized;
    console.warn(`Chromium path missing: ${normalized}`);
  }
  return undefined;
}

function isSkippableSearchHost(hostname) {
  const h = hostname.toLowerCase();
  return SKIP_HOST_SUBSTRINGS.some((s) => h.includes(s));
}

/** Drop Maps, local packs, and directory profile links — we only want direct business websites. */
function isMapOrDirectoryUrl(urlStr) {
  if (!urlStr) return true;
  let u = urlStr.trim().toLowerCase();
  try {
    const parsed = new URL(urlStr);
    u = parsed.href.toLowerCase();
    const host = parsed.hostname.replace(/^www\./, '');
    if (host.includes('maps.google') || u.includes('google.com/maps')) return true;
    if (u.includes('/maps/place/') || u.includes('/maps/search')) return true;
    if (host === 'goo.gl' && u.includes('maps')) return true;
    if (u.includes('bing.com/maps') || u.includes('live.com/map')) return true;
    if (host.includes('openstreetmap')) return true;
    if (host.includes('mapquest')) return true;
    if (host.includes('yelp.')) return true;
    if (host.includes('yellowpages.') || host.includes('yp.com')) return true;
    if (host.includes('foursquare.com')) return true;
    if (host.includes('tripadvisor.') && (u.includes('/restaurant') || u.includes('/hotel') || u.includes('/attraction')))
      return true;
    if (host.includes('apple.com') && u.includes('maps')) return true;
    if (host.includes('here.com') && u.includes('explore')) return true;
  } catch {
    return true;
  }
  if (u.includes('maps.google') || u.includes('google.com/maps')) return true;
  if (u.includes('/maps/place')) return true;
  return false;
}

function normalizeOriginUrl(raw) {
  if (!raw || typeof raw !== 'string') return null;
  let u = raw.trim();
  if (u.startsWith('//')) u = `https:${u}`;
  if (!/^https?:\/\//i.test(u)) u = `https://${u}`;
  try {
    const parsed = new URL(u);
    if (isSkippableSearchHost(parsed.hostname)) return null;
    if (isMapOrDirectoryUrl(parsed.href)) return null;
    const proto = parsed.protocol === 'http:' ? 'http:' : 'https:';
    return `${proto}//${parsed.hostname.toLowerCase()}`;
  } catch {
    return null;
  }
}

function isGoodEmail(email) {
  if (!email || typeof email !== 'string') return false;
  let e = email.trim().replace(/^['"]+|['"]+$/g, '').toLowerCase();
  if (e.endsWith('.') || e.endsWith(',')) e = e.replace(/[.,]+$/, '');
  if (e.length < 6 || !e.includes('@')) return false;
  const [local, domain] = e.split('@');
  if (!local || !domain || !domain.includes('.')) return false;
  if (IMAGE_EXTENSION_TLD.test(domain)) return false;
  if (BLOCKED_EMAIL_PATTERN.test(e)) return false;
  if (BLOCKED_LOCALPART_PREFIXES.test(local)) return false;
  const d = domain.replace(/^www\./, '');
  if (BLOCKED_EMAIL_DOMAINS.has(d)) return false;
  if (/^\d+\.\d+\.\d+\.\d+$/.test(d)) return false;
  if (['facebook.com', 'linkedin.com', 'twitter.com', 'instagram.com'].some((x) => d.endsWith(x))) return false;
  return true;
}

/** Run regex on full HTML (includes `<script>` strings) plus structured mailto. */
function collectEmailsFromHtml(html) {
  const found = new Set();

  const $ = load(html);
  $('a[href^="mailto:"]').each((_, el) => {
    const href = ($(el).attr('href') || '').replace(/^mailto:/i, '').split('?')[0].trim();
    if (isGoodEmail(href)) found.add(href.toLowerCase());
  });

  const visibleText = $.root().text();
  let m;
  const re1 = new RegExp(EMAIL_RE_GLOBAL.source, 'gi');
  while ((m = re1.exec(visibleText)) !== null) {
    if (isGoodEmail(m[0])) found.add(m[0].toLowerCase());
  }

  const re2 = new RegExp(EMAIL_RE_GLOBAL.source, 'gi');
  while ((m = re2.exec(html)) !== null) {
    if (isGoodEmail(m[0])) found.add(m[0].toLowerCase());
  }

  return [...found];
}

function normalizeSocialUrl(href) {
  if (!href) return '';
  try {
    const u = new URL(href.split(/["'\s>]/)[0], 'https://dummy.local');
    const s = u.href.split('?')[0].replace(/\/$/, '');
    return s;
  } catch {
    return href.split('?')[0].trim();
  }
}

function isJunkSocialUrl(href) {
  const low = href.toLowerCase();
  return (
    /\/dialog\/|\/share\?|\/sharer/i.test(low) ||
    /\/intent\/|widget|plugins/i.test(low) ||
    low.includes('facebook.com/tr') ||
    low.includes('facebook.com/plugins')
  );
}

/** Deep scan: anchors + full HTML regex for LinkedIn / Facebook / Instagram / Twitter. */
function collectSocialFromHtml(html) {
  const out = { linkedin: '', facebook: '', instagram: '', twitter: '' };

  const trySet = (key, url) => {
    if (!url || isJunkSocialUrl(url)) return;
    const n = normalizeSocialUrl(url);
    if (!n || isJunkSocialUrl(n)) return;
    if (!out[key]) out[key] = n;
  };

  const $ = load(html);
  $('a[href]').each((_, el) => {
    let href = ($(el).attr('href') || '').trim();
    if (!href) return;
    const low = href.toLowerCase();
    if (low.includes('linkedin.com/')) trySet('linkedin', href);
    if (low.includes('facebook.com/')) trySet('facebook', href);
    if (low.includes('instagram.com/')) trySet('instagram', href);
    if (low.includes('twitter.com/') || low.includes('x.com/')) trySet('twitter', href);
  });

  const patterns = [
    [/https?:\/\/(?:[\w-]+\.)?linkedin\.com\/[^\s"'<>]+/gi, 'linkedin'],
    [/https?:\/\/(?:[\w-]+\.)?facebook\.com\/[^\s"'<>]+/gi, 'facebook'],
    [/https?:\/\/(?:[\w-]+\.)?instagram\.com\/[^\s"'<>]+/gi, 'instagram'],
    [/https?:\/\/(?:[\w-]+\.)?twitter\.com\/[^\s"'<>]+/gi, 'twitter'],
    [/https?:\/\/(?:[\w-]+\.)?x\.com\/[^\s"'<>]+/gi, 'twitter'],
  ];

  for (const [re, key] of patterns) {
    const r = new RegExp(re.source, re.flags);
    let m;
    while ((m = r.exec(html)) !== null) {
      trySet(key, m[0]);
    }
  }

  return out;
}

/** Strip HTML tags and collapse whitespace for CSV-safe names. */
function cleanWebsiteName(raw) {
  if (!raw) return '';
  const noTags = String(raw).replace(/<[^>]*>/g, ' ');
  const $ = load(`<div id="hexa-name"></div>`);
  $('#hexa-name').text(noTags);
  return $('#hexa-name').text().replace(/\s+/g, ' ').trim().slice(0, 200);
}

function extractSiteName(html) {
  const $ = load(html);
  const og = $('meta[property="og:site_name"]').attr('content')?.trim();
  if (og) return cleanWebsiteName(og);
  const title = $('title').first().text().trim();
  if (title) {
    const cleaned = cleanWebsiteName(title.split(/[|\-–—]/)[0].trim());
    if (cleaned) return cleaned;
  }
  const h1 = $('h1').first().text().trim();
  if (h1) return cleanWebsiteName(h1);
  return '';
}

function mergeSocial(prev, next) {
  return {
    linkedin: prev.linkedin || next.linkedin || '',
    facebook: prev.facebook || next.facebook || '',
    instagram: prev.instagram || next.instagram || '',
    twitter: prev.twitter || next.twitter || '',
  };
}

function mergeExtract(html) {
  const emails = collectEmailsFromHtml(html);
  const social = collectSocialFromHtml(html);
  return { emails, ...social };
}

function detectContactFormOnly(html, alreadyHasEmails) {
  if (alreadyHasEmails) return false;
  const $ = load(html);
  if ($('form').length === 0) return false;
  let contactish = false;
  $('form').each((_, f) => {
    const $f = $(f);
    const idClass = `${$f.attr('id') || ''} ${$f.attr('class') || ''} ${$f.attr('action') || ''}`.toLowerCase();
    if (/contact|enquir|reach|feedback|connect/i.test(idClass)) contactish = true;
    if ($f.find('input[type="email"], input[name*="email" i], textarea, input[type="text"]').length > 0)
      contactish = true;
  });
  if ($('form input[type="submit"], form button[type="submit"]').length && /contact|send|submit/i.test($.root().text()))
    contactish = true;
  return contactish;
}

function formatSocialLinks(s) {
  const parts = [];
  if (s.linkedin) parts.push(`LinkedIn: ${s.linkedin}`);
  if (s.facebook) parts.push(`Facebook: ${s.facebook}`);
  if (s.instagram) parts.push(`Instagram: ${s.instagram}`);
  if (s.twitter) parts.push(`Twitter: ${s.twitter}`);
  return parts.join(' | ');
}

async function gotoSiteStable(page, url) {
  try {
    await page.goto(url, { waitUntil: 'networkidle', timeout: SITE_GOTO_TIMEOUT_MS });
  } catch {
    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: SITE_GOTO_TIMEOUT_MS });
  }
}

/**
 * Google web search — visible browser; query biases away from Maps pack URLs.
 */
async function fetchGoogleUrls(page, keyword) {
  const q = encodeURIComponent(
    `${keyword} official website contact -site:google.com/maps -site:maps.google.com -site:goo.gl`,
  );
  const searchUrl = `https://www.google.com/search?q=${q}&num=20&hl=en`;
  console.log('Search (Google):', searchUrl.slice(0, 120) + '…');
  await page.goto(searchUrl, { waitUntil: 'domcontentloaded', timeout: SEARCH_GOTO_TIMEOUT_MS });
  await sleep(2000);
  const html = await page.content();
  if (/unusual traffic|captcha|consent/i.test(html) && /google/i.test(await page.url())) {
    console.warn('Google may be showing consent/captcha; rely on other engines or complete manually in browser.');
  }
  const $ = load(html);
  const urls = [];
  const seen = new Set();
  $('#search a[href^="http"], #rso a[href^="http"], div.g a[href^="http"]').each((_, el) => {
    const href = ($(el).attr('href') || '').trim();
    if (!href || seen.has(href)) return;
    if (href.includes('google.com/search') || href.includes('googleusercontent.com')) return;
    seen.add(href);
    urls.push(href);
  });
  return urls;
}

async function fetchDuckDuckGoUrls(page, keyword) {
  const q = encodeURIComponent(`${keyword} official website contact -maps`);
  const searchUrl = `https://html.duckduckgo.com/html/?q=${q}`;
  console.log('Search (DuckDuckGo):', searchUrl);
  await page.goto(searchUrl, { waitUntil: 'domcontentloaded', timeout: SEARCH_GOTO_TIMEOUT_MS });
  await sleep(1500);
  const html = await page.content();
  const $ = load(html);
  const urls = [];
  $('a.result__a').each((_, el) => {
    const href = ($(el).attr('href') || '').trim();
    if (href) urls.push(href);
  });
  return urls;
}

async function fetchBingUrls(page, keyword) {
  const q = encodeURIComponent(`${keyword} official company website contact`);
  const searchUrl = `https://www.bing.com/search?q=${q}`;
  console.log('Search (Bing fallback):', searchUrl);
  await page.goto(searchUrl, { waitUntil: 'domcontentloaded', timeout: SEARCH_GOTO_TIMEOUT_MS });
  await sleep(1500);
  const html = await page.content();
  const $ = load(html);
  const urls = [];
  $('li.b_algo h2 a').each((_, el) => {
    const href = ($(el).attr('href') || '').trim();
    if (href) urls.push(href);
  });
  return urls;
}

/** Prefer Google-order URLs first (already ranked), then DDG/Bing. */
function uniqueOriginsPrioritized(urlLists, limit) {
  const seen = new Set();
  const origins = [];
  for (const list of urlLists) {
    for (const raw of list) {
      if (isMapOrDirectoryUrl(raw)) continue;
      const origin = normalizeOriginUrl(raw);
      if (!origin) continue;
      const host = new URL(origin).hostname;
      if (seen.has(host)) continue;
      seen.add(host);
      origins.push(origin);
      if (origins.length >= limit * 4) return origins.slice(0, limit);
    }
  }
  return origins.slice(0, limit);
}

/**
 * Homepage + contact hunter: nav link, then /contact-us, /contact, /get-in-touch, /about-us, etc.
 * Merges emails/social from every successfully loaded page. Detects "Form Only" when no email.
 */
async function visitHomeAndContact(context, origin) {
  const emails = new Set();
  let social = { linkedin: '', facebook: '', instagram: '', twitter: '' };
  let siteName = '';
  let homeUrl = `${origin}/`;
  let homeHtml = '';
  let lastHtmlForFormCheck = '';
  let formOnly = false;

  const page = await context.newPage();
  try {
    console.log('  → homepage', homeUrl);
    await gotoSiteStable(page, homeUrl);
    await sleep(400);
    let html = await page.content();
    homeHtml = html;
    lastHtmlForFormCheck = html;
    siteName = extractSiteName(html) || cleanWebsiteName(new URL(origin).hostname.replace(/^www\./, ''));
    const homeData = mergeExtract(html);
    homeData.emails.forEach((e) => emails.add(e));
    social = mergeSocial(social, homeData);

    let contactUrl = null;
    try {
      const links = await page.$$eval('a[href]', (nodes) => {
        const out = [];
        for (const a of nodes) {
          const href = a.getAttribute('href') || '';
          const text = (a.textContent || '').toLowerCase();
          if (
            /contact|get in touch|reach us|enquiry|inquiry|connect/i.test(text) ||
            /\/contact|\/get-in-touch|\/about/i.test(href)
          ) {
            out.push(href);
          }
        }
        return out.slice(0, 20);
      });

      const originObj = new URL(origin);
      for (const href of links) {
        if (!href || href.startsWith('javascript') || href.startsWith('#')) continue;
        try {
          const abs = new URL(href, origin + '/').href;
          const u = new URL(abs);
          if (u.hostname.replace(/^www\./, '') !== originObj.hostname.replace(/^www\./, '')) continue;
          if (isMapOrDirectoryUrl(abs)) continue;
          if (/\/contact|\/get-in-touch|\/reach|\/enquir|\/about/i.test(u.pathname)) {
            contactUrl = `${u.origin}${u.pathname}${u.search}`;
            break;
          }
        } catch {
          /* skip */
        }
      }
    } catch {
      /* skip */
    }

    const pathCandidates = [
      contactUrl,
      `${origin}/contact-us`,
      `${origin}/contact`,
      `${origin}/get-in-touch`,
      `${origin}/about-us`,
      `${origin}/contact_us`,
      `${origin}/contactus`,
    ].filter(Boolean);

    const tried = new Set([homeUrl.replace(/\/$/, '') || homeUrl]);
    for (const candidate of pathCandidates) {
      const key = candidate.split('?')[0].replace(/\/$/, '') || candidate;
      if (tried.has(key)) continue;
      tried.add(key);
      try {
        console.log('  → contact', candidate);
        await gotoSiteStable(page, candidate);
        await sleep(400);
        html = await page.content();
        lastHtmlForFormCheck = html;
        const part = mergeExtract(html);
        part.emails.forEach((e) => emails.add(e));
        social = mergeSocial(social, part);
      } catch {
        console.warn('  skip contact path', candidate);
      }
    }

    const hasEmails = emails.size > 0;
    if (
      !hasEmails &&
      (detectContactFormOnly(homeHtml, false) || detectContactFormOnly(lastHtmlForFormCheck, false))
    ) {
      formOnly = true;
    }
  } finally {
    await page.close().catch(() => null);
  }

  return {
    websiteName: siteName,
    url: origin,
    emails: [...emails],
    social,
    formOnly,
  };
}

function buildLaunchOptions(customExecutable) {
  const launchOptions = {
    headless: HEADLESS,
    slowMo: 50,
    timeout: BROWSER_LAUNCH_TIMEOUT_MS,
    args: ['--disable-blink-features=AutomationControlled'],
  };
  if (customExecutable) {
    launchOptions.executablePath = customExecutable;
    console.log('executablePath:', customExecutable);
  }
  return launchOptions;
}

function escapeCsvField(s) {
  const v = String(s ?? '');
  if (/[",\n\r]/.test(v)) return `"${v.replace(/"/g, '""')}"`;
  return v;
}

function writeCsv(rows, filePath) {
  const headers = ['Website Name', 'URL', 'Email', 'Social Links'];
  const lines = [headers.join(',')];
  for (const r of rows) {
    let emailCol = (r.emails && r.emails.length) ? r.emails.join(';') : '';
    if (!emailCol && r.formOnly) emailCol = 'Form Only';
    const socialCol = formatSocialLinks(r.social || {});
    lines.push(
      [
        escapeCsvField(r.websiteName),
        escapeCsvField(r.url),
        escapeCsvField(emailCol),
        escapeCsvField(socialCol),
      ].join(','),
    );
  }
  fs.writeFileSync(filePath, lines.join('\n'), 'utf8');
}

async function main() {
  const args = parseArgs();
  const keyword = (args.keyword || '').trim();
  const limit = Math.max(1, parseInt(args.limit || '10', 10) || 10);
  const outputFile =
    args.output ||
    path.resolve(process.cwd(), 'output', 'deep_scrape_leads.csv');

  if (!keyword) {
    console.error(
      'Usage: node src/web_search_scraper/web_search_scraper.js --keyword "Real Estate Manchester" [--limit 10] [--output out.csv]',
    );
    process.exit(1);
  }

  const outDir = path.dirname(outputFile);
  if (!fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });

  const customExecutable = resolveChromiumExecutablePath();
  console.log(
    'Browser Launching... (headless:',
    HEADLESS,
    ') — networkidle @',
    SITE_GOTO_TIMEOUT_MS,
    'ms; browser restarts every',
    SITES_PER_BROWSER_BATCH,
    'sites',
  );

  const launchBase = buildLaunchOptions(customExecutable);
  const contextOptions = {
    userAgent:
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    locale: 'en-US',
  };

  /* --- Search phase: one browser, then close to free RAM --- */
  let searchBrowser = await chromium.launch(launchBase);
  let searchContext = await searchBrowser.newContext(contextOptions);
  const searchPage = await searchContext.newPage();
  let googleUrls = [];
  try {
    googleUrls = await fetchGoogleUrls(searchPage, keyword);
  } catch (e) {
    console.warn('Google search failed:', e.message || e);
  }
  let ddgUrls = await fetchDuckDuckGoUrls(searchPage, keyword);
  let bingUrls = [];
  if (ddgUrls.length < Math.min(5, limit)) {
    console.log('Augmenting with Bing results...');
    try {
      bingUrls = await fetchBingUrls(searchPage, keyword);
    } catch (e) {
      console.warn('Bing search failed:', e.message || e);
    }
  }
  await searchPage.close();
  await searchBrowser.close();
  console.log('Search phase finished; browser closed.');

  const targets = uniqueOriginsPrioritized([googleUrls, ddgUrls, bingUrls], limit);
  console.log(`Direct sites to visit: ${targets.length} (maps/listings filtered; limit ${limit})`);

  const rows = [];
  for (let start = 0; start < targets.length; start += SITES_PER_BROWSER_BATCH) {
    const batch = targets.slice(start, start + SITES_PER_BROWSER_BATCH);
    console.log(
      `\n--- Batch ${Math.floor(start / SITES_PER_BROWSER_BATCH) + 1}: ${batch.length} sites (new browser) ---`,
    );
    const visitBrowser = await chromium.launch(launchBase);
    const visitContext = await visitBrowser.newContext(contextOptions);

    for (let j = 0; j < batch.length; j += 1) {
      const i = start + j;
      if (i > 0) {
        const waitMs = randomInterSiteDelayMs();
        console.log(`Delay ${Math.round(waitMs / 100) / 10}s before next site...`);
        await sleep(waitMs);
      }
      const origin = batch[j];
      console.log(`[${i + 1}/${targets.length}]`, origin);
      try {
        const data = await visitHomeAndContact(visitContext, origin);
        rows.push({
          websiteName: data.websiteName,
          url: data.url,
          emails: data.emails,
          social: data.social,
          formOnly: data.formOnly,
        });
      } catch (err) {
        console.error('Site failed:', origin, err.message || err);
        rows.push({
          websiteName: '',
          url: origin,
          emails: [],
          social: { linkedin: '', facebook: '', instagram: '', twitter: '' },
          formOnly: false,
        });
      }
    }

    await visitBrowser.close();
    console.log(`Batch browser closed (${batch.length} sites processed).`);
  }

  writeCsv(rows, outputFile);
  console.log(`Saved ${rows.length} rows to ${outputFile}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
