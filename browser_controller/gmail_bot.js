import fs from 'fs';
import path from 'path';
import { chromium } from 'playwright';
import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';

const BASE_DIR = path.resolve();
const STORAGE_STATE = path.join(BASE_DIR, 'browser_controller', 'gmail_storage_state.json');
const SCREENSHOT_DIR = path.join(BASE_DIR, 'browser_controller', 'screenshots');

function randomDelay(min = 2000, max = 5000) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function interpolateTemplate(template, recipient) {
  return template
    .replace(/{{\s*name\s*}}/gi, recipient.name || '')
    .replace(/{{\s*website\s*}}/gi, recipient.website || '')
    .trim();
}

async function ensureDirectory(dir) {
  await fs.promises.mkdir(dir, { recursive: true });
}

async function saveStorageState(context) {
  await context.storageState({ path: STORAGE_STATE });
}

async function createContext(browser) {
  const opts = {
    headless: false,
  };
  if (fs.existsSync(STORAGE_STATE)) {
    opts.storageState = STORAGE_STATE;
  }
  return await browser.newContext(opts);
}

async function waitForLogin(page) {
  await page.goto('https://mail.google.com', { waitUntil: 'networkidle' });
  await wait(1500);

  const composeSelector = 'button[gh="cm"], button:has-text("Compose")';
  if (await page.locator(composeSelector).count()) {
    console.log('Already logged into Gmail.');
    return;
  }

  console.log('Please login to Gmail manually in the opened browser window.');
  await page.waitForSelector(composeSelector, { timeout: 0 });
  console.log('Login detected. Saving session state.');
}

async function checkForWarnings(page) {
  const warningText = page.locator('text=unusual activity');
  if (await warningText.count()) {
    throw new Error('Gmail unusual activity warning detected. Aborting.');
  }
}

async function openCompose(page) {
  const composeButton = page.locator('button:has-text("Compose"), [gh="cm"]').first();
  await composeButton.click();
  await page.waitForTimeout(1200 + randomDelay(0, 500));
}

async function fillEmail(page, recipient, subject, body, index) {
  const toField = page.locator('textarea[name="to"]');
  await toField.waitFor({ state: 'visible', timeout: 10000 });
  await toField.fill(recipient.email);
  await wait(randomDelay(800, 1300));

  const subjectBox = page.locator('input[name="subjectbox"]');
  await subjectBox.fill(subject);
  await wait(randomDelay(800, 1300));

  let bodyFrame = page.locator('div[aria-label="Message Body"]');
  if (!(await bodyFrame.count())) {
    bodyFrame = page.locator('div[role="textbox"]');
  }
  await bodyFrame.first().fill(body);
  await wait(randomDelay(800, 1300));

  await ensureDirectory(SCREENSHOT_DIR);
  await page.screenshot({ path: path.join(SCREENSHOT_DIR, `compose-${index + 1}.png`) });
}

async function sendMessage(page, index) {
  const sendButton = page.locator('div[role="button"][aria-label*="Send"], div[role="button"][data-tooltip*="Send"]');
  await sendButton.first().click();
  await page.waitForTimeout(2500 + randomDelay(0, 1500));
  await page.screenshot({ path: path.join(SCREENSHOT_DIR, `sent-${index + 1}.png`) });
  await page.keyboard.press('Escape');
}

async function sendBatchEmails(credentials, emailList, template, subject) {
  if (!Array.isArray(emailList) || emailList.length === 0) {
    throw new Error('Email list is empty.');
  }

  const browser = await chromium.launch({ headless: false, args: ['--start-maximized'], slowMo: 50 });
  const context = await createContext(browser);
  const page = await context.newPage();
  await page.setViewportSize({ width: 1600, height: 1000 });

  await waitForLogin(page);
  await saveStorageState(context);
  await checkForWarnings(page);

  const batches = [];
  for (let i = 0; i < emailList.length; i += 3) {
    batches.push(emailList.slice(i, i + 3));
  }

  let sentCount = 0;
  for (let batchIndex = 0; batchIndex < batches.length; batchIndex += 1) {
    const batch = batches[batchIndex];
    console.log(`Sending batch ${batchIndex + 1}/${batches.length} with ${batch.length} message(s).`);
    for (let emailIndex = 0; emailIndex < batch.length; emailIndex += 1) {
      const recipient = batch[emailIndex];
      const body = interpolateTemplate(template, recipient);
      console.log(`Sending email ${sentCount + 1}/${emailList.length} to ${recipient.email}`);

      await openCompose(page);
      await fillEmail(page, recipient, subject, body, sentCount);
      await sendMessage(page, sentCount);
      sentCount += 1;
      await wait(randomDelay(2000, 5000));
      await checkForWarnings(page);
    }

    console.log(`Completed batch ${batchIndex + 1}/${batches.length}.`);
    if (batchIndex < batches.length - 1) {
      await wait(randomDelay(3000, 6000));
    }
  }

  await saveStorageState(context);
  await browser.close();
  console.log(`Finished sending ${sentCount} email(s).`);
  return { sent: sentCount };
}

async function run() {
  const argv = yargs(hideBin(process.argv))
    .option('payload', {
      type: 'string',
      describe: 'Path to JSON payload file containing emails, template, and subject',
      demandOption: true,
    })
    .help()
    .parse();

  const payloadPath = path.resolve(argv.payload);
  if (!fs.existsSync(payloadPath)) {
    throw new Error(`Payload file not found: ${payloadPath}`);
  }

  const payloadText = await fs.promises.readFile(payloadPath, 'utf8');
  const payload = JSON.parse(payloadText);
  const emailList = payload.emails.map((item) => ({
    email: String(item.email || '').trim(),
    name: item.name || '',
    website: item.website || '',
  })).filter((item) => item.email);

  const result = await sendBatchEmails(payload.credentials || {}, emailList, payload.template || '', payload.subject || '');
  console.log(JSON.stringify(result));
}

run().catch((error) => {
  console.error(`ERROR: ${error.message || error}`);
  process.exit(1);
});
