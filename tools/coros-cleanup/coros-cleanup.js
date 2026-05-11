#!/usr/bin/env node
/*
 * COROS Training Hub cleanup CLI.
 *
 * A shareable, browser-agnostic helper that opens a real browser, asks the user
 * to log in, performs a dry-run, writes backups, and only deletes after an
 * explicit confirmation phrase.
 */

const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const readline = require("node:readline/promises");
const { stdin: input, stdout: output } = require("node:process");
const { spawn } = require("node:child_process");

const APP_URL = "https://t.coros.com/admin/views/activities";
const DEFAULT_PORT = 9222;
const DEFAULT_PAGE_SIZE = 100;
const DEFAULT_DELAY_MS = 650;
const DATA_DIR = path.resolve(process.cwd(), "coros-cleanup-data");

const REGION_CONFIG = {
  1: { teamapi: "https://teamapi.coros.com", token: "CPL-coros-token" },
  2: { teamapi: "https://teamcnapi.coros.com", token: "CPL-coros-token" },
  3: { teamapi: "https://teameuapi.coros.com", token: "CPL-coros-token" },
  4: { teamapi: "https://teamsgapi.coros.com", token: "CPL-coros-token" },
  101: { teamapi: "https://teamapitest.coros.com", token: "CPL-test-coros-token" },
  102: { teamapi: "https://teamcnapitest.coros.com", token: "CPL-test-coros-token" },
  103: { teamapi: "https://teameuapitest.coros.com", token: "CPL-test-coros-token" },
  104: { teamapi: "https://teamsgapitest.coros.com", token: "CPL-test-coros-token" },
};

const BROWSERS = {
  edge: {
    name: "Microsoft Edge",
    win: [
      "C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe",
      "C:\\Program Files\\Microsoft\\Edge\\Application\\msedge.exe",
    ],
    darwin: ["/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge"],
    linux: ["microsoft-edge", "microsoft-edge-stable"],
  },
  chrome: {
    name: "Google Chrome",
    win: [
      "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",
      "C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe",
    ],
    darwin: ["/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"],
    linux: ["google-chrome", "google-chrome-stable", "chromium", "chromium-browser"],
  },
  brave: {
    name: "Brave",
    win: [
      "C:\\Program Files\\BraveSoftware\\Brave-Browser\\Application\\brave.exe",
      "C:\\Program Files (x86)\\BraveSoftware\\Brave-Browser\\Application\\brave.exe",
    ],
    darwin: ["/Applications/Brave Browser.app/Contents/MacOS/Brave Browser"],
    linux: ["brave-browser", "brave"],
  },
};

function usage() {
  return [
    "COROS Training Hub cleanup CLI",
    "",
    "Interactive:",
    "  node coros-cleanup.js",
    "",
    "Non-interactive:",
    "  node coros-cleanup.js dry-run --browser edge --cutoff 2025-09-04",
    "  node coros-cleanup.js delete --from-backup coros-cleanup-data/dry-run-...json --confirm \"DELETE ...\"",
    "  node coros-cleanup.js history --browser chrome",
    "",
    "Options:",
    "  --browser edge|chrome|brave",
    "  --cutoff YYYY-MM-DD       Delete candidates are strictly before this date.",
    "  --mode before-date|duplicates-before-date",
    "  --port N                  Default: 9222.",
    "  --page-size N             Default: 100.",
    "  --delay-ms N              Default: 650.",
    "  --from-backup FILE        Required for delete.",
    "  --confirm TEXT            Required for delete.",
    "  --limit N                 Optional delete cap for testing.",
    "",
    "Safety:",
    "  dry-run writes a full JSON backup and never deletes.",
    "  delete only uses records from a backup and requires the exact confirmation phrase.",
  ].join("\n");
}

function parseArgs(argv) {
  const args = {
    command: "wizard",
    browser: "",
    cutoff: "",
    mode: "before-date",
    port: DEFAULT_PORT,
    pageSize: DEFAULT_PAGE_SIZE,
    delayMs: DEFAULT_DELAY_MS,
    fromBackup: "",
    confirm: "",
    limit: 0,
  };

  if (argv[2] && !argv[2].startsWith("-")) {
    args.command = argv[2];
  }
  for (let i = args.command === "wizard" ? 2 : 3; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--browser") args.browser = argv[++i];
    else if (arg === "--cutoff") args.cutoff = argv[++i];
    else if (arg === "--mode") args.mode = argv[++i];
    else if (arg === "--port") args.port = Number(argv[++i]);
    else if (arg === "--page-size") args.pageSize = Number(argv[++i]);
    else if (arg === "--delay-ms") args.delayMs = Number(argv[++i]);
    else if (arg === "--from-backup") args.fromBackup = argv[++i];
    else if (arg === "--confirm") args.confirm = argv[++i];
    else if (arg === "--limit") args.limit = Number(argv[++i]);
    else if (arg === "--help" || arg === "-h") args.command = "help";
    else throw new Error(`Unknown argument: ${arg}`);
  }
  return args;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function ensureDataDir() {
  fs.mkdirSync(DATA_DIR, { recursive: true });
}

function stamp() {
  return new Date().toISOString().replace(/[:.]/g, "-");
}

function resolveExecutable(browserKey) {
  const browser = BROWSERS[browserKey];
  if (!browser) throw new Error(`Unsupported browser: ${browserKey}`);
  const platform = process.platform === "win32" ? "win" : process.platform;
  const candidates = browser[platform] || [];
  for (const candidate of candidates) {
    if (path.isAbsolute(candidate) && fs.existsSync(candidate)) return candidate;
    if (!path.isAbsolute(candidate)) return candidate;
  }
  throw new Error(`${browser.name} executable not found. Install it or choose another browser.`);
}

async function fetchJson(url, options) {
  const response = await fetch(url, options);
  if (!response.ok) throw new Error(`HTTP ${response.status} for ${url}`);
  return response.json();
}

async function isDebugPortReady(port) {
  try {
    await fetchJson(`http://127.0.0.1:${port}/json/version`);
    return true;
  } catch {
    return false;
  }
}

function launchBrowser(browserKey, port) {
  const exe = resolveExecutable(browserKey);
  const profileDir = path.resolve(DATA_DIR, `browser-profile-${browserKey}`);
  fs.mkdirSync(profileDir, { recursive: true });
  const child = spawn(exe, [
    `--remote-debugging-port=${port}`,
    `--user-data-dir=${profileDir}`,
    "--no-first-run",
    "--no-default-browser-check",
    APP_URL,
  ], {
    detached: true,
    stdio: "ignore",
  });
  child.unref();
  return profileDir;
}

async function ensureBrowser(browserKey, port) {
  if (await isDebugPortReady(port)) return { launched: false, profileDir: "" };
  const profileDir = launchBrowser(browserKey, port);
  for (let i = 0; i < 30; i += 1) {
    if (await isDebugPortReady(port)) return { launched: true, profileDir };
    await sleep(500);
  }
  throw new Error(`Browser DevTools port ${port} did not become ready.`);
}

async function connectPage(port) {
  const targets = await fetchJson(`http://127.0.0.1:${port}/json/list`);
  let page = targets.find((target) => target.type === "page" && target.url.includes("t.coros.com"));
  if (!page) {
    await fetchJson(`http://127.0.0.1:${port}/json/new?${encodeURIComponent(APP_URL)}`);
    await sleep(1500);
    const refreshed = await fetchJson(`http://127.0.0.1:${port}/json/list`);
    page = refreshed.find((target) => target.type === "page" && target.url.includes("t.coros.com"));
  }
  if (!page || !page.webSocketDebuggerUrl) {
    throw new Error("COROS page target not found. Open the COROS Training Hub activities page in the launched browser.");
  }

  const ws = new WebSocket(page.webSocketDebuggerUrl);
  let id = 0;
  const pending = new Map();
  ws.onmessage = (event) => {
    const message = JSON.parse(event.data);
    if (!message.id || !pending.has(message.id)) return;
    const { resolve, reject } = pending.get(message.id);
    pending.delete(message.id);
    if (message.error) reject(new Error(JSON.stringify(message.error)));
    else resolve(message.result);
  };
  await new Promise((resolve, reject) => {
    ws.onopen = resolve;
    ws.onerror = reject;
  });

  function send(method, params = {}) {
    const message = { id: ++id, method, params };
    ws.send(JSON.stringify(message));
    return new Promise((resolve, reject) => pending.set(message.id, { resolve, reject }));
  }
  return { ws, send };
}

async function readBrowserState(port) {
  const { ws, send } = await connectPage(port);
  try {
    await send("Network.enable");
    const cookieResult = await send("Network.getCookies", {
      urls: [
        APP_URL,
        "https://teamapi.coros.com",
        "https://teamcnapi.coros.com",
        "https://teameuapi.coros.com",
        "https://teamsgapi.coros.com",
      ],
    });
    const evalResult = await send("Runtime.evaluate", {
      expression: "({href: location.href, title: document.title, readyState: document.readyState})",
      returnByValue: true,
    });
    return {
      page: evalResult.result.value,
      cookies: cookieResult.cookies || [],
    };
  } finally {
    ws.close();
  }
}

function buildClient(cookies) {
  const cookieMap = new Map(cookies.map((cookie) => [cookie.name, cookie.value]));
  const regionId = Number(cookieMap.get("CPL-coros-region") || 1);
  const config = REGION_CONFIG[regionId] || REGION_CONFIG[1];
  const accessToken = cookieMap.get(config.token);
  if (!accessToken) {
    throw new Error(`Not logged in or token missing. Could not find ${config.token} for region ${regionId}.`);
  }
  const cookieHeader = cookies
    .filter((cookie) => cookie.domain && cookie.domain.includes("coros.com"))
    .map((cookie) => `${cookie.name}=${cookie.value}`)
    .join("; ");

  async function request(apiPath, params = {}, userId) {
    const url = new URL(apiPath, config.teamapi);
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== "") url.searchParams.set(key, String(value));
    });
    const headers = { accessToken, Cookie: cookieHeader };
    if (userId) headers.YFHeader = JSON.stringify({ userId });
    const response = await fetch(url.toString(), { method: "GET", headers });
    const text = await response.text();
    let data;
    try {
      data = JSON.parse(text);
    } catch {
      data = text;
    }
    if (!response.ok) throw new Error(`HTTP ${response.status}: ${String(text).slice(0, 300)}`);
    if (data && data.result && data.result !== "0000") {
      throw new Error(`COROS API result ${data.result}: ${data.message || String(text).slice(0, 300)}`);
    }
    return data.data || data;
  }

  return { regionId, request };
}

function compactDate(text) {
  return String(text).replaceAll("-", "");
}

function dashedDate(text) {
  const value = String(text);
  return `${value.slice(0, 4)}-${value.slice(4, 6)}-${value.slice(6, 8)}`;
}

function activityDate(activity) {
  if (activity.happenDay) return dashedDate(activity.happenDay);
  if (activity.startDay) return dashedDate(activity.startDay);
  if (!activity.startTime) return "";
  const date = new Date(Number(activity.startTime) * 1000);
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function summarizeRecord(record) {
  return {
    labelId: record.labelId,
    date: activityDate(record),
    name: record.name || "",
    sportType: record.sportType,
    distance: record.distance,
    workoutTime: record.workoutTime,
    startTime: record.startTime,
    startTimezone: record.startTimezone,
  };
}

async function getClientFromBrowser(args) {
  await ensureBrowser(args.browser, args.port);
  const state = await readBrowserState(args.port);
  const client = buildClient(state.cookies);
  const account = await client.request("/account/query");
  if (!account.userId) throw new Error("Logged in account could not be verified.");
  return { ...client, account, page: state.page };
}

async function fetchActivities(client, account, { cutoff, pageSize, delayMs }) {
  const endDay = Number(compactDate(cutoff)) - 1;
  const records = [];
  let pageNumber = 1;
  let totalPage = 1;
  let count = 0;
  do {
    const page = await client.request("/activity/query", { size: pageSize, pageNumber, endDay }, account.userId);
    const dataList = page.dataList || [];
    records.push(...dataList);
    totalPage = Number(page.totalPage || totalPage || 1);
    count = Number(page.count || count || records.length);
    pageNumber += 1;
    if (pageNumber <= totalPage) await sleep(delayMs);
  } while (pageNumber <= totalPage);
  return { records, serverCount: count, endDay };
}

function filterByMode(records, mode) {
  if (mode === "before-date") return records;
  if (mode !== "duplicates-before-date") throw new Error(`Unsupported mode: ${mode}`);

  const groups = new Map();
  for (const record of records) {
    const key = String(record.startTime || "");
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(record);
  }
  const selected = [];
  for (const group of groups.values()) {
    if (group.length < 2) continue;
    selected.push(...group);
  }
  return selected;
}

function makeStats(records) {
  const dates = records.map(activityDate).filter(Boolean).sort();
  const byYear = {};
  const byGroupSize = {};
  const groups = new Map();
  for (const record of records) {
    const date = activityDate(record);
    if (date) byYear[date.slice(0, 4)] = (byYear[date.slice(0, 4)] || 0) + 1;
    const key = String(record.startTime || "");
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(record);
  }
  for (const group of groups.values()) {
    byGroupSize[group.length] = (byGroupSize[group.length] || 0) + 1;
  }
  return {
    count: records.length,
    minDate: dates[0] || "",
    maxDate: dates[dates.length - 1] || "",
    byYear,
    groups: groups.size,
    byGroupSize,
  };
}

async function dryRun(args) {
  const { account, regionId, request } = await getClientFromBrowser(args);
  const fetched = await fetchActivities({ request }, account, args);
  const selected = filterByMode(fetched.records, args.mode);
  const stats = makeStats(selected);
  const plan = {
    tool: "coros-cleanup",
    version: 1,
    createdAt: new Date().toISOString(),
    mode: args.mode,
    cutoff: args.cutoff,
    serverEndDay: fetched.endDay,
    regionId,
    userId: account.userId,
    email: account.email || "",
    fetchedCount: fetched.records.length,
    selectedCount: selected.length,
    stats,
    sample: selected.slice(0, 20).map(summarizeRecord),
    records: selected,
    confirmText: `DELETE ${selected.length} COROS ACTIVITIES BEFORE ${args.cutoff}`,
  };

  ensureDataDir();
  const file = path.resolve(DATA_DIR, `dry-run-${args.mode}-before-${args.cutoff}-${stamp()}.json`);
  fs.writeFileSync(file, JSON.stringify(plan, null, 2), "utf8");
  printDryRun(plan, file);
  return { plan, file };
}

function printDryRun(plan, file) {
  console.log("");
  console.log(`Dry-run mode: ${plan.mode}`);
  console.log(`Matched: ${plan.selectedCount}`);
  console.log(`Date range: ${plan.stats.minDate || "n/a"} to ${plan.stats.maxDate || "n/a"}`);
  console.log(`By year: ${JSON.stringify(plan.stats.byYear)}`);
  console.log(`Group sizes by startTime: ${JSON.stringify(plan.stats.byGroupSize)}`);
  console.table(plan.sample);
  console.log(`Backup: ${file}`);
  console.log(`Confirmation phrase: ${plan.confirmText}`);
  console.log("");
}

async function deleteFromBackup(args) {
  if (!args.fromBackup) throw new Error("--from-backup is required for delete.");
  const backupPath = path.resolve(args.fromBackup);
  const plan = JSON.parse(fs.readFileSync(backupPath, "utf8"));
  if (!Array.isArray(plan.records)) throw new Error("Backup has no records array.");
  if (!plan.confirmText || args.confirm !== plan.confirmText) {
    throw new Error(`Confirmation mismatch. Expected exactly: ${plan.confirmText}`);
  }

  const { account, request } = await getClientFromBrowser(args);
  const records = args.limit ? plan.records.slice(0, args.limit) : plan.records;
  const out = path.resolve(DATA_DIR, `delete-log-before-${plan.cutoff}-${stamp()}.json`);
  const ndjson = `${out}.ndjson`;
  fs.writeFileSync(ndjson, "", "utf8");

  const log = [];
  for (let i = 0; i < records.length; i += 1) {
    const record = records[i];
    const entryBase = { index: i, ...summarizeRecord(record) };
    try {
      const result = await request("/activity/delete", { labelId: record.labelId }, account.userId);
      const entry = { ok: true, ...entryBase, result };
      log.push(entry);
      fs.appendFileSync(ndjson, `${JSON.stringify(entry)}\n`, "utf8");
    } catch (error) {
      const entry = { ok: false, ...entryBase, error: String(error && error.message ? error.message : error) };
      log.push(entry);
      fs.appendFileSync(ndjson, `${JSON.stringify(entry)}\n`, "utf8");
      fs.writeFileSync(out, JSON.stringify({ ok: false, backup: backupPath, attempted: log.length, deleted: log.filter((x) => x.ok).length, log }, null, 2), "utf8");
      throw error;
    }
    if ((i + 1) % 25 === 0 || i + 1 === records.length) {
      console.log(`Deleted ${i + 1}/${records.length}`);
    }
    if (i + 1 < records.length) await sleep(args.delayMs);
  }

  fs.writeFileSync(out, JSON.stringify({
    ok: true,
    backup: backupPath,
    confirmText: plan.confirmText,
    attempted: log.length,
    deleted: log.filter((x) => x.ok).length,
    log,
  }, null, 2), "utf8");
  console.log(`Delete complete. Deleted ${log.length}/${records.length}.`);
  console.log(`Log: ${out}`);
}

async function history(args) {
  const { account, regionId, request } = await getClientFromBrowser(args);
  const now = new Date();
  const futureCutoff = `${now.getFullYear() + 1}-01-01`;
  const fetched = await fetchActivities({ request }, account, {
    cutoff: futureCutoff,
    pageSize: args.pageSize,
    delayMs: args.delayMs,
  });
  const stats = makeStats(fetched.records);
  console.log("");
  console.log(`Account: ${account.email || account.userId}`);
  console.log(`Region: ${regionId}`);
  console.log(`Activities before ${futureCutoff}: ${fetched.records.length}`);
  console.log(`Date range: ${stats.minDate || "n/a"} to ${stats.maxDate || "n/a"}`);
  console.log(`By year: ${JSON.stringify(stats.byYear)}`);
  console.log(`Group sizes by startTime: ${JSON.stringify(stats.byGroupSize)}`);
}

async function askChoice(rl, question, choices, fallback) {
  const labels = choices.map((choice, index) => `${index + 1}. ${choice.label}`).join("\n");
  const answer = (await rl.question(`${question}\n${labels}\n> `)).trim();
  if (!answer) return fallback;
  const index = Number(answer);
  if (Number.isInteger(index) && index >= 1 && index <= choices.length) return choices[index - 1].value;
  const match = choices.find((choice) => choice.value === answer || choice.label.toLowerCase() === answer.toLowerCase());
  if (match) return match.value;
  return fallback;
}

async function wizard(args) {
  const rl = readline.createInterface({ input, output });
  try {
    console.log("COROS Training Hub cleanup wizard");
    console.log("This tool opens a browser, asks you to log in, runs a dry-run, and requires explicit confirmation before deletion.");
    args.browser = await askChoice(rl, "Choose a browser:", [
      { label: "Microsoft Edge", value: "edge" },
      { label: "Google Chrome", value: "chrome" },
      { label: "Brave", value: "brave" },
    ], "edge");

    await ensureBrowser(args.browser, args.port);
    console.log("");
    console.log("A browser window should be open. Log in to COROS Training Hub if needed.");
    await rl.question("After the activities page is loaded, press Enter to continue...");

    await history(args);
    console.log("");
    args.mode = await askChoice(rl, "Choose deletion mode:", [
      { label: "All activities before a date", value: "before-date" },
      { label: "Only duplicate startTime groups before a date", value: "duplicates-before-date" },
    ], "before-date");
    args.cutoff = (await rl.question("Delete candidates strictly before date (YYYY-MM-DD): ")).trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(args.cutoff)) throw new Error("Invalid cutoff date.");

    const { plan, file } = await dryRun(args);
    if (!plan.selectedCount) return;
    const proceed = (await rl.question(`Type the exact phrase to delete, or press Enter to stop:\n${plan.confirmText}\n> `)).trim();
    if (proceed !== plan.confirmText) {
      console.log("Stopped. No deletion was performed.");
      return;
    }
    args.fromBackup = file;
    args.confirm = proceed;
    await deleteFromBackup(args);
  } finally {
    rl.close();
  }
}

async function main() {
  const args = parseArgs(process.argv);
  if (args.command === "help") {
    console.log(usage());
    return;
  }
  ensureDataDir();
  if (args.command === "wizard") {
    await wizard(args);
  } else if (args.command === "dry-run") {
    if (!args.browser) args.browser = "edge";
    if (!args.cutoff) throw new Error("--cutoff is required for dry-run.");
    await ensureBrowser(args.browser, args.port);
    await dryRun(args);
  } else if (args.command === "delete") {
    if (!args.browser) args.browser = "edge";
    await ensureBrowser(args.browser, args.port);
    await deleteFromBackup(args);
  } else if (args.command === "history") {
    if (!args.browser) args.browser = "edge";
    await ensureBrowser(args.browser, args.port);
    await history(args);
  } else {
    throw new Error(`Unknown command: ${args.command}`);
  }
}

main().catch((error) => {
  console.error(error && error.stack ? error.stack : error);
  process.exitCode = 1;
});
