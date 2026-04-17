/**
 * run-pipeline.ts — Milestone 1 end-to-end walking skeleton.
 *
 * Bible §12 Milestone 1 exit criterion:
 *   "see real Dice jobs classified by hard filter. Some pass, some reject.
 *    Eyeball check — does it look right?"
 *
 * What this does:
 *   1. Spawns: python -m scraper --source dice --max 20
 *   2. Finds the JSONL written to scraper/output/
 *   3. For each job: sanitizeJob → hardFilter → postFetchChecks
 *   4. Prints per-job verdict + summary breakdown
 *
 * Run from project root:
 *   npx tsx job-filter/scripts/run-pipeline.ts
 *
 * Options (env vars):
 *   SOURCE=linkedin          default: dice
 *   MAX=50                   default: 20
 *   HEADED=1                 show browser window (Playwright sources)
 *   JSONL=/path/to/file      skip scrape, read existing JSONL directly
 */

import { spawnSync }    from "child_process";
import * as fs          from "fs";
import * as path        from "path";
import * as readline    from "readline";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

import { hardFilter }      from "../src/filter";
import { postFetchChecks } from "../src/post-fetch";
import { sanitizeJob }     from "../src/sanitize";
import { validateProfile } from "../src/validate";

// ---------------------------------------------------------------------------
// Paths — derived from __dirname so they work regardless of cwd
// ---------------------------------------------------------------------------

// job-filter/scripts/ → job-filter/
const JOB_FILTER_DIR  = path.resolve(__dirname, "..");
// job-filter/ → project root
const PROJECT_ROOT    = path.resolve(JOB_FILTER_DIR, "..");
// scraper output lives at project root level
const SCRAPER_OUT_DIR = path.join(PROJECT_ROOT, "scraper", "output");
// profile.json lives inside job-filter/ per bible §13
const PROFILE_PATH    = path.join(JOB_FILTER_DIR, "profile.json");

// ---------------------------------------------------------------------------
// Config from env
// ---------------------------------------------------------------------------

const SOURCE         = process.env.SOURCE ?? "dice";
const MAX_JOBS       = parseInt(process.env.MAX ?? "20", 10);
const HEADED         = Boolean(process.env.HEADED);
const JSONL_OVERRIDE = process.env.JSONL  ?? "";

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<void> {

  // 1. Load + validate profile
  if (!fs.existsSync(PROFILE_PATH)) {
    die(
      `Profile not found at ${PROFILE_PATH}\n` +
      `Expected: job-filter/profile.json\n` +
      `If your profile is named profile-v2.json, copy it:\n` +
      `  cp profile-v2.json job-filter/profile.json`
    );
  }

  const profile = JSON.parse(fs.readFileSync(PROFILE_PATH, "utf-8"));

  try {
    validateProfile(profile);
  } catch (err) {
    die(`Profile validation failed: ${err}`);
  }

  log(`Profile loaded: ${profile.meta?.profile_id ?? "unknown"}`);

  // 2. Scrape (or use existing JSONL)
  const jsonlPath = JSONL_OVERRIDE
    ? JSONL_OVERRIDE
    : runScraper(SOURCE, MAX_JOBS, HEADED);

  if (!fs.existsSync(jsonlPath)) {
    die(`JSONL not found: ${jsonlPath}`);
  }

  log(`Reading: ${jsonlPath}`);

  // 3. Filter each job
  const nowIso  = new Date().toISOString();
  const results = await processJobs(jsonlPath, profile, nowIso);

  // 4. Print results + summary
  printResults(results, SOURCE);
}

// ---------------------------------------------------------------------------
// Scraper spawn
// ---------------------------------------------------------------------------

function runScraper(source: string, maxJobs: number, headed: boolean): string {
  const args = [
    "-m", "scraper",
    "--source", source,
    "--max",    String(maxJobs),
    ...(headed ? ["--headed"] : []),
  ];

  log(`Spawning: python ${args.join(" ")}`);

  const result = spawnSync("python", args, {
    cwd:      PROJECT_ROOT,
    encoding: "utf-8",
    stdio:    ["ignore", "pipe", "pipe"],
  });

  // Echo scraper stderr so progress is visible during scrape
  if (result.stderr) process.stderr.write(result.stderr);

  if (result.status === 2) {
    die(
      `Cookie file missing.\n` +
      `Save browser cookies to: config/cookies/${source}.json\n` +
      `See instructions.md Step 0.`
    );
  }
  if (result.status !== 0) {
    die(`Scraper exited with code ${result.status}`);
  }

  // Find the newest JSONL written for this source
  const jsonlPath = findNewestJsonl(source);
  if (!jsonlPath) {
    die(
      `No JSONL found in ${SCRAPER_OUT_DIR} for source "${source}".\n` +
      `Scraper may have written 0 jobs or output dir does not exist.`
    );
  }

  return jsonlPath!;
}

function findNewestJsonl(source: string): string | null {
  if (!fs.existsSync(SCRAPER_OUT_DIR)) return null;

  const files = fs
    .readdirSync(SCRAPER_OUT_DIR)
    .filter(f => f.startsWith(`${source}_`) && f.endsWith(".jsonl"))
    .map(f => ({
      name:  f,
      mtime: fs.statSync(path.join(SCRAPER_OUT_DIR, f)).mtimeMs,
    }))
    .sort((a, b) => b.mtime - a.mtime);

  return files.length ? path.join(SCRAPER_OUT_DIR, files[0].name) : null;
}

// ---------------------------------------------------------------------------
// Job processing — the pipeline
// ---------------------------------------------------------------------------

interface JobResult {
  title:   string;
  company: string;
  verdict: string;
  reason:  string | null;
  flags:   string[];
}

async function processJobs(
  jsonlPath: string,
  profile: unknown,
  nowIso: string,
): Promise<JobResult[]> {
  const results: JobResult[] = [];

  const rl = readline.createInterface({
    input:     fs.createReadStream(jsonlPath, "utf-8"),
    crlfDelay: Infinity,
  });

  for await (const line of rl) {
    const trimmed = line.trim();
    if (!trimmed) continue;

    let raw: any;
    try {
      raw = JSON.parse(trimmed);
    } catch {
      log(`Skipping malformed line: ${trimmed.slice(0, 80)}`);
      continue;
    }

    // Stage 1 — sanitize (clamps suspect source_score values, per bible §5 stage 3)
    const sanitized = sanitizeJob(raw);

    // Stage 2 — hard filter (deterministic rules, per bible §5 stage 4)
    const filterResult = hardFilter(sanitized, profile as any);

    // Stage 3 — post-fetch checks (per bible §5 stage 6)
    // description_raw is "" at scrape time so this mostly flags posted_at_missing
    const checked = postFetchChecks(sanitized, nowIso);

    // Merge flags from all three stages, dedup
    const allFlags = [
      ...(sanitized.meta?.flags ?? []),
      ...(filterResult.flags    ?? []),
      ...checked,
    ];

    results.push({
      title:   sanitized.title         ?? "",
      company: sanitized.company?.name ?? "",
      verdict: filterResult.verdict,
      reason:  filterResult.reason     ?? null,
      flags:   [...new Set(allFlags)],
    });
  }

  return results;
}

// ---------------------------------------------------------------------------
// Output
// ---------------------------------------------------------------------------

function printResults(results: JobResult[], source: string): void {
  const SEP = "─".repeat(90);

  console.log(`\n${SEP}`);
  console.log(`  ${source.toUpperCase()} — ${results.length} jobs processed`);
  console.log(SEP);

  for (const r of results) {
    const icon    = r.verdict === "PASS" ? "✓" : "✗";
    const title   = pad(r.title,   48);
    const company = pad(r.company, 26);
    const detail  = r.verdict === "REJECT"
      ? `REJECT  ${r.reason ?? ""}`
      : "PASS";
    const flags   = r.flags.length ? `  [${r.flags.join(", ")}]` : "";
    console.log(`  ${icon}  ${title}  ${company}  ${detail}${flags}`);
  }

  // Summary
  const passed   = results.filter(r => r.verdict === "PASS");
  const rejected = results.filter(r => r.verdict === "REJECT");

  console.log(`\n${SEP}`);
  console.log(`  SUMMARY`);
  console.log(SEP);
  console.log(`  Total    ${results.length}`);
  console.log(`  Passed   ${passed.length}`);
  console.log(`  Rejected ${rejected.length}`);

  if (rejected.length) {
    console.log(`\n  Reject reasons:`);
    for (const [reason, count] of tally(rejected.map(r => r.reason ?? "unknown"))) {
      console.log(`    ${String(count).padStart(3)}x  ${reason}`);
    }
  }

  const allFlags = results.flatMap(r => r.flags);
  if (allFlags.length) {
    console.log(`\n  Flags:`);
    for (const [flag, count] of tally(allFlags)) {
      console.log(`    ${String(count).padStart(3)}x  ${flag}`);
    }
  }

  console.log(`${SEP}\n`);
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function tally(items: string[]): [string, number][] {
  const map: Record<string, number> = {};
  for (const item of items) map[item] = (map[item] ?? 0) + 1;
  return Object.entries(map).sort((a, b) => b[1] - a[1]);
}

function pad(s: string, len: number): string {
  if (!s) s = "";
  return s.length > len ? s.slice(0, len - 1) + "…" : s.padEnd(len);
}

function log(msg: string): void {
  process.stderr.write(`[pipeline] ${msg}\n`);
}

function die(msg: string): never {
  process.stderr.write(`[pipeline] ERROR: ${msg}\n`);
  process.exit(1);
}

// ---------------------------------------------------------------------------

main().catch(err => {
  process.stderr.write(`[pipeline] Unhandled error: ${err}\n`);
  process.exit(1);
});