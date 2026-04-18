/**
 * run-pipeline.ts — Milestone 2 end-to-end pipeline.
 *
 * Bible §5 stages wired in order:
 *   Stage 3  — sanitizeJob
 *   Stage 4  — hardFilter       (REJECTs dropped here)
 *   Stage 5  — fetchJobPage     (PASS jobs only)
 *   Stage 6  — postFetchChecks  (with real description_raw)
 *   Stage 7  — extract          (LLM → structured fields)
 *   Stage 10 — normalizeSkills  (alias map lookup)
 *
 * Run from project root:
 *   npx tsx job-filter/scripts/run-pipeline.ts
 *
 * Options (env vars):
 *   SOURCE=linkedin     default: dice
 *   MAX=50              default: 20
 *   HEADED=1            show browser window (Playwright sources)
 *   JSONL=/path/file    skip scrape, read existing JSONL directly
 *   EXTRACT=1           enable LLM extraction (default: off — costs API calls)
 */

import "dotenv/config";

import { spawnSync }     from "child_process";
import * as fs           from "fs";
import * as path         from "path";
import * as readline     from "readline";
import { fileURLToPath } from "url";
import { config } from "dotenv";;

import { hardFilter }      from "../src/filter";
import { postFetchChecks } from "../src/post-fetch";
import { sanitizeJob }     from "../src/sanitize";
import { validateProfile } from "../src/validate";
import { normalizeSkill, buildAliasMap } from "../src/skills";

import { fetchJobPage }  from "../../fetcher/src/fetch";
import { extract }       from "../../extractor/src/extract";


// ---------------------------------------------------------------------------
// Paths
// ---------------------------------------------------------------------------

const __filename       = fileURLToPath(import.meta.url);
const __dirname_compat = path.dirname(__filename);

const JOB_FILTER_DIR  = path.resolve(__dirname_compat, "..");
const PROJECT_ROOT    = path.resolve(JOB_FILTER_DIR, "..");
const SCRAPER_OUT_DIR = path.join(PROJECT_ROOT, "scraper", "output");
const PROFILE_PATH    = path.join(PROJECT_ROOT, "config", "profile.json");
const SKILLS_PATH     = path.join(PROJECT_ROOT, "config", "skills.json");
const CONFIG_PATH     = path.join(PROJECT_ROOT, "config", "config.json");
config({ path: path.join(PROJECT_ROOT, ".env") });

// ---------------------------------------------------------------------------
// Config from env
// ---------------------------------------------------------------------------

const SOURCE         = process.env.SOURCE  ?? "dice";
const MAX_JOBS       = parseInt(process.env.MAX ?? "20", 10);
const HEADED         = Boolean(process.env.HEADED);
const JSONL_OVERRIDE = process.env.JSONL   ?? "";
const DO_EXTRACT     = Boolean(process.env.EXTRACT);   // opt-in — costs LLM calls

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<void> {

  // --- Load profile ---
  if (!fs.existsSync(PROFILE_PATH)) {
    die(`Profile not found at ${PROFILE_PATH}\n  cp config/profile-v2.json config/profile.json`);
  }
  const profile = JSON.parse(fs.readFileSync(PROFILE_PATH, "utf-8"));
  try { validateProfile(profile); } catch (err) { die(`Profile validation failed: ${err}`); }
  log(`Profile: ${profile.meta?.profile_id ?? "unknown"}`);

  // --- Load config ---
  if (!fs.existsSync(CONFIG_PATH)) {
    die(`Config not found at ${CONFIG_PATH}`);
  }
  const config = JSON.parse(fs.readFileSync(CONFIG_PATH, "utf-8"));
  const extractorConfig = {
    model:       config.llm.extractor.model       as string,
    max_tokens:  config.llm.extractor.max_tokens  as number,
    temperature: config.llm.extractor.temperature as number,
  };

  // --- Load skill aliases ---
  if (!fs.existsSync(SKILLS_PATH)) {
    die(`Skills not found at ${SKILLS_PATH}`);
  }
  const skillsJson = JSON.parse(fs.readFileSync(SKILLS_PATH, "utf-8"));
  const aliases    = buildAliasMap(skillsJson);
  log(`Skill aliases loaded: ${Object.keys(aliases).length} entries`);

  if (DO_EXTRACT) {
    if (!process.env.OPENROUTER_API_KEY) {
      die("EXTRACT=1 set but OPENROUTER_API_KEY not found.\nAdd it to .env or export it.");
    }
    log(`Extraction enabled — model: ${extractorConfig.model}`);
  } else {
    log("Extraction disabled (set EXTRACT=1 to enable)");
  }

  // --- Scrape or use existing JSONL ---
  const jsonlPath = JSONL_OVERRIDE ? JSONL_OVERRIDE : runScraper(SOURCE, MAX_JOBS, HEADED);
  if (!fs.existsSync(jsonlPath)) die(`JSONL not found: ${jsonlPath}`);
  log(`Reading: ${jsonlPath}`);

  // --- Process ---
  const nowIso  = new Date().toISOString();
  const results = await processJobs(jsonlPath, profile, aliases, extractorConfig, nowIso);

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
    cwd: PROJECT_ROOT, encoding: "utf-8", stdio: ["ignore", "pipe", "pipe"],
  });

  if (result.stderr) process.stderr.write(result.stderr);

  if (result.status === 2) die(`Cookie file missing — config/cookies/${source}.json`);
  if (result.status !== 0) die(`Scraper exited with code ${result.status}`);

  const jsonlPath = findNewestJsonl(source);
  if (!jsonlPath) die(`No JSONL in ${SCRAPER_OUT_DIR} for source "${source}"`);
  return jsonlPath!;
}

function findNewestJsonl(source: string): string | null {
  if (!fs.existsSync(SCRAPER_OUT_DIR)) return null;
  const files = fs
    .readdirSync(SCRAPER_OUT_DIR)
    .filter(f => f.startsWith(`${source}_`) && f.endsWith(".jsonl"))
    .map(f => ({ name: f, mtime: fs.statSync(path.join(SCRAPER_OUT_DIR, f)).mtimeMs }))
    .sort((a, b) => b.mtime - a.mtime);
  return files.length ? path.join(SCRAPER_OUT_DIR, files[0].name) : null;
}

// ---------------------------------------------------------------------------
// Job processing — full pipeline
// ---------------------------------------------------------------------------

interface JobResult {
  title:         string;
  company:       string;
  verdict:       string;
  reason:        string | null;
  flags:         string[];
  // Populated after extraction (only for PASS + EXTRACT=1)
  skills?:       string[];
  yoe_min?:      number | null;
  yoe_max?:      number | null;
  domain?:       string | null;
  fetch_status?: string;
  extract_status?: string;
}

async function processJobs(
  jsonlPath:       string,
  profile:         unknown,
  aliases:         Record<string, string>,
  extractorConfig: { model: string; max_tokens: number; temperature: number },
  nowIso:          string,
): Promise<JobResult[]> {
  const results: JobResult[] = [];

  const rl = readline.createInterface({
    input: fs.createReadStream(jsonlPath, "utf-8"), crlfDelay: Infinity,
  });

  let jobNum = 0;
  for await (const line of rl) {
    const trimmed = line.trim();
    if (!trimmed) continue;

    let raw: any;
    try { raw = JSON.parse(trimmed); }
    catch { log(`Skipping malformed line`); continue; }

    jobNum++;

    // Stage 3 — sanitize
    const sanitized = sanitizeJob(raw);

    // Stage 4 — hard filter
    const filterResult = hardFilter(sanitized, profile as any);

    if (filterResult.verdict === "REJECT") {
      results.push({
        title:   sanitized.title         ?? "",
        company: sanitized.company?.name ?? "",
        verdict: "REJECT",
        reason:  filterResult.reason     ?? null,
        flags:   [...new Set([
          ...(sanitized.meta?.flags ?? []),
          ...(filterResult.flags    ?? []),
        ])],
      });
      continue;  // bible: REJECTs are dropped after stage 4
    }

    // --- PASS — continue through pipeline ---
    log(`[${jobNum}] PASS: ${sanitized.title} @ ${sanitized.company?.name}`);

    // Stage 5 — fetch JD
    let fetchStatus = "skipped";
    if (DO_EXTRACT) {
      log(`  Fetching: ${sanitized.meta?.source_url}`);
      const fetchResult = await fetchJobPage(sanitized.meta?.source_url ?? "");
      fetchStatus = fetchResult.status;

      if (fetchResult.status === "ok") {
        sanitized.description_raw = fetchResult.description_raw;
        log(`  Fetched: ${fetchResult.description_raw.length} chars`);
      } else {
        log(`  Fetch failed: ${fetchResult.error}`);
        sanitized.meta.flags.push("fetch_failed");
      }
    }

    // Stage 6 — post-fetch checks (now has real description_raw if fetched)
    const checked = postFetchChecks(sanitized, nowIso);

    // Stage 7 — extract structured fields
    let extractStatus = "skipped";
    let skills:    string[]     = [];
    let yoeMin:    number | null = null;
    let yoeMax:    number | null = null;
    let domain:    string | null = null;

    if (DO_EXTRACT && sanitized.description_raw) {
      log(`  Extracting...`);
      const extraction = await extract(sanitized.description_raw, extractorConfig);
      // Polite delay between LLM calls — free tier RPM is tight
      if (DO_EXTRACT) await new Promise(r => setTimeout(r, 4000)); // wait before retry
      extractStatus = extraction.status;

      if (extraction.status === "ok" && extraction.fields) {
        const f = extraction.fields;

        // Stage 10 — normalize skill names through alias map
        skills = f.required_skills.map(s => normalizeSkill(s.name, aliases));
        yoeMin = f.years_experience.min;
        yoeMax = f.years_experience.max;
        domain = f.domain;

        // Write extracted fields back onto job for downstream use
        sanitized.required_skills    = f.required_skills.map(s => ({
          ...s, name: normalizeSkill(s.name, aliases),
        }));
        sanitized.years_experience   = { min: f.years_experience.min, max: f.years_experience.max };
        sanitized.education_required = { minimum: f.education_required.minimum, field: f.education_required.field };
        sanitized.responsibilities   = f.responsibilities;
        sanitized.visa_sponsorship   = f.visa_sponsorship;
        sanitized.security_clearance = f.security_clearance;
        sanitized.domain             = f.domain;

        log(`  Extracted: ${skills.length} skills, YOE ${yoeMin}-${yoeMax}, domain: ${domain}`);

        if (extraction.citation_failures && extraction.citation_failures > 0) {
          log(`  Citation failures: ${extraction.citation_failures}`);
        }
      } else {
        log(`  Extraction failed: ${extraction.error}`);
        sanitized.meta.flags.push("extraction_failed");
      }
    }

    // filterResult.flags already contains sanitized's flags (hardFilter copies them in).
    // Merge with post-fetch flags and dedup.
    const allFlags = [...new Set([...filterResult.flags, ...checked])];

    results.push({
      title:          sanitized.title         ?? "",
      company:        sanitized.company?.name ?? "",
      verdict:        "PASS",
      reason:         null,
      flags:          allFlags,
      skills:         skills.length ? skills : undefined,
      yoe_min:        yoeMin,
      yoe_max:        yoeMax,
      domain:         domain ?? undefined,
      fetch_status:   fetchStatus,
      extract_status: extractStatus,
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
    const title   = pad(r.title,   46);
    const company = pad(r.company, 24);
    const detail  = r.verdict === "REJECT" ? `REJECT  ${r.reason ?? ""}` : "PASS";
    const flags   = r.flags.length ? `  [${r.flags.join(", ")}]` : "";
    console.log(`  ${icon}  ${title}  ${company}  ${detail}${flags}`);

    // Show extracted data under PASS jobs when extraction ran
    if (r.verdict === "PASS" && r.skills?.length) {
      const yoe = r.yoe_min != null
        ? ` | YOE: ${r.yoe_min}${r.yoe_max ? `-${r.yoe_max}` : "+"}yrs`
        : "";
      const dom = r.domain ? ` | domain: ${r.domain}` : "";
      console.log(`       skills: ${r.skills.slice(0, 8).join(", ")}${r.skills.length > 8 ? "…" : ""}${yoe}${dom}`);
    }
  }

  // Summary
  const passed   = results.filter(r => r.verdict === "PASS");
  const rejected = results.filter(r => r.verdict === "REJECT");

  console.log(`\n${SEP}`);
  console.log(`  SUMMARY`);
  console.log(SEP);
  console.log(`  Total      ${results.length}`);
  console.log(`  Passed     ${passed.length}`);
  console.log(`  Rejected   ${rejected.length}`);

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

  if (DO_EXTRACT && passed.length) {
    const extracted = passed.filter(r => r.extract_status === "ok");
    const fetchFail = passed.filter(r => r.fetch_status   === "error");
    console.log(`\n  Extraction: ${extracted.length}/${passed.length} successful`);
    if (fetchFail.length) console.log(`  Fetch failures: ${fetchFail.length}`);
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

const DO_EXTRACT_GLOBAL = DO_EXTRACT; // referenced in processJobs closure
main().catch(err => {
  process.stderr.write(`[pipeline] Unhandled error: ${err}\n`);
  process.exit(1);
});