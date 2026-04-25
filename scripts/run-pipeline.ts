/**
 * run-pipeline.ts — Milestone 6 end-to-end pipeline.
 *
 * Bible §5 stages wired in order:
 *   Stage 3  — sanitizeJob
 *   Stage 4  — hardFilter       (REJECTs dropped here)
 *   Stage 5  — fetchJobPage     (PASS jobs only)
 *   Stage 6  — postFetchChecks  (with real description_raw)
 *   Stage 7  — extract          (LLM → structured fields)
 *   Stage 10 — normalizeSkills  (alias map lookup)
 *   Stage 11 — scoreJob         (deterministic 5-component scoring)
 *   Stage 12 — gate             (score >= threshold → GATE_PASS, else ARCHIVE)
 *   Stage 13 — judge            (LLM verdict: STRONG | MAYBE | WEAK)
 *   Stage 14 — route            (COVER_LETTER | RESULTS | REVIEW_QUEUE | ARCHIVE)
 *   Stage 15 — cover letter     (COVER_LETTER bucket only → output/cover-letters/)
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
 *   SCORE=1             enable scoring (auto-enabled when EXTRACT=1)
 *   JUDGE=1             enable LLM judge (auto-enabled when EXTRACT=1)
 *   COVER=1             enable cover letter generation (auto-enabled when EXTRACT=1)
 *   QUERY="java developer"  Dice search query (default: "full stack developer")
 */

import { spawnSync }     from "child_process";
import * as fs           from "fs";
import * as path         from "path";
import { fileURLToPath } from "url";
import { randomUUID }    from "crypto";
import { config as loadEnv } from "dotenv";
import pLimit            from "p-limit";

import { hardFilter }      from "../src/filter";
import { postFetchChecks } from "../src/post-fetch";
import { sanitizeJob }     from "../src/sanitize";
import { validateProfile } from "../src/validate";
import { normalizeSkill, buildAliasMap } from "../src/skills";

import { fetchJobPage }  from "../../fetcher/src/fetch";
import { extract }       from "../../extractor/src/extract";
import { scoreJob }      from "../../scorer/src/score";
import { embedJob, embedProfile } from "../../scorer/src/embed";
import type { ScoringWeights, ScoreResult } from "../../scorer/src/types";

import { judge, getBucket } from "../../judge/src/judge";
import type { JudgeInput, JudgeResult, FinalBucket } from "../../judge/src/types";

import { generateCoverLetter, saveCoverLetter } from "../../cover-letter/src/generate";
import { loadResume } from "../../cover-letter/src/resume";
import type { CoverLetterInput, CoverLetterConfig } from "../../cover-letter/src/types";

// Dedup + storage — gracefully disabled via SKIP_DEDUP=1 / SKIP_PERSIST=1
import {
  connectRedis, disconnectRedis, isSeen, markSeen,
  findSemanticDuplicate,
} from "../../dedup/src/index";
import {
  runMigrations, saveRun, finishRun, saveJob, closePool,
} from "../../storage/src/index";
import type { JobRecord } from "../../storage/src/types";


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

// Load .env from project root (dotenv/config searches cwd which may differ)
loadEnv({ path: path.join(PROJECT_ROOT, ".env") });

// ---------------------------------------------------------------------------
// Config from env
// ---------------------------------------------------------------------------

const SOURCE         = process.env.SOURCE  ?? "dice";
const MAX_JOBS       = parseInt(process.env.MAX ?? "20", 10);
const HEADED         = Boolean(process.env.HEADED);
const JSONL_OVERRIDE = process.env.JSONL   ?? "";
const DO_EXTRACT     = Boolean(process.env.EXTRACT);   // opt-in — costs LLM calls
const DO_SCORE       = DO_EXTRACT || Boolean(process.env.SCORE);  // auto when extract runs
const DO_JUDGE       = DO_EXTRACT || Boolean(process.env.JUDGE);  // auto when extract runs
const DO_COVER       = DO_EXTRACT || Boolean(process.env.COVER);  // auto when extract runs
const SAVE_FIXTURES  = Boolean(process.env.SAVE_FIXTURES); // save real extraction fixtures
const SKIP_DEDUP     = Boolean(process.env.SKIP_DEDUP);    // bypass Redis + pgvector dedup
const SKIP_PERSIST   = Boolean(process.env.SKIP_PERSIST);  // bypass Postgres persistence 

// Dice-only env vars. Both passed through to the scraper subprocess.
//   QUERY="java developer"          search term (default below)
//   POSTED_WITHIN=ONE|THREE|SEVEN   server-side recency filter:
//                                     ONE   = jobs posted in last 24h
//                                     THREE = jobs posted in last 3 days
//                                     SEVEN = jobs posted in last 7 days
//                                     unset = no filter (all listings)
// Use POSTED_WITHIN=ONE for cron runs to only pull genuinely new jobs.
const QUERY          = process.env.QUERY ?? "java developer";
const POSTED_WITHIN  = process.env.POSTED_WITHIN ?? "";   // "" = no filter

const FIXTURES_DIR   = path.join(PROJECT_ROOT, "extractor", "fixtures");
const CONFIG_DIR     = path.join(PROJECT_ROOT, "config");
const RUN_ID = process.env.RUN_ID ?? randomUUID();
// Each run gets its own subdirectory — old runs are never touched.
// Bucket subfolders (COVER_LETTER / REVIEW_QUEUE) let you triage at a glance.
const COVER_OUT_BASE = path.join(PROJECT_ROOT, "output", "cover-letters");
const COVER_OUT_DIR  = path.join(COVER_OUT_BASE, RUN_ID);

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
    throttle_ms: (config.llm.extractor.throttle_ms ?? 0) as number,
    ...(config.llm.extractor.reasoning
      ? { reasoning: config.llm.extractor.reasoning as Record<string, unknown> }
      : {}),
  };
  const judgeConfig = {
    model:       config.llm.judge.model       as string,
    max_tokens:  config.llm.judge.max_tokens  as number,
    temperature: config.llm.judge.temperature as number,
    throttle_ms: (config.llm.judge.throttle_ms ?? 600) as number,
    ...(config.llm.judge.reasoning
      ? { reasoning: config.llm.judge.reasoning as Record<string, unknown> }
      : {}),
  };
  const coverLetterConfig: CoverLetterConfig = {
    model:       config.llm.cover_letter.model       as string,
    max_tokens:  config.llm.cover_letter.max_tokens  as number,
    temperature: config.llm.cover_letter.temperature as number,
    throttle_ms: (config.llm.cover_letter.throttle_ms ?? 1000) as number,
    review_queue_threshold: (config.llm.cover_letter.review_queue_threshold ?? 0.70) as number,
    ...(config.llm.cover_letter.thinking
      ? { thinking: config.llm.cover_letter.thinking as { type: "enabled"; budget_tokens: number } }
      : {}),
  };
  const scoringWeights: ScoringWeights = config.scoring?.weights ?? {
    skills: 0.35, semantic: 0.25, yoe: 0.15, seniority: 0.15, location: 0.10,
  };
  const scoringThreshold: number = config.scoring?.gate_threshold ?? 0.55;

  // --- Load skill aliases ---
  if (!fs.existsSync(SKILLS_PATH)) {
    die(`Skills not found at ${SKILLS_PATH}`);
  }
  const skillsJson = JSON.parse(fs.readFileSync(SKILLS_PATH, "utf-8"));
  const aliases    = buildAliasMap(skillsJson);
  log(`Skill aliases loaded: ${Object.keys(aliases).length} entries`);

  // --- Load resume (resume.tex preferred, falls back to resume.md) ---
  const resumeText = loadResume(CONFIG_DIR);
  if (resumeText) {
    log(`Resume loaded (${resumeText.length} chars) — cover letters will use real background`);
  } else {
    log(`No resume found — add config/resume.tex to get specific, achievement-backed cover letters`);
  }

  if (DO_EXTRACT) {
    if (!process.env.OPENROUTER_API_KEY) {
      die("EXTRACT=1 set but OPENROUTER_API_KEY not found.\nAdd it to .env or export it.");
    }
    log(`Extraction enabled  — model: ${extractorConfig.model}`);
    log(`Judge enabled       — model: ${judgeConfig.model}`);
    log(`Cover letter enabled— model: ${coverLetterConfig.model}`);
  } else {
    log("Extraction disabled (set EXTRACT=1 to enable)");
    log("Judge/Cover letter disabled (auto-enabled with EXTRACT=1)");
  }

  // --- Storage + dedup init (non-fatal — pipeline continues if unavailable) ---
  if (!SKIP_DEDUP) {
    await connectRedis();
  } else {
    log("Dedup disabled (SKIP_DEDUP=1)");
  }
  if (!SKIP_PERSIST) {
    try {
      await runMigrations();
      await saveRun({ run_id: RUN_ID, source: SOURCE, started_at: new Date().toISOString() });
      log(`Run record saved: ${RUN_ID}`);
    } catch (e: any) {
      log(`Storage init failed (continuing without persistence): ${e.message}`);
    }
  } else {
    log("Persistence disabled (SKIP_PERSIST=1)");
  }

  // --- Profile embedding (once at startup, reused for all jobs) ---
  let profileEmbedding: Float32Array | null = null;
  if (DO_SCORE) {
    log("Scoring enabled — embedding profile...");
    try {
      profileEmbedding = await embedProfile(profile);
      log(`Profile embedded (${profileEmbedding.length}-dim)`);
    } catch (e) {
      log(`Profile embedding failed (scoring will skip semantic component): ${e}`);
    }
  } else {
    log("Scoring disabled (auto-enabled with EXTRACT=1, or set SCORE=1)");
  }

  // --- Scrape or use existing JSONL ---
  const jsonlPath = JSONL_OVERRIDE
  ? JSONL_OVERRIDE
  : runScraper(SOURCE, MAX_JOBS, HEADED, QUERY, POSTED_WITHIN);

  if (!fs.existsSync(jsonlPath)) die(`JSONL not found: ${jsonlPath}`);
  log(`Reading: ${jsonlPath}`);

  // --- Process ---
  const nowIso  = new Date().toISOString();
  const results = await processJobs(
    jsonlPath, profile, aliases,
    extractorConfig, judgeConfig, coverLetterConfig,
    scoringWeights, scoringThreshold,
    profileEmbedding, resumeText, nowIso,
  );

  printResults(results, SOURCE, scoringThreshold);

  // --- Save results to disk (JSONL — always written when EXTRACT=1) ---
  if (DO_EXTRACT) {
    const outPath = path.join(SCRAPER_OUT_DIR, `results_${SOURCE}_${RUN_ID}.jsonl`);
    const lines = results.map(r => JSON.stringify(r)).join("\n");
    fs.writeFileSync(outPath, lines + "\n", "utf-8");
    log(`Results saved: ${outPath}`);
  }

  // --- Finish run record in Postgres ---
  if (!SKIP_PERSIST) {
    await finishRun(RUN_ID, {
      finished_at:  new Date().toISOString(),
      jobs_total:   results.length,
      jobs_passed:  results.filter(r => r.verdict !== "REJECT" && r.verdict !== "DEDUP").length,
      jobs_gated:   results.filter(r => r.verdict === "GATE_PASS").length,
      jobs_covered: results.filter(r => r.cover_letter_path != null).length,
      extractions_attempted: results.filter(r => r.extract_status === "ok" || r.extract_status === "error").length,
      extractions_succeeded: results.filter(r => r.extract_status === "ok").length,
    });
  }

  // --- Disconnect ---
  if (!SKIP_DEDUP)   await disconnectRedis();
  if (!SKIP_PERSIST) await closePool();
}

// ---------------------------------------------------------------------------
// Scraper spawn
// ---------------------------------------------------------------------------

function runScraper(
  source:        string,
  maxJobs:       number,
  headed:        boolean,
  query:         string,
  postedWithin:  string,
): string {
  const args = [
    "-m", "scraper",
    "--source", source,
    "--max",    String(maxJobs),
    ...(headed ? ["--headed"] : []),
  ];
 
  // Dice-only flags. Other sources ignore them (cli.py only passes them to dice).
  if (source === "dice") {
    args.push("--query", query);
    if (postedWithin) {
      args.push("--posted-within", postedWithin);
    }
  }
 
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
  source_url?:   string | null;
  verdict:       string;   // REJECT | PASS | GATE_PASS | ARCHIVE
  reason:        string | null;
  flags:         string[];
  // Populated after extraction (only for PASS + EXTRACT=1)
  skills?:       string[];
  yoe_min?:      number | null;
  yoe_max?:      number | null;
  domain?:       string | null;
  fetch_status?: string;
  extract_status?: string;
  // Populated after scoring (only when SCORE=1 or EXTRACT=1)
  score?:        ScoreResult;
  // Populated after judge (Stage 13-14, only for GATE_PASS when JUDGE=1)
  judge_verdict?:   string | null;
  judge_reasoning?: string | null;
  judge_concerns?:  string[];
  bucket?:          FinalBucket;
  // Populated after cover letter (Stage 15)
  cover_letter_path?: string | null;
  cover_letter_words?: number | null;
}

async function processJobs(
  jsonlPath:           string,
  profile:             unknown,
  aliases:             Record<string, string>,
  extractorConfig:     { model: string; max_tokens: number; temperature: number; throttle_ms: number; reasoning?: Record<string, unknown> },
  judgeConfigArg:      { model: string; max_tokens: number; temperature: number; throttle_ms: number; reasoning?: Record<string, unknown> },
  coverLetterConfigArg: CoverLetterConfig,
  scoringWeights:      ScoringWeights,
  scoringThreshold:    number,
  profileEmbedding:    Float32Array | null,
  resumeText:          string | null,
  nowIso:              string,
): Promise<JobResult[]> {
  // ---------------------------------------------------------------------------
  // Phase 1 — read all JSONL lines + synchronous sanitize/hard-filter
  //
  // Reading everything first gives every job a stable number before any async
  // work starts. No readline streaming: we need the full list to fan out.
  // ---------------------------------------------------------------------------
  const rawLines = fs.readFileSync(jsonlPath, "utf-8").split(/\r?\n/);

  // Shared fixture counter. Increment is always in a synchronous block after
  // an await, so JS single-threaded scheduling keeps it race-free.
  const fixtureRef = {
    count: SAVE_FIXTURES
      ? fs.readdirSync(FIXTURES_DIR).filter(f => f.startsWith("jd-real-") && f.endsWith("-input.txt")).length
      : 0,
  };

  interface PassEntry { jobNum: number; sanitized: any }
  const rejects:   Array<{ jobNum: number; result: JobResult }> = [];
  const passQueue: PassEntry[] = [];

  let jobNum = 0;
  for (const line of rawLines) {
    const trimmed = line.trim();
    if (!trimmed) continue;

    let raw: any;
    try { raw = JSON.parse(trimmed); }
    catch { log(`Skipping malformed line`); continue; }

    jobNum++;
    const n = jobNum;

    // Stage 3 — sanitize
    const sanitized = sanitizeJob(raw);

    // Stage 4 — hard filter
    const filterResult = hardFilter(sanitized, profile as any);

    if (filterResult.verdict === "REJECT") {
      rejects.push({
        jobNum: n,
        result: {
          title:      sanitized.title         ?? "",
          company:    sanitized.company?.name ?? "",
          source_url: sanitized.meta?.source_url ?? null,
          verdict:    "REJECT",
          reason:     filterResult.reason     ?? null,
          flags:      [...new Set(filterResult.flags ?? [])],
        },
      });
      continue;  // bible: REJECTs are dropped after stage 4
    }

    log(`[${n}] PASS: ${sanitized.title} @ ${sanitized.company?.name}`);
    passQueue.push({ jobNum: n, sanitized });
  }

  // ---------------------------------------------------------------------------
  // Stage 2 — Cross-run exact dedup (Redis)
  //
  // Batch-checks every PASS job_id against the Redis seen-set.
  // Jobs seen in a recent run (7-day TTL) are moved to the `dedups` list and
  // skipped from expensive fetch/extract/judge/cover-letter stages.
  // Gracefully disabled when SKIP_DEDUP=1 or Redis is unreachable.
  // ---------------------------------------------------------------------------
  const dedups: Array<{ jobNum: number; result: JobResult }> = [];

  if (!SKIP_DEDUP) {
    const seenFlags = await Promise.all(
      passQueue.map(e => isSeen(SOURCE, e.sanitized.meta?.job_id ?? "")),
    );
    const remaining: typeof passQueue = [];
    for (let i = 0; i < passQueue.length; i++) {
      if (seenFlags[i]) {
        const { jobNum: n, sanitized: s } = passQueue[i];
        log(`[${n}] DEDUP: ${s.title} @ ${s.company?.name} (seen in a recent run)`);
        dedups.push({
          jobNum: n,
          result: {
            title:      s.title         ?? "",
            company:    s.company?.name ?? "",
            source_url: s.meta?.source_url ?? null,
            verdict:    "DEDUP",
            reason:     "already_processed",
            flags:      [],
          },
        });
      } else {
        remaining.push(passQueue[i]);
      }
    }
    passQueue.length = 0;
    passQueue.push(...remaining);
  }

  // ---------------------------------------------------------------------------
  // Phase 2 — concurrent async processing of PASS jobs (fetch→extract→score→judge)
  //
  // pLimit(5): at most 5 jobs running concurrently. Keeps Dice HTTP and
  // OpenRouter LLM load reasonable without serialising everything.
  // ---------------------------------------------------------------------------
  const limit = pLimit(5);

  const passPromises = passQueue.map(({ jobNum: n, sanitized }) =>
    limit(async (): Promise<{ jobNum: number; result: JobResult }> => {

      // Stage 5 — fetch JD
      let fetchStatus = "skipped";
      if (DO_EXTRACT) {
        log(`[${n}]  Fetching: ${sanitized.meta?.source_url}`);
        const fetchResult = await fetchJobPage(sanitized.meta?.source_url ?? "");
        fetchStatus = fetchResult.status;

        if (fetchResult.status === "ok") {
          sanitized.description_raw = fetchResult.description_raw;
          log(`[${n}]  Fetched: ${fetchResult.description_raw.length} chars`);
        } else {
          log(`[${n}]  Fetch failed: ${fetchResult.error}`);
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
        // Small courtesy pause — not rate-limit critical with reasoning disabled,
        // but avoids bursting all 3 concurrent slots simultaneously.
        if (extractorConfig.throttle_ms > 0) {
          await new Promise(r => setTimeout(r, extractorConfig.throttle_ms));
        }

        log(`[${n}]  Extracting...`);
        const extraction = await extract(sanitized.description_raw, extractorConfig);
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
          sanitized.security_clearance = mapClearance(f.security_clearance, sanitized);
          sanitized.domain             = f.domain;

          // Clear stale flags that the hard filter set pre-extraction.
          // Extraction may now have data that resolves the uncertainty.
          const clearFlag = (flag: string) => {
            sanitized.meta.flags = sanitized.meta.flags.filter((x: string) => x !== flag);
          };
          if (f.years_experience.min != null || f.years_experience.max != null) {
            clearFlag("years_experience_missing");
          }
          if (f.visa_sponsorship != null) {
            clearFlag("sponsorship_unclear");
          }
          if (f.education_required.minimum && f.education_required.minimum !== "") {
            clearFlag("education_unparsed");
          }

          log(`[${n}]  Extracted: ${skills.length} skills, YOE ${yoeMin}-${yoeMax}, domain: ${domain}`);

          if (extraction.citation_failures && extraction.citation_failures > 0) {
            log(`[${n}]  Citation failures: ${extraction.citation_failures}`);
          }

          // Save real fixture pair when SAVE_FIXTURES=1 (up to 5 per run).
          // check+increment are in a synchronous block — JS single-threaded
          // scheduling keeps this race-free even with concurrent tasks.
          if (SAVE_FIXTURES && fixtureRef.count < 5 && sanitized.description_raw?.trim()) {
            fixtureRef.count++;
            const slug   = (sanitized.title ?? "job")
              .toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "").slice(0, 35);
            const num    = String(fixtureRef.count).padStart(3, "0");
            const prefix = `jd-real-${num}-${slug}`;
            fs.writeFileSync(path.join(FIXTURES_DIR, `${prefix}-input.txt`),  sanitized.description_raw);
            fs.writeFileSync(path.join(FIXTURES_DIR, `${prefix}-expected.json`), JSON.stringify(extraction.fields, null, 2));
            log(`[${n}]  Fixture saved: ${prefix}`);
          }
        } else {
          log(`[${n}]  Extraction failed: ${extraction.error}`);
          sanitized.meta.flags.push("extraction_failed");
        }
      }

    // Stage 11 — deterministic scoring
    // Runs when DO_SCORE is set. Requires extraction for best results;
    // without it, skills/YOE components will be 0 (no data to compare).
    //
    // Skip scoring when extraction was attempted but failed: without skills
    // data, scoreSkills() returns 1.0 (no bar = perfect match) which produces
    // a misleading ~0.85 score and wastes a downstream judge LLM call.
    const extractionFailed = DO_EXTRACT && extractStatus === "error";
    let scoreResult: ScoreResult | undefined;
    let jobEmbedding: Float32Array | null = null;  // hoisted — also used by pgvector dedup + persist
    if (DO_SCORE && !extractionFailed) {
      try {
        jobEmbedding = await embedJob(sanitized);
      } catch {
        // embedding failure → semantic component = 0, score continues
      }

      scoreResult = scoreJob(
        sanitized as any,
        profile as any,
        jobEmbedding,
        profileEmbedding,
        scoringWeights,
        scoringThreshold,
      );

          log(`[${n}]  Score: ${scoreResult.score.toFixed(3)} (gate: ${scoreResult.gate_passed ? "PASS" : "FAIL"}) | skills=${scoreResult.components.skills.toFixed(2)} yoe=${scoreResult.components.yoe.toFixed(2)} sen=${scoreResult.components.seniority.toFixed(2)} loc=${scoreResult.components.location.toFixed(2)} sem=${scoreResult.components.semantic.toFixed(2)}`);
    } else if (extractionFailed) {
      log(`[${n}]  Score: skipped (extraction failed) → routing to ARCHIVE`);
    }

    // Stage 12 — threshold gate
    // gate_passed jobs proceed to the LLM judge.
    // gate_fail → ARCHIVE bucket.
    // extraction_failed → ARCHIVE (no reliable data to judge on).
    // When scoring is disabled, all PASS jobs go through as PASS (no gate).
    const gateVerdict = extractionFailed
      ? "ARCHIVE"
      : scoreResult
        ? (scoreResult.gate_passed ? "GATE_PASS" : "ARCHIVE")
        : "PASS";

    // sanitized.meta.flags is the live flag set — cleaned up after extraction
    // resolved earlier-flagged uncertainty. filterResult.flags is stale
    // (snapshotted before extraction). Merge live flags with post-fetch checks.
    const allFlags = [...new Set([...(sanitized.meta?.flags ?? []), ...checked])];

    // Stage 13–14 — LLM judge + routing (GATE_PASS only)
    let judgeResult: JudgeResult | undefined;
    // Seed bucket from gate verdict so ARCHIVE (gate_fail / extraction_failed)
    // lands in the archive bucket for the summary — only GATE_PASS paths
    // reassign bucket after the judge runs.
    let bucket: FinalBucket | undefined = gateVerdict === "ARCHIVE" ? "ARCHIVE" : undefined;
    let finalVerdict = gateVerdict;

    // Stage 12.5 — pgvector cross-site semantic dedup
    // Only runs on GATE_PASS jobs with an embedding — avoids calling the
    // judge on a job that's semantically identical to one we already processed.
    if (!SKIP_DEDUP && jobEmbedding && gateVerdict === "GATE_PASS") {
      const dupJobId = await findSemanticDuplicate(Array.from(jobEmbedding), RUN_ID);
      if (dupJobId) {
        log(`[${n}]  Semantic dup of job ${dupJobId} → ARCHIVE`);
        allFlags.push("semantic_duplicate");
        bucket       = "ARCHIVE";
        finalVerdict = "ARCHIVE";
      }
    }

    if (DO_JUDGE && gateVerdict === "GATE_PASS" && scoreResult) {
      // Throttle between LLM calls
      if (judgeConfigArg.throttle_ms > 0) {
        await new Promise(r => setTimeout(r, judgeConfigArg.throttle_ms));
      }

      log(`[${n}]  Judging...`);
      const judgeInput: JudgeInput = {
        job: {
          title:             sanitized.title          ?? "",
          company:           sanitized.company?.name  ?? "",
          employment_type:   sanitized.employment_type ?? null,
          seniority:         sanitized.seniority       ?? null,
          domain:            sanitized.domain          ?? null,
          required_skills:   (sanitized.required_skills ?? []).map((s: any) => ({
            name:           s.name,
            importance:     s.importance ?? "required",
            years_required: s.years_required ?? null,
          })),
          years_experience:  {
            min: sanitized.years_experience?.min ?? null,
            max: sanitized.years_experience?.max ?? null,
          },
          education_required: {
            minimum: sanitized.education_required?.minimum ?? "",
            field:   sanitized.education_required?.field   ?? "",
          },
          visa_sponsorship:  sanitized.visa_sponsorship ?? null,
          responsibilities:  sanitized.responsibilities  ?? [],
          flags:             allFlags,
        },
        score: {
          total:      scoreResult.score,
          components: {
            skills:    scoreResult.components.skills,
            semantic:  scoreResult.components.semantic,
            yoe:       scoreResult.components.yoe,
            seniority: scoreResult.components.seniority,
            location:  scoreResult.components.location,
          },
        },
      };

      judgeResult = await judge(judgeInput, judgeConfigArg);
      bucket      = getBucket(judgeResult, scoreResult.score);
      finalVerdict = gateVerdict;   // still GATE_PASS for the raw record; bucket is the real routing

      if (judgeResult.status === "ok") {
        log(`[${n}]  Judge: ${judgeResult.verdict} → ${bucket}`);
        if (judgeResult.fields?.concerns.length) {
          log(`[${n}]  Concerns: ${judgeResult.fields.concerns.join("; ")}`);
        }
      } else {
        log(`[${n}]  Judge error: ${judgeResult.error} → ${bucket}`);
        allFlags.push("judge_failed");
      }
    }

    // Stage 15 — Cover letter
    // Always: COVER_LETTER bucket.
    // Also: REVIEW_QUEUE jobs with score >= review_queue_threshold get a draft.
    //   Bucket stays REVIEW_QUEUE — human still reviews judge concerns before sending.
    //   Set review_queue_threshold: 1.0 in config to disable this behaviour.
    let coverLetterPath: string | null  = null;
    let coverLetterWords: number | null = null;

    const rvqThreshold = coverLetterConfigArg.review_queue_threshold ?? 0.70;
    const shouldWriteLetter = DO_COVER && scoreResult && (
      bucket === "COVER_LETTER" ||
      (bucket === "REVIEW_QUEUE" && scoreResult.score >= rvqThreshold)
    );

    if (shouldWriteLetter) {
      if (coverLetterConfigArg.throttle_ms > 0) {
        await new Promise(r => setTimeout(r, coverLetterConfigArg.throttle_ms));
      }

      log(`[${n}]  Writing cover letter...`);
      const clInput: CoverLetterInput = {
        job: {
          job_id:           sanitized.meta?.job_id ?? `job-${jobNum}`,
          title:            sanitized.title         ?? "",
          company:          sanitized.company?.name ?? "",
          domain:           sanitized.domain        ?? null,
          employment_type:  sanitized.employment_type ?? null,
          required_skills:  (sanitized.required_skills ?? []).map((s: any) => ({
            name:           s.name,
            importance:     s.importance ?? "required",
            years_required: s.years_required ?? null,
          })),
          responsibilities: sanitized.responsibilities ?? [],
          yoe_min:          sanitized.years_experience?.min ?? null,
          yoe_max:          sanitized.years_experience?.max ?? null,
          visa_sponsorship: sanitized.visa_sponsorship ?? null,
          score:            scoreResult!.score,
          score_components: {
            skills:    scoreResult!.components.skills,
            semantic:  scoreResult!.components.semantic,
            yoe:       scoreResult!.components.yoe,
            seniority: scoreResult!.components.seniority,
            location:  scoreResult!.components.location,
          },
          judge_reasoning: judgeResult?.fields?.reasoning ?? null,
          judge_concerns:  judgeResult?.fields?.concerns  ?? [],
        },
        profile: {
          skills:            (profile as any).skills ?? [],
          years_experience:  (profile as any).years_experience ?? 0,
          education:         (profile as any).education ?? { degree: "bachelor", field: "" },
          preferred_domains: (profile as any).preferred_domains ?? [],
        },
        resume: resumeText,
      };

      const clResult = await generateCoverLetter(clInput, coverLetterConfigArg);
      if (clResult.status === "ok" && clResult.text) {
        try {
          // Route into bucket subfolder so COVER_LETTER and REVIEW_QUEUE are separate.
          const bucketLabel = bucket === "COVER_LETTER" ? "COVER_LETTER" : "REVIEW_QUEUE";
          const clOutDir    = path.join(COVER_OUT_DIR, bucketLabel);
          coverLetterPath   = saveCoverLetter(clResult, clInput, clOutDir);
          coverLetterWords = clResult.word_count ?? null;
          log(`[${n}]  Cover letter: ${coverLetterPath} (${coverLetterWords} words)`);
        } catch (e) {
          log(`[${n}]  Cover letter save failed: ${e}`);
          allFlags.push("cover_letter_save_failed");
        }
      } else {
        log(`[${n}]  Cover letter generation failed: ${clResult.error}`);
        allFlags.push("cover_letter_failed");
      }
    }

    // Stage 16 — persist to Postgres + mark seen in Redis
    // Runs after all stages so a mid-run crash doesn't mark the job as seen
    // prematurely (it would be retried on the next run).
    const jobId = (sanitized.meta?.job_id as string | undefined) ?? `job-${n}`;

    if (!SKIP_PERSIST) {
      const jobRecord: JobRecord = {
        job_id:          jobId,
        run_id:          RUN_ID,
        source:          SOURCE,
        source_url:      sanitized.meta?.source_url    ?? null,
        title:           sanitized.title               ?? null,
        company:         sanitized.company?.name       ?? null,
        posted_at:       sanitized.meta?.posted_at     ?? null,
        scraped_at:      sanitized.meta?.scraped_at    ?? null,
        description_raw: sanitized.description_raw     ?? null,
        meta:            sanitized.meta                ?? null,
        extracted:       sanitized.required_skills?.length
                           ? { required_skills: sanitized.required_skills,
                               years_experience: sanitized.years_experience,
                               domain: sanitized.domain,
                               responsibilities: sanitized.responsibilities }
                           : null,
        embedding:       jobEmbedding ? Array.from(jobEmbedding) : null,
        filter_verdict:  finalVerdict,
        filter_flags:    allFlags,
        score: scoreResult ? {
          total:     scoreResult.score,
          skills:    scoreResult.components.skills,
          semantic:  scoreResult.components.semantic,
          yoe:       scoreResult.components.yoe,
          seniority: scoreResult.components.seniority,
          location:  scoreResult.components.location,
        } : null,
        judge_verdict:   judgeResult?.verdict            ?? null,
        judge_bucket:    bucket                          ?? null,
        judge_reasoning: judgeResult?.fields?.reasoning  ?? null,
        judge_concerns:  judgeResult?.fields?.concerns   ?? [],
        cover_letter_path:  coverLetterPath,
        cover_letter_words: coverLetterWords,
        cover_letter_model: coverLetterPath ? coverLetterConfigArg.model : null,
      };
      await saveJob(jobRecord);
    }

    if (!SKIP_DEDUP) {
      await markSeen(SOURCE, jobId);
    }

    return {
      jobNum: n,
      result: {
        title:               sanitized.title         ?? "",
        company:             sanitized.company?.name ?? "",
        source_url:          sanitized.meta?.source_url ?? null,
        verdict:             finalVerdict,
        reason:              null,
        flags:               allFlags,
        skills:              skills.length ? skills : undefined,
        yoe_min:             yoeMin,
        yoe_max:             yoeMax,
        domain:              domain ?? undefined,
        fetch_status:        fetchStatus,
        extract_status:      extractStatus,
        score:               scoreResult,
        judge_verdict:       judgeResult?.verdict   ?? null,
        judge_reasoning:     judgeResult?.fields?.reasoning ?? null,
        judge_concerns:      judgeResult?.fields?.concerns  ?? [],
        bucket,
        cover_letter_path:   coverLetterPath,
        cover_letter_words:  coverLetterWords,
      },
    };
  })); // end limit()

  // Merge rejects + dedups + pass results, sort by original jobNum to preserve input order
  const passResults = await Promise.all(passPromises);
  return [...rejects, ...dedups, ...passResults]
    .sort((a, b) => a.jobNum - b.jobNum)
    .map(r => r.result);
}

// ---------------------------------------------------------------------------
// Output
// ---------------------------------------------------------------------------

function printResults(results: JobResult[], source: string, threshold: number): void {
  const SEP = "─".repeat(90);

  console.log(`\n${SEP}`);
  console.log(`  ${source.toUpperCase()} — ${results.length} jobs processed`);
  console.log(SEP);

  for (const r of results) {
    const icon = r.verdict === "REJECT"                    ? "✗"
               : r.verdict === "DEDUP"                     ? "↩"
               : r.verdict === "ARCHIVE"                   ? "○"
               : r.bucket  === "COVER_LETTER"              ? "★"
               : r.bucket  === "RESULTS"                   ? "✓"
               : r.bucket  === "REVIEW_QUEUE"              ? "?"
               : r.bucket  === "ARCHIVE"                   ? "○"
               : "·";

    const title   = pad(r.title,   42);
    const company = pad(r.company, 22);

    let detail: string;
    if (r.verdict === "REJECT") {
      detail = `REJECT  ${r.reason ?? ""}`;
    } else if (r.verdict === "ARCHIVE") {
      detail = `ARCHIVE  score=${r.score?.score.toFixed(3) ?? "?"}`;
    } else if (r.verdict === "GATE_PASS" && r.bucket) {
      const judgeTag = r.judge_verdict ? ` [${r.judge_verdict}]` : "";
      detail = `${r.bucket}${judgeTag}  score=${r.score?.score.toFixed(3) ?? "?"}`;
    } else if (r.verdict === "GATE_PASS") {
      detail = `GATE_PASS  score=${r.score?.score.toFixed(3) ?? "?"}`;
    } else {
      detail = "PASS";
    }

    const flags = r.flags.length ? `  [${r.flags.join(", ")}]` : "";
    console.log(`  ${icon}  ${title}  ${company}  ${detail}${flags}`);

    // Show score breakdown
    if (r.score && r.verdict !== "REJECT") {
      const c = r.score.components;
      console.log(
        `       score: skills=${c.skills.toFixed(2)} yoe=${c.yoe.toFixed(2)} ` +
        `seniority=${c.seniority.toFixed(2)} location=${c.location.toFixed(2)} ` +
        `semantic=${c.semantic.toFixed(2)}`
      );
    }

    // Show judge reasoning when present
    if (r.judge_reasoning) {
      console.log(`       judge:  ${r.judge_reasoning}`);
    }

    // Show cover letter path when generated
    if (r.cover_letter_path) {
      console.log(`       cover:  ${r.cover_letter_path} (${r.cover_letter_words} words)`);
    }

    // Show extracted skills
    if ((r.verdict === "GATE_PASS" || r.verdict === "PASS") && r.skills?.length) {
      const yoe = r.yoe_min != null
        ? ` | YOE: ${r.yoe_min}${r.yoe_max ? `-${r.yoe_max}` : "+"}yrs`
        : "";
      const dom = r.domain ? ` | domain: ${r.domain}` : "";
      console.log(`       skills: ${r.skills.slice(0, 8).join(", ")}${r.skills.length > 8 ? "…" : ""}${yoe}${dom}`);
    }
  }

  // Summary
  const passed     = results.filter(r => r.verdict === "PASS");
  const gatePassed = results.filter(r => r.verdict === "GATE_PASS");
  const archived   = results.filter(r => r.verdict === "ARCHIVE");
  const rejected   = results.filter(r => r.verdict === "REJECT");
  const deduped    = results.filter(r => r.verdict === "DEDUP");

  const coverLetter  = results.filter(r => r.bucket === "COVER_LETTER");
  const resultsQueue = results.filter(r => r.bucket === "RESULTS");
  const reviewQueue  = results.filter(r => r.bucket === "REVIEW_QUEUE");
  const archiveBucket = results.filter(r => r.bucket === "ARCHIVE");

  console.log(`\n${SEP}`);
  console.log(`  SUMMARY`);
  console.log(SEP);
  console.log(`  Total        ${results.length}`);
  console.log(`  Passed       ${passed.length + gatePassed.length}  (hard filter pass)`);
  if (DO_SCORE) {
    if (DO_JUDGE && gatePassed.length > 0) {
      console.log(`  Gate PASS    ${gatePassed.length}  (score >= ${threshold})`);
      console.log(`  ├─ COVER_LETTER  ${coverLetter.length}   STRONG + score ≥ 0.70`);
      console.log(`  ├─ RESULTS       ${resultsQueue.length}   STRONG + score < 0.70`);
      console.log(`  ├─ REVIEW_QUEUE  ${reviewQueue.length}   MAYBE`);
      console.log(`  └─ ARCHIVE       ${archiveBucket.length}   WEAK or judge error`);
    } else {
      console.log(`  Gate PASS    ${gatePassed.length}  (score >= ${threshold})`);
    }
    console.log(`  Archive      ${archived.length}  (score < ${threshold})`);
  }
  console.log(`  Rejected     ${rejected.length}  (hard filter reject)`);
  if (deduped.length) {
    console.log(`  Deduped      ${deduped.length}  (seen in recent run — skipped)`);
  }

  if (DO_SCORE && (gatePassed.length + archived.length) > 0) {
    const scored = [...gatePassed, ...archived];
    const avgScore = scored.reduce((s, r) => s + (r.score?.score ?? 0), 0) / scored.length;
    const maxScore = Math.max(...scored.map(r => r.score?.score ?? 0));
    console.log(`\n  Scores:  avg=${avgScore.toFixed(3)}  max=${maxScore.toFixed(3)}  threshold=${threshold}`);

    // Component averages (useful for tuning weights)
    const avgComp = {
      skills:    avg(scored.map(r => r.score?.components.skills    ?? 0)),
      yoe:       avg(scored.map(r => r.score?.components.yoe       ?? 0)),
      seniority: avg(scored.map(r => r.score?.components.seniority ?? 0)),
      location:  avg(scored.map(r => r.score?.components.location  ?? 0)),
      semantic:  avg(scored.map(r => r.score?.components.semantic  ?? 0)),
    };
    console.log(
      `  Avg components:  skills=${avgComp.skills.toFixed(2)}  ` +
      `yoe=${avgComp.yoe.toFixed(2)}  seniority=${avgComp.seniority.toFixed(2)}  ` +
      `location=${avgComp.location.toFixed(2)}  semantic=${avgComp.semantic.toFixed(2)}`
    );
  }

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

  if (DO_EXTRACT) {
    const allPassed = [...passed, ...gatePassed, ...archived];
    const extracted = allPassed.filter(r => r.extract_status === "ok");
    const fetchFail = allPassed.filter(r => r.fetch_status   === "error");
    console.log(`\n  Extraction: ${extracted.length}/${allPassed.length} successful`);
    if (fetchFail.length) console.log(`  Fetch failures: ${fetchFail.length}`);
  }

  if (DO_COVER) {
    const letters = results.filter(r => r.cover_letter_path);
    if (letters.length) {
      console.log(`\n  Cover letters written: ${letters.length}`);
      for (const r of letters) {
        console.log(`    ${r.title} @ ${r.company} → ${r.cover_letter_path}`);
      }
    } else if (coverLetter.length === 0) {
      console.log(`\n  Cover letters: none (no COVER_LETTER bucket jobs this run)`);
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

function avg(nums: number[]): number {
  return nums.length ? nums.reduce((a, b) => a + b, 0) / nums.length : 0;
}

/**
 * Map extractor's security_clearance enum to job-filter's enum.
 * Extractor: "none" | "required" | "preferred" | "unknown"
 * Filter:    "none" | "public_trust" | "secret" | "top_secret"
 *
 * "required" → "secret"   (triggers CLEARANCE_REQUIRED reject if clearance_eligible: false)
 * "preferred"/"unknown" → "none" + clearance_unclear flag
 */
function mapClearance(extractorValue: string, job: any): string {
  switch (extractorValue) {
    case "none":
      return "none";
    case "required":
      return "secret";
    case "preferred":
    case "unknown":
      if (!job.meta.flags.includes("clearance_unclear")) {
        job.meta.flags.push("clearance_unclear");
      }
      return "none";
    default:
      if (!job.meta.flags.includes("clearance_unclear")) {
        job.meta.flags.push("clearance_unclear");
      }
      return "none";
  }
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