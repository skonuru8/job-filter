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
import * as readline     from "readline";
import { fileURLToPath } from "url";
import { config as loadEnv } from "dotenv";

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
const FIXTURES_DIR   = path.join(PROJECT_ROOT, "extractor", "fixtures");
const COVER_OUT_DIR  = path.join(PROJECT_ROOT, "output", "cover-letters");
const CONFIG_DIR     = path.join(PROJECT_ROOT, "config");
const RUN_ID = new Date().toISOString().replace(/[:.]/g, "-");

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
  };
  const judgeConfig = {
    model:       config.llm.judge.model       as string,
    max_tokens:  config.llm.judge.max_tokens  as number,
    temperature: config.llm.judge.temperature as number,
    throttle_ms: (config.llm.judge.throttle_ms ?? 600) as number,
  };
  const coverLetterConfig: CoverLetterConfig = {
    model:       config.llm.cover_letter.model       as string,
    max_tokens:  config.llm.cover_letter.max_tokens  as number,
    temperature: config.llm.cover_letter.temperature as number,
    throttle_ms: (config.llm.cover_letter.throttle_ms ?? 1000) as number,
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
  const jsonlPath = JSONL_OVERRIDE ? JSONL_OVERRIDE : runScraper(SOURCE, MAX_JOBS, HEADED);
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

  // --- Save results to disk ---
  if (DO_EXTRACT) {
      const outPath = path.join(SCRAPER_OUT_DIR, `results_${SOURCE}_${RUN_ID}.jsonl`);
      const lines = results.map(r => JSON.stringify(r)).join("\n");
      fs.writeFileSync(outPath, lines + "\n", "utf-8");
      log(`Results saved: ${outPath}`);
  }
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
  // Populated after cover letter (Stage 15, only for COVER_LETTER bucket)
  cover_letter_path?: string | null;
  cover_letter_words?: number | null;
}

async function processJobs(
  jsonlPath:           string,
  profile:             unknown,
  aliases:             Record<string, string>,
  extractorConfig:     { model: string; max_tokens: number; temperature: number; throttle_ms: number },
  judgeConfigArg:      { model: string; max_tokens: number; temperature: number; throttle_ms: number },
  coverLetterConfigArg: CoverLetterConfig,
  scoringWeights:      ScoringWeights,
  scoringThreshold:    number,
  profileEmbedding:    Float32Array | null,
  resumeText:          string | null,
  nowIso:              string,
): Promise<JobResult[]> {
  const results: JobResult[] = [];

  const rl = readline.createInterface({
    input: fs.createReadStream(jsonlPath, "utf-8"), crlfDelay: Infinity,
  });

  // Count existing real fixtures so we number new ones correctly
  let fixtureCount = SAVE_FIXTURES
    ? fs.readdirSync(FIXTURES_DIR).filter(f => f.startsWith("jd-real-") && f.endsWith("-input.txt")).length
    : 0;

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
        flags:   [...new Set(filterResult.flags ?? [])],
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
      // Throttle BEFORE call (bible fix: throttle before, not after)
      if (jobNum > 1 && extractorConfig.throttle_ms > 0) {
        await new Promise(r => setTimeout(r, extractorConfig.throttle_ms));
      }

      log(`  Extracting...`);
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

        log(`  Extracted: ${skills.length} skills, YOE ${yoeMin}-${yoeMax}, domain: ${domain}`);

        if (extraction.citation_failures && extraction.citation_failures > 0) {
          log(`  Citation failures: ${extraction.citation_failures}`);
        }

        // Save real fixture pair when SAVE_FIXTURES=1 (up to 5 per run)
        if (SAVE_FIXTURES && fixtureCount < 5 && sanitized.description_raw?.trim()) {
          fixtureCount++;
          const slug = (sanitized.title ?? "job")
            .toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "").slice(0, 35);
          const n      = String(fixtureCount).padStart(3, "0");
          const prefix = `jd-real-${n}-${slug}`;
          fs.writeFileSync(path.join(FIXTURES_DIR, `${prefix}-input.txt`),  sanitized.description_raw);
          fs.writeFileSync(path.join(FIXTURES_DIR, `${prefix}-expected.json`), JSON.stringify(extraction.fields, null, 2));
          log(`  Fixture saved: ${prefix}`);
        }
      } else {
        log(`  Extraction failed: ${extraction.error}`);
        sanitized.meta.flags.push("extraction_failed");
      }
    }

    // Stage 11 — deterministic scoring
    // Runs when DO_SCORE is set. Requires extraction for best results;
    // without it, skills/YOE components will be 0 (no data to compare).
    let scoreResult: ScoreResult | undefined;
    if (DO_SCORE) {
      let jobEmbedding: Float32Array | null = null;
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

      log(`  Score: ${scoreResult.score.toFixed(3)} (gate: ${scoreResult.gate_passed ? "PASS" : "FAIL"}) | skills=${scoreResult.components.skills.toFixed(2)} yoe=${scoreResult.components.yoe.toFixed(2)} sen=${scoreResult.components.seniority.toFixed(2)} loc=${scoreResult.components.location.toFixed(2)} sem=${scoreResult.components.semantic.toFixed(2)}`);
    }

    // Stage 12 — threshold gate
    // gate_passed jobs proceed to the LLM judge.
    // gate_fail → ARCHIVE bucket.
    // When scoring is disabled, all PASS jobs go through as PASS (no gate).
    const gateVerdict = scoreResult
      ? (scoreResult.gate_passed ? "GATE_PASS" : "ARCHIVE")
      : "PASS";

    // sanitized.meta.flags is the live flag set — cleaned up after extraction
    // resolved earlier-flagged uncertainty. filterResult.flags is stale
    // (snapshotted before extraction). Merge live flags with post-fetch checks.
    const allFlags = [...new Set([...(sanitized.meta?.flags ?? []), ...checked])];

    // Stage 13–14 — LLM judge + routing (GATE_PASS only)
    let judgeResult: JudgeResult | undefined;
    let bucket: FinalBucket | undefined;
    let finalVerdict = gateVerdict;

    if (DO_JUDGE && gateVerdict === "GATE_PASS" && scoreResult) {
      // Throttle between LLM calls
      if (judgeConfigArg.throttle_ms > 0) {
        await new Promise(r => setTimeout(r, judgeConfigArg.throttle_ms));
      }

      log(`  Judging...`);
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
        log(`  Judge: ${judgeResult.verdict} → ${bucket}`);
        if (judgeResult.fields?.concerns.length) {
          log(`  Concerns: ${judgeResult.fields.concerns.join("; ")}`);
        }
      } else {
        log(`  Judge error: ${judgeResult.error} → ${bucket}`);
        allFlags.push("judge_failed");
      }
    }

    // Stage 15 — Cover letter (COVER_LETTER bucket only)
    let coverLetterPath: string | null  = null;
    let coverLetterWords: number | null = null;

    if (DO_COVER && bucket === "COVER_LETTER" && scoreResult) {
      if (coverLetterConfigArg.throttle_ms > 0) {
        await new Promise(r => setTimeout(r, coverLetterConfigArg.throttle_ms));
      }

      log(`  Writing cover letter...`);
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
          score:            scoreResult.score,
          score_components: {
            skills:    scoreResult.components.skills,
            semantic:  scoreResult.components.semantic,
            yoe:       scoreResult.components.yoe,
            seniority: scoreResult.components.seniority,
            location:  scoreResult.components.location,
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
          coverLetterPath  = saveCoverLetter(clResult, clInput, COVER_OUT_DIR);
          coverLetterWords = clResult.word_count ?? null;
          log(`  Cover letter: ${coverLetterPath} (${coverLetterWords} words)`);
        } catch (e) {
          log(`  Cover letter save failed: ${e}`);
          allFlags.push("cover_letter_save_failed");
        }
      } else {
        log(`  Cover letter generation failed: ${clResult.error}`);
        allFlags.push("cover_letter_failed");
      }
    }

    results.push({
      title:               sanitized.title         ?? "",
      company:             sanitized.company?.name ?? "",
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
    });
  }

  return results;
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
               : r.verdict === "ARCHIVE"                   ? "○"
               : r.bucket  === "COVER_LETTER"              ? "★"
               : r.bucket  === "REVIEW_QUEUE"              ? "?"
               : r.bucket  === "ARCHIVE"                   ? "○"
               : "✓";

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