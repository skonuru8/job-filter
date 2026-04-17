import { FLAGS } from "./constants.ts"
import type { Job } from "./types.ts"

/**
 * Post-fetch checks — runs after the JD is fetched, before scoring.
 * Returns flags to be merged into job.meta.flags by the caller.
 *
 * nowIso should be a single timestamp captured at pipeline-run start
 * and reused for every job in the run. Within-run consistency beats
 * within-job freshness for staleness decisions.
 */
export function postFetchChecks(job: Job, nowIso: string): string[] {
  const flags: string[] = []

  // Education regex recovery — only if structured field was absent.
  const jobMin = job.education_required?.minimum?.trim() || null
  if (!jobMin) {
    const degreeRegex = /\b(bachelor|master|phd|doctorate|b\.?s\.?|m\.?s\.?|degree)\b/i
    if (degreeRegex.test(job.description_raw ?? "")) {
      flags.push(FLAGS.EDUCATION_UNPARSED)
    }
  }

  // Staleness. Unparseable posted_at treated as missing.
  const posted = job.meta?.posted_at
  if (!posted) {
    flags.push(FLAGS.POSTED_AT_MISSING)
  } else {
    const postedMs = Date.parse(posted)
    if (isNaN(postedMs)) {
      flags.push(FLAGS.POSTED_AT_MISSING)
    } else {
      const nowMs = Date.parse(nowIso)
      const ageDays = (nowMs - postedMs) / 86400000
      if (ageDays > 30) flags.push(FLAGS.STALE_POSTING)
    }
  }

  return flags
}
