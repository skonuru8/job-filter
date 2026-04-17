import { FLAGS } from "./constants.ts"
import type { Job } from "./types.ts"

/**
 * Sanitize a scraped job before it reaches hardFilter.
 * Pure: returns a deep-cloned, normalized copy.
 *
 * Currently handles:
 *   - source_score out of 0–100 range → nulled + SOURCE_SCORE_INVALID flag
 *
 * Extend here as new sanitization cases are discovered from real scraper output.
 */
export function sanitizeJob(job: Job): Job {
  const out = structuredClone(job)
  out.meta = out.meta ?? ({} as Job["meta"])
  out.meta.flags = [...(out.meta.flags ?? [])]

  const s = out.meta.source_score
  if (s != null) {
    if (typeof s !== "number" || isNaN(s) || s < 0 || s > 100) {
      out.meta.source_score = null
      out.meta.flags.push(FLAGS.SOURCE_SCORE_INVALID)
    }
  }

  return out
}
