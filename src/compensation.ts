import { FX_TO_USD, SOURCE_SCORE_WEIGHT_BY_SITE } from "./constants.ts"
import type { Job } from "./types.ts"

/**
 * Normalize any pay interval + currency to annual USD.
 * Returns null if any input is invalid — caller decides how to handle.
 * Rejects non-number input to catch silent string coercion bugs.
 */
export function toAnnualUSD(
  val: unknown,
  interval: string | null | undefined,
  currency: string | null | undefined
): number | null {
  if (typeof val !== "number" || isNaN(val)) return null
  if (!interval || !currency) return null

  const annualLocal = {
    hourly: val * 2080,
    monthly: val * 12,
    annual: val,
  }[interval]
  if (annualLocal === undefined) return null

  const fx = FX_TO_USD[currency.toUpperCase()]
  if (fx === undefined) return null

  return annualLocal * fx
}

/**
 * Apply a per-source match-score boost on top of a base score.
 * Capped at 1.0; source_score expected in 0–100 range (validated upstream).
 */
export function applySourceScore(baseScore: number, job: Job): number {
  if (job.meta?.source_score == null) return baseScore
  const weight = SOURCE_SCORE_WEIGHT_BY_SITE[job.meta.source_site] ?? 0
  if (weight === 0) return baseScore
  return Math.min(baseScore + (job.meta.source_score / 100) * weight, 1.0)
}
