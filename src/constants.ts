// Level maps
export const SENIORITY_LEVEL: Record<string, number> = {
  intern: 0,
  junior: 1,
  mid: 2,
  senior: 3,
  lead: 3,
  manager: 3,
  staff: 4,
  principal: 5,
}

export const DEGREE_LEVEL: Record<string, number> = {
  none: 0,
  associate: 1,
  bachelor: 2,
  master: 3,
  phd: 4,
}

// Enforced enums — validated in hardFilter or validateProfile
export const LOCATION_TYPES = ["remote", "hybrid", "onsite"] as const
export const EMPLOYMENT_TYPES = [
  "full_time",
  "contract",
  "contract_to_hire",
  "part_time",
] as const
export const CLEARANCE_LEVELS = [
  "none",
  "public_trust",
  "secret",
  "top_secret",
] as const
export const COMPENSATION_INTERVALS = ["hourly", "monthly", "annual"] as const

// Reference-only enums — documented for extractor/profile-loader, not enforced here
export const COMPANY_TYPES = ["product", "consulting", "agency", "unknown"] as const
export const SKILL_CATEGORIES = [
  "language",
  "framework",
  "cloud",
  "tool",
  "methodology",
  "domain",
] as const
export const SKILL_IMPORTANCES = ["must", "preferred"] as const
export const CONFIDENCE_LEVELS = ["expert", "strong", "familiar"] as const

// FX rates — hardcoded, static. Update by editing this file and shipping.
// Coarse filter + max-of-range absorbs ~2% weekly drift on majors.
export const FX_TO_USD: Record<string, number> = {
  USD: 1.0,
  EUR: 1.08,
  GBP: 1.27,
  CAD: 0.73,
  INR: 0.012,
  AUD: 0.66,
}

// Enumerated flags — LLM judge contract must handle each.
export const FLAGS = {
  SPONSORSHIP_UNCLEAR: "sponsorship_unclear",
  CLEARANCE_UNCLEAR: "clearance_unclear",
  REMOTE_UNCLEAR: "remote_unclear",
  EMPLOYMENT_TYPE_UNCLEAR: "employment_type_unclear",
  EDUCATION_UNPARSED: "education_unparsed",
  SENIORITY_ADJACENT: "seniority_adjacent",
  YEARS_EXPERIENCE_MISSING: "years_experience_missing",
  OVERQUALIFIED: "overqualified",
  COMPENSATION_MISSING: "compensation_missing",
  COMPENSATION_INTERVAL_MISSING: "compensation_interval_missing",
  CURRENCY_UNSUPPORTED: "currency_unsupported",
  SOURCE_SCORE_INVALID: "source_score_invalid",
  POSTED_AT_MISSING: "posted_at_missing",
  STALE_POSTING: "stale_posting",
  JUDGE_FAILED: "judge_failed",
  THIRD_PARTY_CONTRACT: "third_party_contract",
} as const

// Destinations for final verdicts.
export const BUCKETS = {
  COVER_LETTER: "cover_letter",
  RESULTS: "results",
  REVIEW_QUEUE: "review_queue",
  ARCHIVE: "archive",
} as const

// Per-source match-score boost. Missing keys = 0 weight.
export const SOURCE_SCORE_WEIGHT_BY_SITE: Record<string, number> = {
  jobright: 0.1,
}
