import {
  SENIORITY_LEVEL,
  DEGREE_LEVEL,
  LOCATION_TYPES,
  EMPLOYMENT_TYPES,
  CLEARANCE_LEVELS,
  COMPENSATION_INTERVALS,
  FX_TO_USD,
  FLAGS,
} from "./constants.ts"
import type { Job, Profile, FilterResult } from "./types.ts"
import { toAnnualUSD } from "./compensation.ts"

/**
 * Hard filter — listing-metadata-only checks. PURE: does not mutate inputs.
 *
 * Profile is assumed already validated by validateProfile(). Invalid profile
 * data reaching this function is a programming error, not runtime input.
 */
export function hardFilter(job: Job, profile: Profile): FilterResult {
  const flags = [...(job.meta?.flags ?? [])]
  const reject = (reason: string): FilterResult => ({
    verdict: "REJECT",
    reason,
    flags: [...flags],
  })

  // ── RULE 1 — Visa ──────────────────────────────────────────────
  if (
    job.visa_sponsorship === false &&
    profile.work_authorization.requires_sponsorship
  ) {
    return reject("no_sponsorship")
  }
  if (
    job.visa_sponsorship === null &&
    profile.work_authorization.requires_sponsorship
  ) {
    flags.push(FLAGS.SPONSORSHIP_UNCLEAR)
  }

  // ── RULE 2 — Clearance ─────────────────────────────────────────
  const clearance = job.security_clearance
  if (
    clearance &&
    !CLEARANCE_LEVELS.includes(clearance as (typeof CLEARANCE_LEVELS)[number])
  ) {
    flags.push(FLAGS.CLEARANCE_UNCLEAR)
  } else if (
    clearance &&
    clearance !== "none" &&
    !profile.work_authorization.clearance_eligible
  ) {
    return reject("clearance_required")
  }

  // ── RULE 3 — Location ──────────────────────────────────────────
  // 3a: null OR unknown-but-truthy location.type → flag, not reject
  const locType = job.location?.type
  if (
    !locType ||
    !LOCATION_TYPES.includes(locType as (typeof LOCATION_TYPES)[number])
  ) {
    flags.push(FLAGS.REMOTE_UNCLEAR)
  } else if (!profile.location.acceptable_types.includes(locType)) {
    return reject("location_type_mismatch")
  }

  // 3b: for onsite/hybrid, check country (harder boundary) then city.
  // Empty geo data on onsite/hybrid → flag (suspicious) rather than silent pass.
  if (locType === "onsite" || locType === "hybrid") {
    const norm = (s: string | null | undefined) =>
      (s ?? "").trim().toLowerCase()
    const jobCountries = (job.location?.countries ?? [])
      .map(norm)
      .filter(Boolean)
    const jobCities = (job.location?.cities ?? []).map(norm).filter(Boolean)
    const okCountries = (profile.location.acceptable_countries ?? []).map(norm)
    const okCities = (profile.location.acceptable_cities ?? []).map(norm)

    if (jobCountries.length === 0 && jobCities.length === 0) {
      flags.push(FLAGS.REMOTE_UNCLEAR)
    } else {
      if (
        jobCountries.length > 0 &&
        okCountries.length > 0 &&
        !jobCountries.some((c) => okCountries.includes(c))
      ) {
        return reject("location_country_mismatch")
      }
      if (
        okCities.length > 0 &&
        jobCities.length > 0 &&
        !jobCities.some((c) => okCities.includes(c))
      ) {
        return reject("location_city_mismatch")
      }
    }
  }

  // ── RULE 4 — Seniority ─────────────────────────────────────────
  const jobLevel = SENIORITY_LEVEL[job.seniority]
  if (jobLevel === undefined) return reject("unknown_seniority")

  const profileLevels = profile.acceptable_seniority.map(
    (s) => SENIORITY_LEVEL[s]
  )
  const minLvl = Math.min(...profileLevels)
  const maxLvl = Math.max(...profileLevels)

  if (jobLevel < minLvl) return reject("seniority_below_floor")
  if (jobLevel > maxLvl + 1) return reject("seniority_above_ceiling")
  if (jobLevel === maxLvl + 1) flags.push(FLAGS.SENIORITY_ADJACENT)

  // ── RULE 5 — Education (structured field only; regex in §8) ────
  const jobMin = job.education_required?.minimum?.trim() || null
  if (jobMin) {
    if (!(jobMin in DEGREE_LEVEL)) return reject("unknown_education_value")
    if (DEGREE_LEVEL[jobMin] > DEGREE_LEVEL[profile.education.degree]) {
      return reject("education_mismatch")
    }
  }

  // ── RULE 6 — Employment type ───────────────────────────────────
  if (!job.employment_type) {
    flags.push(FLAGS.EMPLOYMENT_TYPE_UNCLEAR)
  } else if (
    !EMPLOYMENT_TYPES.includes(
      job.employment_type as (typeof EMPLOYMENT_TYPES)[number]
    )
  ) {
    flags.push(FLAGS.EMPLOYMENT_TYPE_UNCLEAR)
  } else if (!profile.acceptable_employment.includes(job.employment_type)) {
    return reject("employment_type_mismatch")
  }

  // ── RULE 7 — Years of experience ───────────────────────────────
  // Asymmetric tolerance:
  //   +2 under  — postings inflate requirements; candidates 2 years short often succeed.
  //   +3 over   — overqualified is a softer signal than underqualified; flag, don't reject.
  const jobYoeMin = job.years_experience?.min
  const jobYoeMax = job.years_experience?.max
  if (jobYoeMin == null && jobYoeMax == null) {
    flags.push(FLAGS.YEARS_EXPERIENCE_MISSING)
  } else {
    if (jobYoeMin != null && jobYoeMin > profile.years_experience + 2) {
      return reject("years_experience_gap")
    }
    if (jobYoeMax != null && profile.years_experience > jobYoeMax + 3) {
      flags.push(FLAGS.OVERQUALIFIED)
    }
  }

  // ── RULE 8 — Compensation ──────────────────────────────────────
  const jobVal = job.compensation?.max ?? job.compensation?.min ?? null
  const jobInterval = job.compensation?.interval
  const jobCurrency = job.compensation?.currency

  if (jobVal === null) {
    flags.push(FLAGS.COMPENSATION_MISSING)
  } else if (
    !jobInterval ||
    !COMPENSATION_INTERVALS.includes(
      jobInterval as (typeof COMPENSATION_INTERVALS)[number]
    )
  ) {
    flags.push(FLAGS.COMPENSATION_INTERVAL_MISSING)
  } else if (
    !jobCurrency ||
    FX_TO_USD[jobCurrency.toUpperCase()] === undefined
  ) {
    flags.push(FLAGS.CURRENCY_UNSUPPORTED)
  } else {
    const jobUSD = toAnnualUSD(jobVal, jobInterval, jobCurrency)
    const profileUSD = toAnnualUSD(
      profile.compensation.min_acceptable,
      profile.compensation.interval,
      profile.compensation.currency
    )
    if (jobUSD !== null && profileUSD !== null && jobUSD < profileUSD) {
      return reject("below_comp_floor")
    }
  }

  return { verdict: "PASS", reason: null, flags: [...flags] }
}
