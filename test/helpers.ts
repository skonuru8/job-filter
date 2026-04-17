import { readFileSync, readdirSync } from "node:fs"
import { join } from "node:path"
import type { Job, Profile } from "../src/types.ts"

/**
 * A baseline valid job. Fixtures override only the fields under test.
 * This job PASSES hardFilter against baseProfile with no flags.
 */
export function baseJob(): Job {
  return {
    meta: {
      job_id: "base_job_1",
      schema_version: "1.0.0",
      source_site: "indeed",
      source_url: "https://example.com/jobs/base",
      source_score: null,
      posted_at: "2026-04-15T00:00:00Z",
      scraped_at: "2026-04-17T00:00:00Z",
      run_id: "run_test",
      flags: [],
    },
    title: "Senior Software Engineer",
    seniority: "senior",
    employment_type: "full_time",
    company: { name: "Acme Inc", type: "product" },
    location: {
      type: "remote",
      timezone: "America/New_York",
      cities: [],
      countries: ["USA"],
    },
    compensation: {
      min: 140000,
      max: 180000,
      currency: "USD",
      interval: "annual",
    },
    required_skills: [
      {
        name: "typescript",
        years_required: 3,
        importance: "must",
        category: "language",
      },
    ],
    years_experience: { min: 4, max: 8 },
    education_required: { minimum: "bachelor", field: "" },
    visa_sponsorship: true,
    security_clearance: "none",
    domain: "fintech",
    responsibilities: ["Build things"],
    description_raw: "We are hiring a Senior Software Engineer. BS required.",
  }
}

/**
 * A baseline valid profile. Fixtures override only the fields under test.
 */
export function baseProfile(): Profile {
  return {
    meta: {
      profile_id: "test_profile",
      schema_version: "1.0.0",
      version: "1",
      last_updated: "2026-04-17T00:00:00Z",
    },
    target_titles: ["Software Engineer"],
    acceptable_seniority: ["mid", "senior", "staff"],
    acceptable_employment: ["full_time", "contract_to_hire"],
    location: {
      current_city: "New York",
      current_country: "USA",
      timezone: "America/New_York",
      acceptable_types: ["remote", "hybrid"],
      acceptable_cities: ["New York", "San Francisco"],
      acceptable_countries: ["USA"],
      willing_to_relocate: false,
    },
    compensation: {
      min_acceptable: 130000,
      currency: "USD",
      interval: "annual",
    },
    skills: [
      { name: "typescript", years: 5, confidence: "expert", category: "language" },
    ],
    years_experience: 6,
    education: { degree: "bachelor", field: "Computer Science" },
    work_authorization: {
      requires_sponsorship: false,
      visa_type: "citizen",
      clearance_eligible: true,
    },
    preferred_domains: ["fintech"],
    deal_breakers: [],
  }
}

/**
 * Deep merge — used to apply a fixture's override onto a base.
 * Arrays are REPLACED, not concatenated (fixture intent is almost always
 * "this specific array, not added to the base").
 */
export function deepMerge<T>(base: T, override: any): T {
  // undefined means "override not specified" → keep base
  if (override === undefined) return base
  // null means "override to null" → replace base
  if (override === null) return null as unknown as T
  if (Array.isArray(override)) return override as T
  if (typeof override !== "object") return override as T
  if (typeof base !== "object" || base === null) return override as T

  const out: any = Array.isArray(base) ? [...(base as any)] : { ...base }
  for (const key of Object.keys(override)) {
    out[key] = deepMerge((base as any)[key], override[key])
  }
  return out
}

export interface HardFilterFixture {
  name: string
  job?: Partial<Job>
  profile?: Partial<Profile>
  expected: {
    verdict: "PASS" | "REJECT"
    reason: string | null
    flags: string[]
  }
}

export interface PostFetchFixture {
  name: string
  job?: Partial<Job>
  nowIso?: string
  expected_flags: string[]
}

export interface SanitizeFixture {
  name: string
  job?: Partial<Job>
  expected: {
    source_score: number | null
    added_flags: string[]
  }
}

export function loadFixtures<T>(dir: string): T[] {
  const fullDir = join(process.cwd(), "fixtures", dir)
  const files = readdirSync(fullDir).filter((f) => f.endsWith(".json"))
  return files
    .map((f) => JSON.parse(readFileSync(join(fullDir, f), "utf-8")) as T)
    .sort((a: any, b: any) => a.name.localeCompare(b.name))
}
