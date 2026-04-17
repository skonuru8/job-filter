export interface JobMeta {
  job_id: string
  schema_version: string
  source_site: string
  source_url: string
  source_score: number | null
  posted_at: string | null
  scraped_at: string
  run_id: string
  flags: string[]
}

export interface Compensation {
  min: number | null
  max: number | null
  currency: string | null
  interval: string | null
}

export interface JobLocation {
  type: string | null
  timezone: string | null
  cities: string[]
  countries: string[]
}

export interface RequiredSkill {
  name: string
  years_required: number | null
  importance: string
  category: string
}

export interface Job {
  meta: JobMeta
  title: string
  seniority: string
  employment_type: string | null
  company: { name: string; type: string }
  location: JobLocation
  compensation: Compensation
  required_skills: RequiredSkill[]
  years_experience: { min: number | null; max: number | null }
  education_required: { minimum: string; field: string }
  visa_sponsorship: boolean | null
  security_clearance: string
  domain: string | null
  responsibilities: string[]
  description_raw: string
}

export interface ProfileCompensation {
  min_acceptable: number
  currency: string
  interval: string
}

export interface Profile {
  meta: {
    profile_id: string
    schema_version: string
    version: string
    last_updated: string
  }
  target_titles: string[]
  acceptable_seniority: string[]
  acceptable_employment: string[]
  location: {
    current_city: string
    current_country: string
    timezone: string
    acceptable_types: string[]
    acceptable_cities: string[]
    acceptable_countries: string[]
    willing_to_relocate: boolean
  }
  compensation: ProfileCompensation
  skills: Array<{
    name: string
    years: number
    confidence: string
    category: string
  }>
  years_experience: number
  education: { degree: string; field: string }
  work_authorization: {
    requires_sponsorship: boolean
    visa_type: string
    clearance_eligible: boolean
  }
  preferred_domains: string[]
  deal_breakers: string[]
}

export type Verdict = "PASS" | "REJECT"

export interface FilterResult {
  verdict: Verdict
  reason: string | null
  flags: string[]
}
