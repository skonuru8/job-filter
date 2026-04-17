import { readFileSync } from "node:fs"
import { join, resolve } from "node:path"
import { validateProfile } from "./validate.ts"
import type { Profile } from "./types.ts"

/**
 * A single entry in skills.json.
 * This format merges alias resolution (for JD extraction) with the user's own
 * skill assessment (for profile.skills) in one source of truth.
 */
export interface SkillEntry {
  canonical: string            // display form, e.g. "Spring Boot"
  aliases: string[]            // strings that canonicalize to this skill
  category: string             // language | framework | cloud | tool | methodology
  years: number                // user's years with this skill
  confidence: string           // expert | strong | familiar
}

export type SkillDictionary = Record<string, SkillEntry>

/**
 * Derived from skills.json: maps lowercased alias → lowercased canonical.
 * Passed to normalizeSkill() throughout the pipeline.
 */
export type AliasMap = Record<string, string>

export interface LoadedConfig {
  profile: Profile
  aliases: AliasMap
  dictionary: SkillDictionary
}

/**
 * Convert skills.json dictionary into the alias map normalizeSkill expects.
 * Also maps the canonical to itself (identity), so normalizing "java" → "java".
 */
export function buildAliasMap(dict: SkillDictionary): AliasMap {
  const map: AliasMap = {}
  for (const entry of Object.values(dict)) {
    const canonical = entry.canonical.toLowerCase()
    map[canonical] = canonical
    for (const alias of entry.aliases) {
      map[alias.toLowerCase()] = canonical
    }
  }
  return map
}

/**
 * Derive profile.skills from skills.json.
 * Order preserved from the dictionary file — keep semantically grouped entries together.
 */
export function buildProfileSkills(dict: SkillDictionary): Profile["skills"] {
  return Object.values(dict).map((entry) => ({
    name: entry.canonical.toLowerCase(),
    years: entry.years,
    confidence: entry.confidence,
    category: entry.category,
  }))
}

/**
 * Load profile.json and skills.json, merge them, validate.
 * `configDir` defaults to ../config relative to the job-filter package root.
 *
 * Throws if either file is missing or if the merged profile fails validation.
 */
export function loadConfig(configDir?: string): LoadedConfig {
  const dir = configDir ?? resolve(process.cwd(), "..", "config")

  const profileRaw = JSON.parse(
    readFileSync(join(dir, "profile.json"), "utf-8")
  ) as Omit<Profile, "skills"> & { skills?: unknown }

  const dictionary = JSON.parse(
    readFileSync(join(dir, "skills.json"), "utf-8")
  ) as SkillDictionary

  // skills.json is the source of truth — ignore any skills[] in profile.json
  const skills = buildProfileSkills(dictionary)
  const profile: Profile = { ...(profileRaw as any), skills }

  validateProfile(profile)

  const aliases = buildAliasMap(dictionary)

  return { profile, aliases, dictionary }
}
