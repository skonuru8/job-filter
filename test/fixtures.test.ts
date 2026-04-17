import { describe, it, expect } from "vitest"
import { hardFilter } from "../src/filter.ts"
import { sanitizeJob } from "../src/sanitize.ts"
import { postFetchChecks } from "../src/post-fetch.ts"
import {
  baseJob,
  baseProfile,
  deepMerge,
  loadFixtures,
  type HardFilterFixture,
  type SanitizeFixture,
  type PostFetchFixture,
} from "./helpers.ts"

describe("hardFilter fixtures", () => {
  const fixtures = loadFixtures<HardFilterFixture>("hard-filter")

  for (const fx of fixtures) {
    it(fx.name, () => {
      const job = deepMerge(baseJob(), fx.job ?? {})
      const profile = deepMerge(baseProfile(), fx.profile ?? {})
      const result = hardFilter(job, profile)

      expect(result.verdict).toBe(fx.expected.verdict)
      expect(result.reason).toBe(fx.expected.reason)
      // Order-independent comparison for flags
      expect([...result.flags].sort()).toEqual([...fx.expected.flags].sort())
    })
  }
})

describe("sanitize fixtures", () => {
  const fixtures = loadFixtures<SanitizeFixture>("sanitize")

  for (const fx of fixtures) {
    it(fx.name, () => {
      // Sanitize fixtures may pass string sentinel values for testing type guards.
      // Apply the override directly so TS type system doesn't block us.
      const raw: any = deepMerge(baseJob(), fx.job ?? {})
      const result = sanitizeJob(raw)

      expect(result.meta.source_score).toBe(fx.expected.source_score)

      const baseFlags = (fx.job?.meta?.flags ?? baseJob().meta.flags) as string[]
      const addedFlags = result.meta.flags.filter((f) => !baseFlags.includes(f))
      expect([...addedFlags].sort()).toEqual([...fx.expected.added_flags].sort())
    })
  }
})

describe("post-fetch fixtures", () => {
  const fixtures = loadFixtures<PostFetchFixture>("post-fetch")

  for (const fx of fixtures) {
    it(fx.name, () => {
      const job = deepMerge(baseJob(), fx.job ?? {})
      const nowIso = fx.nowIso ?? "2026-04-17T00:00:00Z"
      const flags = postFetchChecks(job, nowIso)
      expect([...flags].sort()).toEqual([...fx.expected_flags].sort())
    })
  }
})
