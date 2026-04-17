import { describe, it, expect } from "vitest"
import { hardFilter } from "../src/filter.ts"
import { baseJob, baseProfile } from "./helpers.ts"

describe("hardFilter purity", () => {
  it("does not mutate the input job object across repeated calls", () => {
    const job = baseJob()
    const profile = baseProfile()
    const snapshot = JSON.stringify(job)

    hardFilter(job, profile)
    hardFilter(job, profile)
    hardFilter(job, profile)

    expect(JSON.stringify(job)).toBe(snapshot)
  })

  it("does not mutate the input profile object", () => {
    const job = baseJob()
    const profile = baseProfile()
    const snapshot = JSON.stringify(profile)

    hardFilter(job, profile)
    expect(JSON.stringify(profile)).toBe(snapshot)
  })

  it("returned flags array is a fresh copy — caller cannot mutate internal state", () => {
    const job = baseJob()
    job.visa_sponsorship = null
    const profile = baseProfile()
    profile.work_authorization.requires_sponsorship = true

    const result = hardFilter(job, profile)
    expect(result.flags).toContain("sponsorship_unclear")

    // Mutate the returned flags — should not affect a subsequent call
    result.flags.push("injected_by_caller")

    const result2 = hardFilter(job, profile)
    expect(result2.flags).not.toContain("injected_by_caller")
  })

  it("same input produces the same output (determinism)", () => {
    const job = baseJob()
    const profile = baseProfile()
    const a = hardFilter(job, profile)
    const b = hardFilter(job, profile)
    expect(a).toEqual(b)
  })
})
