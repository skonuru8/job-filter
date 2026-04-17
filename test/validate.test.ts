import { describe, it, expect } from "vitest"
import { validateProfile } from "../src/validate.ts"
import { baseProfile, deepMerge } from "./helpers.ts"
import type { Profile } from "../src/types.ts"

function withOverride(override: any): Profile {
  return deepMerge(baseProfile(), override) as Profile
}

describe("validateProfile", () => {
  it("accepts the baseline profile", () => {
    expect(() => validateProfile(baseProfile())).not.toThrow()
  })

  it("throws on empty acceptable_seniority", () => {
    expect(() => validateProfile(withOverride({ acceptable_seniority: [] })))
      .toThrow(/acceptable_seniority/)
  })

  it("throws on empty acceptable_employment", () => {
    expect(() => validateProfile(withOverride({ acceptable_employment: [] })))
      .toThrow(/acceptable_employment/)
  })

  it("throws on empty location.acceptable_types", () => {
    expect(() => validateProfile(withOverride({ location: { acceptable_types: [] } })))
      .toThrow(/acceptable_types/)
  })

  it("throws on invalid seniority enum value", () => {
    expect(() => validateProfile(withOverride({ acceptable_seniority: ["wizard"] })))
      .toThrow(/seniority/)
  })

  it("throws on invalid employment type", () => {
    expect(() => validateProfile(withOverride({ acceptable_employment: ["freelance_adventure"] })))
      .toThrow(/employment/)
  })

  it("throws on invalid location type", () => {
    expect(() => validateProfile(withOverride({ location: { acceptable_types: ["beach"] } })))
      .toThrow(/location type/)
  })

  it("throws on invalid education degree", () => {
    expect(() => validateProfile(withOverride({ education: { degree: "kindergarten", field: "X" } })))
      .toThrow(/degree/)
  })

  it("throws on non-number min_acceptable", () => {
    expect(() => validateProfile(withOverride({ compensation: { min_acceptable: "lots", currency: "USD", interval: "annual" } })))
      .toThrow(/min_acceptable/)
  })

  it("throws on negative min_acceptable", () => {
    expect(() => validateProfile(withOverride({ compensation: { min_acceptable: -1000, currency: "USD", interval: "annual" } })))
      .toThrow(/min_acceptable/)
  })

  it("throws on invalid compensation interval", () => {
    expect(() => validateProfile(withOverride({ compensation: { min_acceptable: 100000, currency: "USD", interval: "biweekly" } })))
      .toThrow(/interval/)
  })

  it("throws on profile currency not in FX map", () => {
    expect(() => validateProfile(withOverride({ compensation: { min_acceptable: 100000, currency: "SGD", interval: "annual" } })))
      .toThrow(/FX map/)
  })

  it("throws on non-number years_experience", () => {
    expect(() => validateProfile(withOverride({ years_experience: "many" })))
      .toThrow(/years_experience/)
  })

  it("throws on negative years_experience", () => {
    expect(() => validateProfile(withOverride({ years_experience: -2 })))
      .toThrow(/years_experience/)
  })
})
