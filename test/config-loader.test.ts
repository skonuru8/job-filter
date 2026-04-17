import { describe, it, expect } from "vitest"
import { resolve } from "node:path"
import {
  loadConfig,
  buildAliasMap,
  buildProfileSkills,
  type SkillDictionary,
} from "../src/config-loader.ts"
import { normalizeSkill } from "../src/skills.ts"

const CONFIG_DIR = resolve(process.cwd(), "..", "config")

describe("loadConfig", () => {
  it("loads profile.json + skills.json and produces a valid merged profile", () => {
    const { profile, aliases, dictionary } = loadConfig(CONFIG_DIR)

    expect(profile.meta.profile_id).toBe("sarath_konuru")
    expect(profile.skills.length).toBeGreaterThan(50)
    expect(Object.keys(dictionary).length).toBeGreaterThan(50)
    expect(Object.keys(aliases).length).toBeGreaterThan(200)
  })

  it("profile.skills is derived from skills.json — every skill appears by canonical-lowercased name", () => {
    const { profile, dictionary } = loadConfig(CONFIG_DIR)
    const skillNames = new Set(profile.skills.map((s) => s.name))

    for (const entry of Object.values(dictionary)) {
      expect(skillNames.has(entry.canonical.toLowerCase())).toBe(true)
    }
  })

  it("alias map resolves common aliases to the canonical form used in profile.skills", () => {
    const { aliases } = loadConfig(CONFIG_DIR)

    expect(normalizeSkill("K8s", aliases)).toBe("kubernetes")
    expect(normalizeSkill("k8s", aliases)).toBe("kubernetes")
    expect(normalizeSkill("Kubernetes", aliases)).toBe("kubernetes")
    expect(normalizeSkill("JS", aliases)).toBe("javascript")
    expect(normalizeSkill("TS", aliases)).toBe("typescript")
    expect(normalizeSkill("springboot", aliases)).toBe("spring boot")
    expect(normalizeSkill("amazon dynamodb", aliases)).toBe("dynamodb")
  })

  it("unknown skills fall through to lowercased original (no alias match)", () => {
    const { aliases } = loadConfig(CONFIG_DIR)
    expect(normalizeSkill("Rust", aliases)).toBe("rust")
    expect(normalizeSkill("Haskell", aliases)).toBe("haskell")
  })
})

describe("buildAliasMap", () => {
  it("includes every alias plus the canonical itself", () => {
    const dict: SkillDictionary = {
      foo: {
        canonical: "Foo",
        aliases: ["foo", "f", "phoo"],
        category: "tool",
        years: 1,
        confidence: "familiar",
      },
    }
    const aliases = buildAliasMap(dict)
    expect(aliases["foo"]).toBe("foo")
    expect(aliases["f"]).toBe("foo")
    expect(aliases["phoo"]).toBe("foo")
  })

  it("lowercases both aliases and canonicals", () => {
    const dict: SkillDictionary = {
      foo: {
        canonical: "FOO",
        aliases: ["FOO", "BAR"],
        category: "tool",
        years: 1,
        confidence: "familiar",
      },
    }
    const aliases = buildAliasMap(dict)
    expect(aliases["foo"]).toBe("foo")
    expect(aliases["bar"]).toBe("foo")
  })
})

describe("buildProfileSkills", () => {
  it("produces one skill per dictionary entry with fields copied through", () => {
    const dict: SkillDictionary = {
      foo: {
        canonical: "Foo",
        aliases: [],
        category: "tool",
        years: 3,
        confidence: "strong",
      },
      bar: {
        canonical: "Bar",
        aliases: [],
        category: "language",
        years: 1,
        confidence: "familiar",
      },
    }
    const skills = buildProfileSkills(dict)
    expect(skills).toHaveLength(2)
    expect(skills[0]).toEqual({ name: "foo", years: 3, confidence: "strong", category: "tool" })
    expect(skills[1]).toEqual({ name: "bar", years: 1, confidence: "familiar", category: "language" })
  })
})
