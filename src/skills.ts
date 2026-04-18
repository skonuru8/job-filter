/**
 * Normalize a skill name to its canonical form.
 * Invariant: any skill.name at or downstream of the scoring layer
 * has been run through this function.
 *
 * Aliases are loaded once at pipeline-run start from skill-aliases.json
 * and passed to every call site. Loading per-job would thrash.
 */
export function normalizeSkill(
  name: string | null | undefined,
  aliases: Record<string, string>
): string {
  if (!name || typeof name !== "string") return name as string
  const key = name.trim().toLowerCase()
  return aliases[key] ?? key
}

/**
 * Build the flat alias → canonical map from config/skills.json.
 *
 * Input format (config/skills.json):
 *   {
 *     "spring_boot": {
 *       "canonical": "Spring Boot",
 *       "aliases": ["spring boot", "springboot", "spring-boot", ...]
 *     }
 *   }
 *
 * Output format (what normalizeSkill expects):
 *   {
 *     "spring boot":  "spring boot",
 *     "springboot":   "spring boot",
 *     "spring-boot":  "spring boot",
 *     "k8s":          "kubernetes",
 *     "js":           "javascript",
 *     ...
 *   }
 *
 * Call once at pipeline start, pass the result to every normalizeSkill call.
 */
export function buildAliasMap(
  skillsJson: Record<string, { canonical: string; aliases: string[] }>
): Record<string, string> {
  const map: Record<string, string> = {}

  for (const entry of Object.values(skillsJson)) {
    const canonical = entry.canonical.trim().toLowerCase()
    for (const alias of entry.aliases) {
      map[alias.trim().toLowerCase()] = canonical
    }
  }

  return map
}