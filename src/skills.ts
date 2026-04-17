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
