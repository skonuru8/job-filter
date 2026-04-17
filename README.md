# job-filter

Hard-filter stage for the job pipeline. Implements the v4 design
(see `design-v4.md` at the repo root).

## What this is

A pure-function TypeScript module that takes a scraped job + a user profile
and decides whether the job survives the structured-field filter. Runs
*before* JD fetch, so it's cheap and fast — the goal is to reject 60–70%
of scraped jobs using only listing metadata.

No dependencies beyond Vitest for testing.

## What this is not

This is one stage in the larger pipeline. Out of scope here:

- Scraping (JobSpy + custom scrapers)
- Within-site and cross-site deduplication
- JD fetch + structured extraction
- Embedding generation
- Deterministic scoring layer
- LLM judge and cover letter generation
- Orchestration (BullMQ, scheduler)

Each has its own spec and will land as a sibling module.

## Running

```bash
npm install
npm test          # one-shot
npm run test:watch
```

## Structure

```
src/
  constants.ts    # FLAGS, BUCKETS, level maps, FX table, enums
  types.ts        # Job, Profile, FilterResult types
  validate.ts     # validateProfile — runs once at profile load
  sanitize.ts     # sanitizeJob — clamps suspect values
  filter.ts       # hardFilter — the pure-function core
  post-fetch.ts   # postFetchChecks — runs after JD fetch
  compensation.ts # toAnnualUSD, applySourceScore
  skills.ts       # normalizeSkill
  index.ts        # public exports

fixtures/
  hard-filter/    # 33 fixtures: every reject reason + every flag + edge cases
  sanitize/       # 4 fixtures: source_score validation
  post-fetch/     # 7 fixtures: education regex + posted_at handling

test/
  helpers.ts      # baseJob, baseProfile, deepMerge, fixture loader
  fixtures.test.ts  # loads JSON fixtures, runs them against the code
  validate.test.ts  # validateProfile throws on bad config
  purity.test.ts    # hardFilter does not mutate inputs
```

## Fixture format

Each fixture is a JSON file containing only the fields that differ from
the baseline. The test runner deep-merges the override onto `baseJob()`
and `baseProfile()` before calling the function under test.

```json
{
  "name": "reject: no_sponsorship when job says no and profile requires it",
  "job": { "visa_sponsorship": false },
  "profile": { "work_authorization": { "requires_sponsorship": true } },
  "expected": { "verdict": "REJECT", "reason": "no_sponsorship", "flags": [] }
}
```

Flag comparisons are order-independent (sorted before equality check).

## Using the module

```typescript
import {
  validateProfile,
  sanitizeJob,
  hardFilter,
  postFetchChecks,
  normalizeSkill,
} from "./src/index.ts"

// At profile load
validateProfile(profile)   // throws if config is invalid

// At pipeline-run start
const nowIso = new Date().toISOString()
const aliases = loadAliases()  // from skill-aliases.json

// Per job
const sanitized = sanitizeJob(rawJob)
const hard = hardFilter(sanitized, profile)
sanitized.meta.flags = [...sanitized.meta.flags, ...hard.flags]

if (hard.verdict === "REJECT") return { job: sanitized, outcome: hard }

sanitized.description_raw = await fetchJD(sanitized.meta.source_url)

const postFlags = postFetchChecks(sanitized, nowIso)
sanitized.meta.flags = [...sanitized.meta.flags, ...postFlags]

sanitized.required_skills = sanitized.required_skills.map(s => ({
  ...s,
  name: normalizeSkill(s.name, aliases),
}))

// → scoring → judge → bucket routing (separate modules)
```

## Adding new fixtures

Something broke in production, or a scraper surfaced an edge case? Write
a fixture first, then fix the code:

1. Create a JSON file in the appropriate `fixtures/*` subdirectory.
2. Specify only the fields that differ from the baseline.
3. Set `expected` to what the correct behavior should be.
4. `npm test` — it should fail.
5. Fix the code.
6. `npm test` — green.

This is the loop. Don't "fix" something without a fixture capturing the bug.

## Design decisions worth knowing

- **`hardFilter` is a pure function.** No mutation, no I/O. All flag
  accumulation happens in a local array, and the returned `flags` is a
  fresh copy so callers can't leak state back into the function.
- **Profile validation runs once at load, not per job.** Invalid profile
  data reaching `hardFilter` is a programming error, not runtime input.
  This eliminates entire classes of NaN bugs.
- **Unknown enum values on the job side → flag, not reject.** Extractor
  failures shouldn't cause false negatives. If the scraper returns
  `location.type: "flexible"`, we flag `remote_unclear` and let the LLM
  judge resolve it.
- **Asymmetric YOE tolerance.** +2 under (postings inflate requirements),
  +3 over (overqualified is a softer signal than underqualified).
- **FX rates are hardcoded and static.** Coarse filter, max-of-range
  absorbs weekly drift. Update by editing `constants.ts` and shipping.

See `design-v4.md` for the full rationale.
