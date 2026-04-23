import fs   from "fs";
import path from "path";

const inputPath = process.argv[2];
if (!inputPath) {
  console.error("Usage: npx tsx scripts/sort-log.ts <path/to/raw.log>");
  process.exit(1);
}

const lines = fs.readFileSync(inputPath, "utf8").split("\n");

const meta:    string[]              = [];
const jobs:    Map<number, string[]> = new Map();
const summary: string[]              = [];

let inSummary = false;

for (const line of lines) {
  if (line.includes("jobs processed") || line.startsWith("─────")) {
    inSummary = true;
  }
  if (inSummary && !line.includes("jobs processed") && !line.startsWith("─────") && line.startsWith("  ")) {
    // already inside summary — fall through
  }

  if (inSummary) {
    summary.push(line);
    continue;
  }

  const m = line.match(/\[pipeline\]\s+\[(\d+)\]/);
  if (m) {
    const idx = Number(m[1]);
    if (!jobs.has(idx)) jobs.set(idx, []);
    jobs.get(idx)!.push(line);
  } else {
    meta.push(line);
  }
}

const out: string[] = [];

out.push(...meta, "");

const sortedKeys = [...jobs.keys()].sort((a, b) => a - b);
for (const key of sortedKeys) {
  out.push(...jobs.get(key)!, "");
}

out.push(...summary);

const outPath = path.join(path.dirname(inputPath), "pipeline.log");
fs.writeFileSync(outPath, out.join("\n"), "utf8");
console.log(`Written: ${outPath}`);
