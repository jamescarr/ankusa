// Validate conformance/ and run every registered packages/sdk-* against its
// vectors. Node 22 ESM, stdlib only. `mise run check:conformance` calls this.
import { spawn } from "node:child_process";
import { readdirSync, readFileSync } from "node:fs";
import { availableParallelism } from "node:os";
import { fileURLToPath } from "node:url";

const ROOT = new URL("..", import.meta.url);
const CONFORMANCE = new URL("conformance/", ROOT);
const PACKAGES = new URL("packages/", ROOT);

const readJson = (url) => JSON.parse(readFileSync(url, "utf8"));

const featuresFile = readJson(new URL("features.json", CONFORMANCE));
const sdksFile = readJson(new URL("sdks.json", CONFORMANCE));
const features = featuresFile.features ?? [];
const operations = new Set(featuresFile.operations ?? []);

const errors = [];

const featureIds = new Set();
for (const feature of features) {
  if (featureIds.has(feature.id)) errors.push(`duplicate feature id "${feature.id}"`);
  featureIds.add(feature.id);
}

const seenCaseIds = new Set();
const caseCounts = new Map();
let caseCount = 0;

const caseFiles = readdirSync(new URL("cases/", CONFORMANCE))
  .filter((name) => name.endsWith(".json"))
  .sort();
for (const file of caseFiles) {
  const cases = readJson(new URL(`cases/${file}`, CONFORMANCE)).cases ?? [];
  cases.forEach((raw, i) => {
    caseCount += 1;
    const c = raw !== null && typeof raw === "object" ? raw : {};
    for (const field of ["id", "feature", "operation", "input", "expect"]) {
      if (!(field in c)) errors.push(`case #${i} (${file}): missing "${field}"`);
    }
    if (!("id" in c)) return;
    if (seenCaseIds.has(c.id)) errors.push(`duplicate case id "${c.id}" (${file})`);
    seenCaseIds.add(c.id);
    if ("feature" in c) {
      if (!featureIds.has(c.feature)) errors.push(`case "${c.id}" (${file}): unknown feature "${c.feature}"`);
      else caseCounts.set(c.feature, (caseCounts.get(c.feature) ?? 0) + 1);
    }
    if ("operation" in c && !operations.has(c.operation)) {
      errors.push(`case "${c.id}" (${file}): unknown operation "${c.operation}"`);
    }
    if ("expect" in c) {
      const expect = c.expect ?? {};
      const hasOk = "ok" in expect;
      const hasError = "error" in expect;
      if (hasOk === hasError) {
        errors.push(`case "${c.id}" (${file}): expect must have exactly one of "ok" or "error"`);
      } else if (hasError && typeof expect.error?.class !== "string") {
        errors.push(`case "${c.id}" (${file}): expect.error.class must be a string`);
      }
    }
  });
}

for (const feature of features) {
  if (!caseCounts.has(feature.id)) errors.push(`feature "${feature.id}" has no cases`);
}

const packageNames = readdirSync(PACKAGES, { withFileTypes: true })
  .filter((entry) => entry.isDirectory() && entry.name.startsWith("sdk-"))
  .map((entry) => entry.name);
for (const name of packageNames) {
  if (!(name in sdksFile.sdks)) errors.push(`packages/${name} is not registered in conformance/sdks.json`);
}
for (const name of Object.keys(sdksFile.sdks)) {
  if (!packageNames.includes(name)) {
    errors.push(`conformance/sdks.json registers ${name}, but packages/${name} does not exist`);
  }
}

if (errors.length > 0) {
  const prefix = process.env.GITHUB_ACTIONS === "true" ? "::error::" : "";
  for (const message of errors) console.log(`${prefix}${message}`);
  process.exit(1);
}

console.log(`${features.length} features, ${caseCount} cases`);
for (const feature of features) {
  console.log(`  ${feature.id}  ${caseCounts.get(feature.id) ?? 0}`);
}

// The SDKs are independent: each builds and tests in its own directory with its
// own toolchain, so they run concurrently instead of one after another. A cold
// runner spends most of this check waiting on a package manager; the pool is
// bounded because ten toolchains at once thrash a four-core runner's disk and
// memory. Every SDK still runs to completion, so one run reports every failure.
// The checks themselves, their order within an SDK, and the verdict are
// unchanged.
//
// CONFORMANCE_JOBS=1 serialises them. The local `mise run check` sets that: its
// conformance stage overlaps the docker-compose adapters, and
// ankusa_redis's "a Redis that answers in 500 ms holds an ack for the budget"
// asserts a p99 on a quiet machine (measured 304 ms against a 150 ms bound
// when four SDK builds ran beside it).
const CONCURRENCY = Math.max(
  1,
  Math.min(Number(process.env.CONFORMANCE_JOBS) || 4, availableParallelism()),
);
const queue = Object.keys(sdksFile.sdks).sort();
const results = new Map();

async function runSdk(name) {
  const { setup = [], run } = sdksFile.sdks[name];
  const cwd = fileURLToPath(new URL(`packages/${name}`, ROOT));
  for (const [command, ...args] of [...setup, run]) {
    const code = await new Promise((resolve) => {
      const child = spawn(command, args, { cwd, stdio: "inherit" });
      child.on("error", () => resolve(1));
      child.on("close", (status, signal) => resolve(status ?? (signal ? 1 : 0)));
    });
    if (code !== 0) return code;
  }
  return 0;
}

await Promise.all(
  Array.from({ length: Math.min(CONCURRENCY, queue.length) }, async () => {
    while (queue.length > 0) {
      const name = queue.shift();
      results.set(name, await runSdk(name));
    }
  }),
);

let failed = false;
for (const name of [...results.keys()].sort()) {
  const code = results.get(name);
  if (code === 0) {
    console.log(`${name}  pass`);
  } else {
    failed = true;
    console.log(`${name}  FAIL (exit ${code})`);
  }
}
process.exit(failed ? 1 : 0);
