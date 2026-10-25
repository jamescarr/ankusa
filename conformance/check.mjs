// Validate conformance/ and run every registered packages/sdk-* against its
// vectors. Node 22 ESM, stdlib only. `mise run check:conformance` calls this.
import { spawn, spawnSync } from "node:child_process";
import { mkdtempSync, readdirSync, readFileSync, rmSync } from "node:fs";
import { availableParallelism, tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { isDeepStrictEqual } from "node:util";

const ROOT = new URL("..", import.meta.url);
const CONFORMANCE = new URL("conformance/", ROOT);
const PACKAGES = new URL("packages/", ROOT);

const readJson = (url) => JSON.parse(readFileSync(url, "utf8"));

const featuresFile = readJson(new URL("features.json", CONFORMANCE));
const sdksFile = readJson(new URL("sdks.json", CONFORMANCE));
const features = featuresFile.features ?? [];
const operations = new Set(featuresFile.operations ?? []);

// Errors the SDKs raise for input no retry can fix; a case that expects one
// must say `retryable: false` (the consumer failure policy).
const PERMANENT_ERRORS = new Set(["InvalidMessageError", "InvalidSignatureError"]);

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
      } else if (
        hasError &&
        PERMANENT_ERRORS.has(expect.error.class) &&
        expect.error.retryable !== false
      ) {
        errors.push(
          `case "${c.id}" (${file}): ${expect.error.class} is permanent (docs/delivery.md, Consumer failure policy): expect.error.retryable must be false`,
        );
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

// The producer vectors (cases/producer.json) are not hand-written: core's
// `mix ankusa.conformance.gen` encodes a fixed set of hooks with
// `Ankusa.Sink.Message.encode/3`. Regenerate them into a temporary file and
// require the checked-in one to be identical, so a field added to (or removed
// from, or changed in) the encoder without a vector update fails here, before
// any SDK is asked to decode a message the vectors do not know about.
function mix(args, env = {}) {
  return spawnSync("mix", args, {
    cwd: fileURLToPath(new URL("ankusa", PACKAGES)),
    env: { ...process.env, ...env },
    stdio: ["ignore", "inherit", "inherit"],
  });
}

function describeDifference(generated, checkedIn) {
  let now;
  let before;
  try {
    now = JSON.parse(generated.input.message);
    before = JSON.parse(checkedIn.input.message);
  } catch {
    return "its message is not JSON";
  }
  const added = Object.keys(now).filter((key) => !(key in before));
  const removed = Object.keys(before).filter((key) => !(key in now));
  const changed = Object.keys(now).filter((key) => key in before && !isDeepStrictEqual(now[key], before[key]));
  const parts = [];
  if (added.length > 0) parts.push(`message keys added: ${added.join(", ")}`);
  if (removed.length > 0) parts.push(`message keys removed: ${removed.join(", ")}`);
  if (changed.length > 0) parts.push(`message keys changed: ${changed.join(", ")}`);
  if (parts.length === 0) parts.push("the expected decode result differs");
  return parts.join("; ");
}

function producerVectorProblems() {
  const generatedFile = join(mkdtempSync(join(tmpdir(), "ankusa-producer-")), "producer.json");
  try {
    // Both envs share one compiled-deps cache (`.mise/lib/mix_cache.sh`); test
    // is the env `check:package ankusa` warms.
    const env = { MIX_ENV: "test" };
    for (const args of [
      ["local.hex", "--force", "--if-missing"],
      ["local.rebar", "--force", "--if-missing"],
      ["deps.get", "--check-locked"],
      ["ankusa.conformance.gen", "--out", generatedFile],
    ]) {
      const result = mix(args, env);
      if (result.error || result.status !== 0) {
        return [`\`mix ${args[0]}\` in packages/ankusa failed (${result.error?.message ?? `exit ${result.status}`})`];
      }
    }

    const generated = new Map(readJson(pathToFileURL(generatedFile)).cases.map((c) => [c.id, c]));
    const checkedIn = new Map(readJson(new URL("cases/producer.json", CONFORMANCE)).cases.map((c) => [c.id, c]));
    const problems = [];
    for (const id of generated.keys()) {
      if (!checkedIn.has(id)) problems.push(`producer vector "${id}" is generated but missing from cases/producer.json`);
    }
    for (const id of checkedIn.keys()) {
      if (!generated.has(id)) problems.push(`producer vector "${id}" is in cases/producer.json but no longer generated`);
    }
    for (const [id, vector] of generated) {
      const old = checkedIn.get(id);
      if (old && !isDeepStrictEqual(vector, old)) {
        problems.push(`producer vector "${id}" is stale: ${describeDifference(vector, old)}`);
      }
    }
    if (problems.length === 0 && readFileSync(generatedFile, "utf8") !== readFileSync(new URL("cases/producer.json", CONFORMANCE), "utf8")) {
      problems.push("cases/producer.json is not byte-identical to the generated file");
    }
    return problems;
  } finally {
    rmSync(dirname(generatedFile), { recursive: true, force: true });
  }
}

const producerProblems = producerVectorProblems();
if (producerProblems.length > 0) {
  const prefix = process.env.GITHUB_ACTIONS === "true" ? "::error::" : "";
  for (const problem of producerProblems) console.log(`${prefix}${problem}`);
  console.log(
    `${prefix}Ankusa.Sink.Message.encode/3 no longer matches conformance/cases/producer.json: ` +
      "run `mix ankusa.conformance.gen` in packages/ankusa, commit the result, and make every SDK decode it.",
  );
  process.exit(1);
}
console.log("producer vectors  match Sink.Message.encode/3");

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
