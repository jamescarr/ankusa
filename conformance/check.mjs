// Validate conformance/ and run every registered packages/sdk-* against its
// vectors. Node 22 ESM, stdlib only. `mise run check:conformance` calls this.
import { spawnSync } from "node:child_process";
import { readdirSync, readFileSync } from "node:fs";
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

let failed = false;
for (const name of Object.keys(sdksFile.sdks).sort()) {
  const { setup = [], run } = sdksFile.sdks[name];
  const cwd = fileURLToPath(new URL(`packages/${name}`, ROOT));
  let code = 0;
  for (const [command, ...args] of [...setup, run]) {
    const result = spawnSync(command, args, { cwd, stdio: "inherit" });
    if (result.status !== 0) {
      code = result.status ?? 1;
      break;
    }
  }
  if (code === 0) {
    console.log(`${name}  pass`);
  } else {
    failed = true;
    console.log(`${name}  FAIL (exit ${code})`);
  }
}
process.exit(failed ? 1 : 0);
