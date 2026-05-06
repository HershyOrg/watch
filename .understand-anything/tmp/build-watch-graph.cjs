#!/usr/bin/env node
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const root = process.cwd();
const uaDir = path.join(root, ".understand-anything");
const intermediateDir = path.join(uaDir, "intermediate");
const tmpDir = path.join(uaDir, "tmp");
fs.mkdirSync(intermediateDir, { recursive: true });
fs.mkdirSync(tmpDir, { recursive: true });

function readGitHead() {
  try {
    const head = fs.readFileSync(path.join(root, ".git", "HEAD"), "utf8").trim();
    if (!head.startsWith("ref:")) return head;
    const refPath = head.slice(5).trim();
    return fs.readFileSync(path.join(root, ".git", refPath), "utf8").trim();
  } catch {
    return "unknown";
  }
}
const gitCommitHash = readGitHead();
const modulePath = fs.readFileSync(path.join(root, "go.mod"), "utf8").match(/^module\s+(.+)$/m)?.[1]?.trim() || "";

const ignorePath = path.join(uaDir, ".understandignore");
const ignorePatterns = fs.existsSync(ignorePath)
  ? fs.readFileSync(ignorePath, "utf8").split(/\r?\n/).map((line) => line.trim()).filter((line) => line && !line.startsWith("#"))
  : [];

const defaults = [
  ".git/", "vendor/", "node_modules/", "dist/", "build/", "out/", "coverage/",
  ".cache/", ".turbo/", "target/", "obj/", ".idea/", ".vscode/",
  "LICENSE", ".gitignore", ".editorconfig", "*.lock", "package-lock.json",
  "yarn.lock", "pnpm-lock.yaml", "*.png", "*.jpg", "*.jpeg", "*.gif", "*.svg",
  "*.ico", "*.woff", "*.woff2", "*.ttf", "*.eot", "*.mp3", "*.mp4", "*.pdf",
  "*.zip", "*.tar", "*.gz", "*.min.js", "*.min.css", "*.map", "*.generated.*",
];

function globToRegExp(pattern) {
  const dirOnly = pattern.endsWith("/");
  const raw = dirOnly ? pattern.slice(0, -1) : pattern;
  const esc = raw.replace(/[.+^${}()|[\]\\]/g, "\\$&").replace(/\*/g, ".*");
  if (dirOnly) return new RegExp(`(^|/)${esc}(/|$)`);
  if (raw.includes("/")) return new RegExp(`^${esc}$`);
  return new RegExp(`(^|/)${esc}$`);
}

const includeRules = [];
const excludeRules = [];
for (const p of [...defaults, ...ignorePatterns]) {
  const negated = p.startsWith("!");
  const re = globToRegExp(negated ? p.slice(1) : p);
  (negated ? includeRules : excludeRules).push(re);
}

function ignored(file) {
  let excluded = excludeRules.some((re) => re.test(file));
  if (excluded && includeRules.some((re) => re.test(file))) excluded = false;
  return excluded;
}

function category(file) {
  const base = path.basename(file);
  const ext = path.extname(file);
  if ([".md", ".rst", ".txt"].includes(ext) && base !== "LICENSE") return "docs";
  if (file.startsWith(".github/workflows/") || base === "Dockerfile" || base.startsWith("docker-compose") || [".tf", ".tfvars"].includes(ext) || base === "Makefile") return "infra";
  if ([".sql", ".graphql", ".gql", ".proto", ".prisma", ".csv"].includes(ext) || file.endsWith(".schema.json")) return "data";
  if ([".sh", ".bash", ".ps1", ".bat"].includes(ext)) return "script";
  if ([".html", ".htm", ".css", ".scss", ".sass", ".less"].includes(ext)) return "markup";
  if ([".yaml", ".yml", ".json", ".toml", ".xml", ".cfg", ".ini", ".env"].includes(ext) || ["go.mod", "go.sum", "package.json", "pyproject.toml", "Cargo.toml"].includes(base)) return "config";
  return "code";
}

function language(file) {
  const ext = path.extname(file);
  const base = path.basename(file);
  if (ext === ".go") return "go";
  if (ext === ".md") return "markdown";
  if (ext === ".txt") return "text";
  if (ext === ".json") return "json";
  if (base === "go.mod" || base === "go.sum") return "gomod";
  if ([".yaml", ".yml"].includes(ext)) return "yaml";
  return ext ? ext.slice(1) : "text";
}

function walk(dir = root, prefix = "") {
  const entries = fs.readdirSync(dir, { withFileTypes: true });
  const out = [];
  for (const entry of entries) {
    const rel = prefix ? `${prefix}/${entry.name}` : entry.name;
    if (entry.isDirectory()) {
      if ([".git", ".understand-anything"].includes(rel) || ignored(`${rel}/`)) continue;
      out.push(...walk(path.join(dir, entry.name), rel));
    } else if (entry.isFile()) {
      out.push(rel);
    }
  }
  return out;
}
const tracked = walk();
const allFiltered = tracked.filter((file) => !ignored(file));
const files = allFiltered.map((file) => {
  const text = fs.readFileSync(path.join(root, file), "utf8");
  return { path: file, sizeLines: text.length ? text.split(/\r?\n/).length : 0, fileCategory: category(file), language: language(file) };
});

const goFiles = files.filter((f) => f.path.endsWith(".go"));
const packageFiles = new Map();
for (const f of goFiles) {
  const dir = path.dirname(f.path) === "." ? "" : path.dirname(f.path);
  if (!packageFiles.has(dir)) packageFiles.set(dir, []);
  packageFiles.get(dir).push(f.path);
}

function packageRepresentative(importPath) {
  if (!modulePath || !importPath.startsWith(modulePath)) return null;
  const rel = importPath.slice(modulePath.length).replace(/^\//, "");
  const candidates = packageFiles.get(rel || "") || [];
  return candidates.find((p) => path.basename(p) === "watch.go") || candidates.find((p) => path.basename(p) === "manager.go") || candidates.find((p) => path.basename(p) === "wm.go") || candidates[0] || null;
}

function extractImports(text) {
  const out = [];
  const block = text.match(/import\s*\(([\s\S]*?)\)/m);
  if (block) {
    for (const match of block[1].matchAll(/"([^"]+)"/g)) out.push(match[1]);
  }
  for (const match of text.matchAll(/import\s+"([^"]+)"/g)) out.push(match[1]);
  return [...new Set(out)];
}

function exportedName(name) {
  return /^[A-Z]/.test(name);
}

function summarizeFile(file, funcs, types) {
  const base = path.basename(file.path);
  const dir = path.dirname(file.path);
  if (file.fileCategory === "config") {
    if (base === "go.mod") return "Defines the Go module path and language version for the watch library.";
    if (base === "go.sum") return "Pins module checksum data for reproducible Go dependency resolution.";
    return "Stores project configuration used by tooling or editor integrations.";
  }
  if (file.fileCategory === "docs") return "Documents project guidance, conventions, or expected behavior for maintainers.";
  if (dir === "manager") return "Implements manager-side event coordination, reducer execution, effect handling, and lifecycle state for watch flows.";
  if (dir === "wm") return "Implements the lower-level watch machine loop, typed/raw watchers, effects, recovery, and runtime state management.";
  if (dir === "shared") return "Provides shared utility types and concurrency-safe containers used by the watch runtime.";
  if (dir === "api") return "Defines public API-facing handlers and request/response types around watch behavior.";
  if (dir === "demo") return "Shows runnable examples and support code demonstrating watch usage patterns.";
  if (base === "watch.go") return "Exposes the central public watch API and constructors for user-facing watch flows.";
  if (base === "watcher.go") return "Defines watcher behavior that wires user callbacks, manager lifecycle, and event processing.";
  if (base === "types.go") return "Defines core public types that shape watch callbacks, inputs, outputs, and lifecycle contracts.";
  if (base === "memo.go") return "Provides memoization support used by watch logic to retain computed state.";
  return `Defines ${funcs.length || types.length ? "Go symbols" : "package code"} for the watch library.`;
}

function complexity(nonEmpty, funcs, types) {
  if (nonEmpty > 200 || funcs.length + types.length > 15) return "complex";
  if (nonEmpty > 50 || funcs.length + types.length > 4) return "moderate";
  return "simple";
}

const nodes = [];
const edges = [];
const importMap = {};
const functionNodes = new Map();
const typeNodes = new Map();

function addNode(node) {
  if (!nodes.find((n) => n.id === node.id)) nodes.push(node);
}
function addEdge(edge) {
  if (edge.source !== edge.target && !edges.find((e) => e.source === edge.source && e.target === edge.target && e.type === edge.type)) edges.push(edge);
}

for (const file of files) {
  const abs = path.join(root, file.path);
  const text = fs.readFileSync(abs, "utf8");
  const nonEmpty = text.split(/\r?\n/).filter((line) => line.trim()).length;
  const funcs = [...text.matchAll(/func\s+(?:\([^)]*\)\s*)?([A-Za-z_][A-Za-z0-9_]*)\s*\(/g)].map((m) => ({ name: m[1], index: m.index || 0 }));
  const types = [...text.matchAll(/type\s+([A-Za-z_][A-Za-z0-9_]*)\s+(struct|interface|func|\w+)/g)].map((m) => ({ name: m[1], kind: m[2], index: m.index || 0 }));
  const fileType = file.fileCategory === "docs" ? "document" : file.fileCategory === "config" ? "config" : "file";
  const fileId = `${fileType}:${file.path}`;
  addNode({
    id: fileId,
    type: fileType,
    name: file.path,
    filePath: file.path,
    summary: summarizeFile(file, funcs, types),
    tags: [
      file.fileCategory === "code" ? "go-code" : file.fileCategory,
      path.dirname(file.path) === "." ? "root-package" : path.dirname(file.path).replace(/\//g, "-"),
      funcs.some((f) => exportedName(f.name)) || types.some((t) => exportedName(t.name)) ? "public-api" : "internal-implementation",
    ],
    complexity: complexity(nonEmpty, funcs, types),
  });
  const imports = file.path.endsWith(".go") ? extractImports(text).map(packageRepresentative).filter(Boolean) : [];
  importMap[file.path] = [...new Set(imports)];
  for (const target of importMap[file.path]) addEdge({ source: fileId, target: `file:${target}`, type: "imports", weight: 0.7, direction: "forward" });

  if (!file.path.endsWith(".go")) continue;
  for (const fn of funcs.filter((f) => exportedName(f.name) || text.slice(f.index, f.index + 500).split(/\r?\n/).length >= 10)) {
    const id = `function:${file.path}:${fn.name}`;
    functionNodes.set(fn.name, id);
    addNode({ id, type: "function", name: fn.name, filePath: file.path, summary: `Implements ${fn.name} behavior in ${file.path}.`, tags: [exportedName(fn.name) ? "public-api" : "internal", "go-function"], complexity: "moderate" });
    addEdge({ source: fileId, target: id, type: "contains", weight: 1.0, direction: "forward" });
    if (exportedName(fn.name)) addEdge({ source: fileId, target: id, type: "exports", weight: 0.8, direction: "forward" });
  }
  for (const typ of types.filter((t) => exportedName(t.name) || t.kind === "interface" || t.kind === "struct")) {
    const id = `class:${file.path}:${typ.name}`;
    typeNodes.set(typ.name, id);
    addNode({ id, type: "class", name: typ.name, filePath: file.path, summary: `Defines the ${typ.name} ${typ.kind} used by ${file.path}.`, tags: [typ.kind, exportedName(typ.name) ? "public-api" : "internal", "go-type"], complexity: "moderate" });
    addEdge({ source: fileId, target: id, type: "contains", weight: 1.0, direction: "forward" });
    if (exportedName(typ.name)) addEdge({ source: fileId, target: id, type: "exports", weight: 0.8, direction: "forward" });
  }
}

for (const f of files.filter((x) => x.fileCategory === "docs")) {
  for (const target of ["watch.go", "types.go", "watcher.go"].filter((p) => files.some((x) => x.path === p))) {
    addEdge({ source: `document:${f.path}`, target: `file:${target}`, type: "documents", weight: 0.5, direction: "forward" });
  }
}
if (files.some((f) => f.path === "go.mod")) {
  for (const target of goFiles.map((f) => f.path)) addEdge({ source: "config:go.mod", target: `file:${target}`, type: "configures", weight: 0.6, direction: "forward" });
}

const layerDefs = [
  ["layer:public-api", "Public API", "Root package files that expose the watch library surface and user-facing contracts.", (p) => !p.includes("/") && p.endsWith(".go")],
  ["layer:manager-runtime", "Manager Runtime", "Manager package files coordinating events, reducers, effects, lifecycle, logging, and cleanup.", (p) => p.startsWith("manager/")],
  ["layer:watch-machine", "Watch Machine", "The wm package implementing loop state, typed/raw watch execution, effects, recovery, and queue handling.", (p) => p.startsWith("wm/")],
  ["layer:shared-support", "Shared Support", "Reusable shared utilities, safe containers, error helpers, and API adapter files.", (p) => p.startsWith("shared/") || p.startsWith("api/")],
  ["layer:examples-and-docs", "Examples and Docs", "Demonstration programs and maintainer documentation that explain or exercise the library.", (p) => p.startsWith("demo/") || p.endsWith(".md") || p.endsWith(".txt")],
  ["layer:project-config", "Project Config", "Go module metadata and lightweight project/tooling configuration.", (p) => ["go.mod", "go.sum"].includes(p) || p.startsWith(".github/")],
];
const nodeIds = new Set(nodes.map((n) => n.id));
const layers = layerDefs.map(([id, name, description, pred]) => ({
  id, name, description,
  nodeIds: files.filter((f) => pred(f.path)).map((f) => `${f.fileCategory === "docs" ? "document" : f.fileCategory === "config" ? "config" : "file"}:${f.path}`).filter((id) => nodeIds.has(id)),
})).filter((l) => l.nodeIds.length);

const tour = [
  { order: 1, title: "Project Configuration", description: "Start with the Go module and repository guidance to understand the module identity and contribution rules.", nodeIds: ["config:go.mod", "document:CLAUDE.md"].filter((id) => nodeIds.has(id)) },
  { order: 2, title: "Public Watch API", description: "Inspect the root package files that define the user-facing watch API, callback contracts, and watcher lifecycle.", nodeIds: ["file:watch.go", "file:types.go", "file:watcher.go", "file:watcher_api.go"].filter((id) => nodeIds.has(id)) },
  { order: 3, title: "Manager Runtime", description: "Follow how events, reducers, effects, manager state, and cleanup are coordinated inside the manager package.", nodeIds: files.filter((f) => f.path.startsWith("manager/")).slice(0, 8).map((f) => `file:${f.path}`) },
  { order: 4, title: "Watch Machine Loop", description: "Review the wm package to see the lower-level loop mechanics, typed/raw watchers, recovery, and effect-driven event flow.", nodeIds: files.filter((f) => f.path.startsWith("wm/")).slice(0, 8).map((f) => `file:${f.path}`) },
  { order: 5, title: "Examples", description: "Use the demo programs as concrete entry points for how consumers compose the watch library.", nodeIds: files.filter((f) => f.path.startsWith("demo/") && f.path.endsWith(".go")).slice(0, 5).map((f) => `file:${f.path}`) },
].filter((step) => step.nodeIds.length);

const project = {
  name: "watch",
  languages: [...new Set(files.map((f) => f.language))].sort(),
  frameworks: ["Go"],
  description: "Go watch library that coordinates user-facing watchers, manager-driven event/reducer/effect lifecycles, and lower-level watch machine loops.",
  analyzedAt: new Date().toISOString(),
  gitCommitHash,
};
const graph = { version: "1.0.0", project, nodes, edges, layers, tour };

const scan = {
  scriptCompleted: true,
  name: project.name,
  description: project.description,
  languages: project.languages,
  frameworks: project.frameworks,
  complexity: files.length <= 30 ? "small" : "moderate",
  filteredByIgnore: tracked.length - files.length,
  files,
  importMap,
};

const review = {
  issues: [],
  warnings: nodes.filter((n) => !edges.some((e) => e.source === n.id || e.target === n.id)).map((n) => `Node '${n.id}' has no edges (orphan)`),
  stats: {
    totalNodes: nodes.length,
    totalEdges: edges.length,
    totalLayers: layers.length,
    tourSteps: tour.length,
    nodeTypes: nodes.reduce((a, n) => (a[n.type] = (a[n.type] || 0) + 1, a), {}),
    edgeTypes: edges.reduce((a, e) => (a[e.type] = (a[e.type] || 0) + 1, a), {}),
  },
};

fs.writeFileSync(path.join(intermediateDir, "scan-result.json"), JSON.stringify(scan, null, 2));
fs.writeFileSync(path.join(intermediateDir, "assembled-graph.json"), JSON.stringify(graph, null, 2));
fs.writeFileSync(path.join(intermediateDir, "review.json"), JSON.stringify(review, null, 2));
fs.writeFileSync(path.join(uaDir, "knowledge-graph.json"), JSON.stringify(graph, null, 2));
fs.writeFileSync(path.join(uaDir, "meta.json"), JSON.stringify({ lastAnalyzedAt: project.analyzedAt, gitCommitHash, version: "1.0.0", analyzedFiles: files.length }, null, 2));
const fingerprints = {};
for (const f of files) {
  const data = fs.readFileSync(path.join(root, f.path));
  fingerprints[f.path] = { hash: crypto.createHash("sha256").update(data).digest("hex"), sizeLines: f.sizeLines };
}
fs.writeFileSync(path.join(uaDir, "fingerprints.json"), JSON.stringify({ version: "1.0.0", generatedAt: project.analyzedAt, files: fingerprints }, null, 2));

console.log(JSON.stringify({ files: files.length, filtered: tracked.length - files.length, nodes: nodes.length, edges: edges.length, layers: layers.length, tourSteps: tour.length }, null, 2));
