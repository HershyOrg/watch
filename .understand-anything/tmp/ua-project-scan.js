#!/usr/bin/env node
'use strict';

const fs = require('fs');
const path = require('path');
const { execFileSync, execSync } = require('child_process');

const projectRoot = path.resolve(process.argv[2]);
const outputPath = path.resolve(process.argv[3]);

if (!fs.existsSync(projectRoot) || !fs.statSync(projectRoot).isDirectory()) {
  console.error(`Project root not found or not a directory: ${projectRoot}`);
  process.exit(1);
}

// ---------------- Step 1: File discovery ----------------
// Strategy: filesystem walk with skip-dir exclusions, picking up everything
// regardless of .gitignore (so CLAUDE.md, AGENTS.md, etc. are included).
// Also recurses into nested git repos (kaleidoscope/) naturally.
function discoverFiles() {
  const result = [];
  const skipDirs = new Set([
    '.git', 'node_modules', 'vendor', '.venv', 'venv', '__pycache__',
    'dist', 'build', 'out', 'coverage', '.next', '.cache', '.turbo', 'target', 'obj',
    '.idea', '.vscode',
    '.understand-anything', // self-exclude scan tool output/script
    '.claude', // local agent state
  ]);
  function walk(dir) {
    let entries;
    try {
      entries = fs.readdirSync(dir, { withFileTypes: true });
    } catch (e) {
      return;
    }
    for (const e of entries) {
      const full = path.join(dir, e.name);
      if (e.isDirectory()) {
        if (skipDirs.has(e.name)) continue;
        walk(full);
      } else if (e.isFile() || e.isSymbolicLink()) {
        const rel = path.relative(projectRoot, full).split(path.sep).join('/');
        result.push(rel);
      }
    }
  }
  walk(projectRoot);
  return result;
}

const allFiles = discoverFiles();

// Detect binary files by reading first chunk and looking for null bytes
function isBinaryFile(rel) {
  try {
    const fd = fs.openSync(path.join(projectRoot, rel), 'r');
    const buf = Buffer.alloc(8000);
    const n = fs.readSync(fd, buf, 0, 8000, 0);
    fs.closeSync(fd);
    if (n === 0) return false;
    for (let i = 0; i < n; i++) {
      if (buf[i] === 0) return true;
    }
    return false;
  } catch (e) {
    return false;
  }
}

// ---------------- Step 2: Default exclusions ----------------
const EXCLUDED_DIR_SEGMENTS = new Set([
  'node_modules', '.git', 'vendor', 'venv', '.venv', '__pycache__',
  'dist', 'build', 'out', 'coverage', '.next', '.cache', '.turbo', 'target', 'obj',
  '.idea', '.vscode',
]);

const BINARY_EXTS = new Set([
  '.png', '.jpg', '.jpeg', '.gif', '.svg', '.ico', '.woff', '.woff2', '.ttf', '.eot',
  '.mp3', '.mp4', '.pdf', '.zip', '.tar', '.gz',
]);

const LOCK_NAMES = new Set(['package-lock.json', 'yarn.lock', 'pnpm-lock.yaml']);
const MISC_NON_SOURCE = new Set(['LICENSE', '.gitignore', '.editorconfig', '.prettierrc']);

function defaultExcluded(rel) {
  const segs = rel.split('/');
  const base = segs[segs.length - 1];

  for (const seg of segs.slice(0, -1)) {
    if (EXCLUDED_DIR_SEGMENTS.has(seg)) return true;
  }

  // lock files
  if (base.endsWith('.lock')) return true;
  if (LOCK_NAMES.has(base)) return true;

  // binary
  const ext = path.extname(base).toLowerCase();
  if (BINARY_EXTS.has(ext)) return true;

  // generated
  if (base.endsWith('.min.js') || base.endsWith('.min.css') || base.endsWith('.map')) return true;
  if (/\.generated\./.test(base)) return true;

  // misc non-source
  if (MISC_NON_SOURCE.has(base)) return true;
  if (base.startsWith('.eslintrc')) return true;
  if (base.endsWith('.log')) return true;

  // Binary files (extensionless executables, etc.)
  if (!ext && isBinaryFile(rel)) return true;

  return false;
}

let baseFiles = allFiles.filter((f) => !defaultExcluded(f));

// ---------------- Step 2.5: .understandignore ----------------
// Minimal gitignore-like matcher with negation
function loadIgnoreFile(p) {
  if (!fs.existsSync(p)) return null;
  return fs.readFileSync(p, 'utf8').split('\n')
    .map((l) => l.replace(/\r$/, ''))
    .filter((l) => l.trim() && !l.trim().startsWith('#'));
}

function patternToRegex(pattern) {
  // Strip leading ! handled by caller
  let neg = false;
  let p = pattern;
  if (p.startsWith('!')) { neg = true; p = p.slice(1); }
  let dirOnly = false;
  if (p.endsWith('/')) { dirOnly = true; p = p.slice(0, -1); }
  let anchored = false;
  if (p.startsWith('/')) { anchored = true; p = p.slice(1); }
  // If no slash inside (after anchor stripping) pattern matches at any level
  const hasSlash = p.includes('/');
  // Escape regex special, then convert wildcards
  let re = '';
  for (let i = 0; i < p.length; i++) {
    const c = p[i];
    if (c === '*') {
      if (p[i + 1] === '*') {
        // **
        re += '.*';
        i++;
        if (p[i + 1] === '/') i++;
      } else {
        re += '[^/]*';
      }
    } else if (c === '?') {
      re += '[^/]';
    } else if ('.+^$()[]{}|\\'.includes(c)) {
      re += '\\' + c;
    } else {
      re += c;
    }
  }
  let prefix;
  if (anchored || hasSlash) {
    prefix = '^';
  } else {
    prefix = '^(?:.*/)?';
  }
  let suffix = dirOnly ? '(?:/.*)?$' : '(?:/.*)?$';
  return { regex: new RegExp(prefix + re + suffix), neg, dirOnly };
}

function buildMatcher(patterns) {
  const compiled = patterns.map(patternToRegex);
  return function match(rel) {
    let ignored = false;
    for (const p of compiled) {
      if (p.regex.test(rel)) {
        ignored = !p.neg;
      }
    }
    return ignored;
  };
}

const ignoreFiles = [];
const uaIgnore1 = path.join(projectRoot, '.understand-anything', '.understandignore');
const uaIgnore2 = path.join(projectRoot, '.understandignore');
const userPatterns = [];
for (const p of [uaIgnore1, uaIgnore2]) {
  const lines = loadIgnoreFile(p);
  if (lines) { ignoreFiles.push(p); userPatterns.push(...lines); }
}

let filteredByIgnore = 0;
let finalFiles = baseFiles;
if (userPatterns.length > 0) {
  // Re-run filter starting from original list with combined hardcoded + user patterns,
  // but since patterns can include negations, we apply user patterns to baseFiles
  // and also allow `!` to recover defaults — since defaults aren't pattern-based we
  // approximate by applying user patterns to allFiles after excluding only "always"
  // unsafe (binary/lock) defaults, then re-intersecting.
  const matcher = buildMatcher(userPatterns);

  // Build a list using original allFiles, applying defaults but allowing negation to override
  // Approach: for each file in allFiles, check matcher first (user patterns), then defaults.
  // If matcher says ignored => exclude. If matcher says not ignored, fall back to defaults.
  // If a user negation `!pat` matches, we still drop hardcoded-default files unless matcher
  // explicitly returns false-for-ignore via negation. We treat matcher result as authoritative
  // when any user pattern matched the path.
  const userPatternRegexes = userPatterns.map(patternToRegex);
  function userMatched(rel) {
    for (const p of userPatternRegexes) if (p.regex.test(rel)) return true;
    return false;
  }
  finalFiles = allFiles.filter((f) => {
    const um = userMatched(f);
    const userIgnored = matcher(f);
    if (um) {
      // user has explicit say
      if (userIgnored) return false;
      // user negated (force include) — bypass defaults too
      return true;
    }
    // No user pattern match — apply defaults
    return !defaultExcluded(f);
  });
  filteredByIgnore = baseFiles.length - finalFiles.filter((f) => baseFiles.includes(f)).length;
  // recompute filteredByIgnore as count beyond defaults removed
  const baseSet = new Set(baseFiles);
  const finalSet = new Set(finalFiles);
  let removedBeyond = 0;
  for (const f of baseFiles) if (!finalSet.has(f)) removedBeyond++;
  filteredByIgnore = removedBeyond;
}

// ---------------- Step 3 & 4: Languages and categories ----------------
const EXT_LANG = {
  '.ts': 'typescript', '.tsx': 'typescript',
  '.js': 'javascript', '.jsx': 'javascript', '.mjs': 'javascript', '.cjs': 'javascript',
  '.py': 'python',
  '.go': 'go',
  '.rs': 'rust',
  '.java': 'java',
  '.rb': 'ruby',
  '.cpp': 'cpp', '.cc': 'cpp', '.cxx': 'cpp', '.h': 'cpp', '.hpp': 'cpp',
  '.c': 'c',
  '.cs': 'csharp',
  '.swift': 'swift',
  '.kt': 'kotlin',
  '.php': 'php',
  '.vue': 'vue',
  '.svelte': 'svelte',
  '.sh': 'shell', '.bash': 'shell',
  '.md': 'markdown', '.rst': 'markdown',
  '.yaml': 'yaml', '.yml': 'yaml',
  '.json': 'json',
  '.toml': 'toml',
  '.sql': 'sql',
  '.graphql': 'graphql', '.gql': 'graphql',
  '.proto': 'protobuf',
  '.tf': 'terraform', '.tfvars': 'terraform',
  '.html': 'html', '.htm': 'html',
  '.css': 'css', '.scss': 'css', '.sass': 'css', '.less': 'css',
  '.xml': 'xml',
  '.cfg': 'config', '.ini': 'config', '.env': 'config',
};

const BASENAME_LANG = {
  'Dockerfile': 'dockerfile',
  'Makefile': 'makefile',
  'Jenkinsfile': 'jenkinsfile',
};

function detectLanguage(rel) {
  const base = path.basename(rel);
  if (BASENAME_LANG[base]) return BASENAME_LANG[base];
  if (base.startsWith('Dockerfile')) return 'dockerfile';
  const ext = path.extname(base).toLowerCase();
  if (EXT_LANG[ext]) return EXT_LANG[ext];
  // Common config no-ext / dot-files
  if (base === '.env' || base.startsWith('.env.')) return 'config';
  return 'unknown';
}

const CONFIG_BASENAMES = new Set([
  'tsconfig.json', 'package.json', 'pyproject.toml', 'Cargo.toml', 'go.mod', 'go.sum',
  'requirements.txt', 'setup.py', 'setup.cfg', 'Pipfile', 'Gemfile', 'pom.xml',
  'build.gradle', 'build.gradle.kts',
]);

function detectCategory(rel) {
  const base = path.basename(rel);
  const ext = path.extname(base).toLowerCase();
  const lower = rel.toLowerCase();

  // Infra (highest priority among non-binary)
  if (base === 'Dockerfile' || base.startsWith('Dockerfile')) return 'infra';
  if (base.startsWith('docker-compose')) return 'infra';
  if (ext === '.tf' || ext === '.tfvars') return 'infra';
  if (base === 'Makefile' || base === 'Jenkinsfile' || base === 'Procfile' || base === 'Vagrantfile') return 'infra';
  if (lower.startsWith('.github/workflows/')) return 'infra';
  if (base === '.gitlab-ci.yml') return 'infra';
  if (lower.startsWith('.circleci/')) return 'infra';
  if (base.endsWith('.k8s.yaml') || base.endsWith('.k8s.yml')) return 'infra';
  if (rel.split('/').includes('k8s') || rel.split('/').includes('kubernetes')) return 'infra';

  // Docs
  if (ext === '.md' || ext === '.rst') return 'docs';
  if (ext === '.txt' && base !== 'LICENSE') return 'docs';

  // Config
  if (['.yaml', '.yml', '.json', '.toml', '.xml', '.cfg', '.ini', '.env'].includes(ext)) return 'config';
  if (CONFIG_BASENAMES.has(base)) return 'config';
  if (base.startsWith('.env')) return 'config';

  // Data
  if (['.sql', '.graphql', '.gql', '.proto', '.prisma', '.csv'].includes(ext)) return 'data';
  if (base.endsWith('.schema.json')) return 'data';

  // Script
  if (['.sh', '.bash', '.ps1', '.bat'].includes(ext)) return 'script';

  // Markup
  if (['.html', '.htm', '.css', '.scss', '.sass', '.less'].includes(ext)) return 'markup';

  return 'code';
}

// ---------------- Step 5: Line counting (batched) ----------------
function countLinesBatch(files) {
  const map = new Map();
  if (files.length === 0) return map;
  const BATCH = 200;
  for (let i = 0; i < files.length; i += BATCH) {
    const chunk = files.slice(i, i + BATCH);
    try {
      const args = ['-l', '--', ...chunk];
      const out = execFileSync('wc', args, { cwd: projectRoot, encoding: 'utf8', maxBuffer: 64 * 1024 * 1024 });
      const lines = out.split('\n').filter(Boolean);
      // wc emits "  N path" per file; if multiple files, last line is "total"
      for (const ln of lines) {
        const m = ln.match(/^\s*(\d+)\s+(.+)$/);
        if (!m) continue;
        const n = parseInt(m[1], 10);
        const p = m[2];
        if (p === 'total') continue;
        map.set(p, n);
      }
    } catch (e) {
      // fallback: per-file
      for (const f of chunk) {
        try {
          const content = fs.readFileSync(path.join(projectRoot, f), 'utf8');
          map.set(f, content.split('\n').length);
        } catch (_) {
          map.set(f, 0);
        }
      }
    }
  }
  return map;
}

const lineCounts = countLinesBatch(finalFiles);

// ---------------- Step 6: Frameworks ----------------
const frameworks = new Set();

function safeRead(rel) {
  try { return fs.readFileSync(path.join(projectRoot, rel), 'utf8'); } catch (e) { return null; }
}

function safeJson(rel) {
  const c = safeRead(rel);
  if (!c) return null;
  try { return JSON.parse(c); } catch (e) { return null; }
}

const fileSet = new Set(finalFiles);

// package.json (top-level + nested)
const packageJsons = finalFiles.filter((f) => path.basename(f) === 'package.json');
let projectName = null;
let rawDescription = '';
const JS_FW_MAP = {
  'react': 'React', 'vue': 'Vue', 'svelte': 'Svelte', '@angular/core': 'Angular',
  'express': 'Express', 'fastify': 'Fastify', 'koa': 'Koa',
  'next': 'Next.js', 'nuxt': 'Nuxt',
  'vite': 'Vite', 'vitest': 'Vitest', 'jest': 'Jest', 'mocha': 'Mocha',
  'tailwindcss': 'Tailwind CSS', 'prisma': 'Prisma',
  'typeorm': 'TypeORM', 'sequelize': 'Sequelize', 'mongoose': 'Mongoose',
  'redux': 'Redux', 'zustand': 'Zustand', 'mobx': 'MobX',
};
for (const pj of packageJsons) {
  const data = safeJson(pj);
  if (!data) continue;
  if (!projectName && pj === 'package.json') {
    projectName = data.name || null;
    rawDescription = data.description || '';
  }
  const deps = Object.assign({}, data.dependencies || {}, data.devDependencies || {});
  for (const k of Object.keys(deps)) {
    if (JS_FW_MAP[k]) frameworks.add(JS_FW_MAP[k]);
  }
}

// tsconfig
if (finalFiles.some((f) => path.basename(f) === 'tsconfig.json')) frameworks.add('TypeScript');

// Cargo.toml
const cargos = finalFiles.filter((f) => path.basename(f) === 'Cargo.toml');
const RUST_FW = ['actix-web', 'axum', 'rocket', 'diesel', 'tokio', 'serde', 'warp'];
for (const c of cargos) {
  const txt = safeRead(c) || '';
  if (!projectName && c === 'Cargo.toml') {
    const m = txt.match(/\[package\][^\[]*?name\s*=\s*"([^"]+)"/s);
    if (m) projectName = m[1];
  }
  for (const fw of RUST_FW) {
    const re = new RegExp('^\\s*' + fw.replace(/[-/]/g, (s) => '\\' + s) + '\\s*=', 'm');
    if (re.test(txt)) frameworks.add(fw);
  }
}

// go.mod (multiple)
const gomods = finalFiles.filter((f) => path.basename(f) === 'go.mod');
const GO_FW_MAP = {
  'github.com/gin-gonic/gin': 'Gin',
  'github.com/labstack/echo': 'Echo',
  'github.com/gofiber/fiber': 'Fiber',
  'github.com/go-chi/chi': 'Chi',
  'gorm.io/gorm': 'GORM',
};
for (const gm of gomods) {
  const txt = safeRead(gm) || '';
  if (!projectName && gm === 'go.mod') {
    const m = txt.match(/^module\s+(\S+)/m);
    if (m) {
      const segs = m[1].split('/');
      projectName = segs[segs.length - 1];
    }
  }
  for (const k of Object.keys(GO_FW_MAP)) {
    if (txt.includes(k)) frameworks.add(GO_FW_MAP[k]);
  }
}

// Python (requirements.txt, pyproject.toml)
const PY_FW_MAP = {
  'django': 'Django', 'djangorestframework': 'Django REST Framework',
  'fastapi': 'FastAPI', 'flask': 'Flask',
  'sqlalchemy': 'SQLAlchemy', 'alembic': 'Alembic',
  'celery': 'Celery', 'pydantic': 'Pydantic',
  'uvicorn': 'Uvicorn', 'gunicorn': 'Gunicorn',
  'aiohttp': 'aiohttp', 'tornado': 'Tornado', 'starlette': 'Starlette',
  'pytest': 'pytest', 'hypothesis': 'Hypothesis', 'channels': 'Django Channels',
};
for (const f of finalFiles.filter((f) => path.basename(f) === 'requirements.txt')) {
  const txt = safeRead(f) || '';
  for (const ln of txt.split('\n')) {
    const name = ln.split(/[<>=!~;\s]/)[0].trim().toLowerCase();
    if (PY_FW_MAP[name]) frameworks.add(PY_FW_MAP[name]);
  }
}
for (const f of finalFiles.filter((f) => path.basename(f) === 'pyproject.toml')) {
  const txt = safeRead(f) || '';
  if (!projectName && f === 'pyproject.toml') {
    const m1 = txt.match(/\[project\][^\[]*?name\s*=\s*"([^"]+)"/s);
    const m2 = txt.match(/\[tool\.poetry\][^\[]*?name\s*=\s*"([^"]+)"/s);
    if (m1) projectName = m1[1]; else if (m2) projectName = m2[1];
  }
  for (const k of Object.keys(PY_FW_MAP)) {
    const re = new RegExp('["\\\'\\s]' + k + '["\\\'\\s<>=!]', 'i');
    if (re.test(txt)) frameworks.add(PY_FW_MAP[k]);
  }
  if (/\[tool\.pytest\.ini_options\]/.test(txt)) frameworks.add('pytest');
  if (/\[tool\.django\]/.test(txt)) frameworks.add('Django');
}

// Gemfile
for (const f of finalFiles.filter((f) => path.basename(f) === 'Gemfile')) {
  const txt = safeRead(f) || '';
  const RB = { 'rails': 'Rails', 'railties': 'Rails', 'sinatra': 'Sinatra', 'grape': 'Grape',
    'rspec': 'RSpec', 'sidekiq': 'Sidekiq', 'devise': 'Devise', 'pundit': 'Pundit' };
  for (const k of Object.keys(RB)) {
    if (new RegExp("['\"]" + k + "['\"]").test(txt)) frameworks.add(RB[k]);
  }
}

// JVM
for (const f of finalFiles.filter((f) => ['pom.xml', 'build.gradle', 'build.gradle.kts'].includes(path.basename(f)))) {
  const txt = safeRead(f) || '';
  const JVM = ['spring-boot', 'spring-web', 'spring-data', 'quarkus', 'micronaut', 'hibernate', 'jakarta', 'junit', 'ktor'];
  for (const k of JVM) if (txt.includes(k)) frameworks.add(k);
}

// Infra detection
if (finalFiles.some((f) => path.basename(f).startsWith('Dockerfile'))) frameworks.add('Docker');
if (finalFiles.some((f) => /^docker-compose(\..+)?\.ya?ml$/.test(path.basename(f)))) frameworks.add('Docker Compose');
if (finalFiles.some((f) => f.endsWith('.tf'))) frameworks.add('Terraform');
if (finalFiles.some((f) => f.startsWith('.github/workflows/') && (f.endsWith('.yml') || f.endsWith('.yaml')))) frameworks.add('GitHub Actions');
if (finalFiles.some((f) => f === '.gitlab-ci.yml')) frameworks.add('GitLab CI');
if (finalFiles.some((f) => path.basename(f) === 'Jenkinsfile')) frameworks.add('Jenkins');

// ---------------- Step 8: Project name fallback ----------------
if (!projectName) projectName = path.basename(projectRoot);

// ---------------- README head ----------------
let readmeHead = '';
const readmeCandidates = ['README.md', 'README.MD', 'readme.md', 'README.rst', 'README', 'CLAUDE.md', 'AGENTS.md'];
for (const r of readmeCandidates) {
  if (fileSet.has(r)) {
    const txt = safeRead(r) || '';
    readmeHead = txt.split('\n').slice(0, 10).join('\n');
    break;
  }
}

// ---------------- Step 9: Import resolution ----------------
function resolveImport(fromFile, importPath) {
  // Only relative paths handled here; caller filters
  const fromDir = path.dirname(fromFile);
  let resolved = path.posix.normalize(path.posix.join(fromDir, importPath));
  if (resolved.startsWith('./')) resolved = resolved.slice(2);
  // Try direct
  if (fileSet.has(resolved)) return resolved;
  const candidates = [
    resolved + '.ts', resolved + '.tsx', resolved + '.js', resolved + '.jsx',
    resolved + '/index.ts', resolved + '/index.js', resolved + '/index.tsx', resolved + '/index.jsx',
    resolved + '.py', resolved + '.go', resolved + '.rs', resolved + '.rb',
  ];
  for (const c of candidates) if (fileSet.has(c)) return c;
  return null;
}

// Discover Go module roots (each go.mod) for resolving non-relative go imports
const goModules = []; // [{ dir, modulePath }]
for (const gm of gomods) {
  const txt = safeRead(gm) || '';
  const m = txt.match(/^module\s+(\S+)/m);
  if (m) {
    const dir = path.dirname(gm) === '.' ? '' : path.dirname(gm);
    goModules.push({ dir, modulePath: m[1] });
  }
}
// Sort: longest dir first (more specific module wins)
goModules.sort((a, b) => b.dir.length - a.dir.length);

function resolveGoImport(importPath) {
  for (const gm of goModules) {
    if (importPath === gm.modulePath || importPath.startsWith(gm.modulePath + '/')) {
      const sub = importPath === gm.modulePath ? '' : importPath.slice(gm.modulePath.length + 1);
      const baseDir = gm.dir ? gm.dir + '/' + sub : sub;
      // Find any .go file inside that directory (best-effort: pick first)
      const dirPrefix = baseDir.endsWith('/') ? baseDir : baseDir + '/';
      const matched = [];
      for (const f of finalFiles) {
        if (!f.endsWith('.go')) continue;
        if (f.startsWith(dirPrefix) && f.indexOf('/', dirPrefix.length) === -1) {
          matched.push(f);
        } else if (baseDir === '' && f.indexOf('/') === -1) {
          matched.push(f);
        }
      }
      // Filter out _test.go
      const nonTest = matched.filter((f) => !f.endsWith('_test.go'));
      const pick = nonTest.length > 0 ? nonTest : matched;
      return pick;
    }
  }
  return [];
}

const importMap = {};

function extractTsJsImports(content) {
  const out = [];
  const re1 = /(?:^|\n)\s*import\s+[^'"\n;]*?from\s+['"]([^'"]+)['"]/g;
  const re2 = /(?:^|\n)\s*import\s+['"]([^'"]+)['"]/g;
  const re3 = /require\(\s*['"]([^'"]+)['"]\s*\)/g;
  let m;
  while ((m = re1.exec(content))) out.push(m[1]);
  while ((m = re2.exec(content))) out.push(m[1]);
  while ((m = re3.exec(content))) out.push(m[1]);
  return out;
}

function extractPyImports(content) {
  const out = [];
  // from .x import y / from ..x import y / from . import y
  const re = /(?:^|\n)\s*from\s+(\.+)([\w\.]*)\s+import\s+/g;
  let m;
  while ((m = re.exec(content))) {
    const dots = m[1];
    const mod = m[2];
    out.push({ dots, mod });
  }
  return out;
}

function extractRustImports(content) {
  // very rough: capture `use crate::a::b;`, `use super::a;`, `mod x;`
  const out = [];
  let m;
  const re1 = /\buse\s+(crate|super|self)::([\w:]+)/g;
  while ((m = re1.exec(content))) out.push({ kind: 'use', root: m[1], path: m[2] });
  const re2 = /\bmod\s+(\w+)\s*;/g;
  while ((m = re2.exec(content))) out.push({ kind: 'mod', name: m[1] });
  return out;
}

function extractRubyImports(content) {
  const out = [];
  const re = /require_relative\s+['"]([^'"]+)['"]/g;
  let m;
  while ((m = re.exec(content))) out.push(m[1]);
  return out;
}

function extractGoImports(content) {
  const out = [];
  // single-line
  const re1 = /^\s*import\s+"([^"]+)"\s*$/gm;
  let m;
  while ((m = re1.exec(content))) out.push(m[1]);
  // grouped
  const re2 = /import\s*\(\s*([\s\S]*?)\)/g;
  while ((m = re2.exec(content))) {
    const block = m[1];
    const re3 = /"([^"]+)"/g;
    let mm;
    while ((mm = re3.exec(block))) out.push(mm[1]);
  }
  return out;
}

// Pre-compute: directory containing each file's go.mod
function findOwningGoModule(rel) {
  for (const gm of goModules) {
    if (gm.dir === '' || rel.startsWith(gm.dir + '/')) return gm;
  }
  return null;
}

const fileEntries = [];
for (const rel of finalFiles) {
  const language = detectLanguage(rel);
  const fileCategory = detectCategory(rel);
  const sizeLines = lineCounts.get(rel) || 0;
  fileEntries.push({ path: rel, language, sizeLines, fileCategory });
}

// Build importMap
for (const entry of fileEntries) {
  const rel = entry.path;
  if (entry.fileCategory !== 'code') {
    importMap[rel] = [];
    continue;
  }
  const lang = entry.language;
  const resolved = new Set();

  let content;
  try {
    content = fs.readFileSync(path.join(projectRoot, rel), 'utf8');
  } catch (e) {
    importMap[rel] = [];
    continue;
  }

  if (lang === 'typescript' || lang === 'javascript' || lang === 'vue' || lang === 'svelte') {
    for (const imp of extractTsJsImports(content)) {
      if (imp.startsWith('./') || imp.startsWith('../')) {
        const r = resolveImport(rel, imp);
        if (r) resolved.add(r);
      }
    }
  } else if (lang === 'python') {
    const fromDirParts = path.dirname(rel).split('/').filter((x) => x && x !== '.');
    for (const { dots, mod } of extractPyImports(content)) {
      // dots length: 1 = current pkg, 2 = parent, etc.
      const upLevels = dots.length - 1;
      const baseParts = fromDirParts.slice(0, fromDirParts.length - upLevels);
      const modParts = mod ? mod.split('.') : [];
      const targetBase = [...baseParts, ...modParts].join('/');
      const candidates = [
        targetBase + '.py',
        targetBase + '/__init__.py',
      ];
      for (const c of candidates) if (fileSet.has(c)) resolved.add(c);
    }
  } else if (lang === 'go') {
    const owning = findOwningGoModule(rel);
    for (const imp of extractGoImports(content)) {
      const matched = resolveGoImport(imp);
      for (const m of matched) {
        if (m !== rel) resolved.add(m);
      }
    }
  } else if (lang === 'rust') {
    // best-effort skip — leave empty
  } else if (lang === 'ruby') {
    for (const imp of extractRubyImports(content)) {
      const r = resolveImport(rel, imp);
      if (r) resolved.add(r);
    }
  }

  importMap[rel] = Array.from(resolved).sort();
}

// ---------------- Languages ----------------
const langSet = new Set();
for (const e of fileEntries) if (e.language && e.language !== 'unknown') langSet.add(e.language);
const languages = Array.from(langSet).sort();

// Sort files by path
fileEntries.sort((a, b) => a.path.localeCompare(b.path));

// ---------------- Complexity ----------------
const total = fileEntries.length;
let complexity;
if (total <= 30) complexity = 'small';
else if (total <= 150) complexity = 'moderate';
else if (total <= 500) complexity = 'large';
else complexity = 'very-large';

// ---------------- Output ----------------
const result = {
  scriptCompleted: true,
  name: projectName,
  rawDescription: rawDescription || '',
  readmeHead: readmeHead || '',
  languages,
  frameworks: Array.from(frameworks).sort(),
  files: fileEntries,
  totalFiles: fileEntries.length,
  filteredByIgnore,
  estimatedComplexity: complexity,
  importMap,
};

fs.mkdirSync(path.dirname(outputPath), { recursive: true });
fs.writeFileSync(outputPath, JSON.stringify(result, null, 2));
process.exit(0);
