---
layers: universal, repo-type/native-addon
---

## Repo-specific checks (@harperfast/rocksdb-js)

This is a Node.js native addon (TS over an N-API C++ binding) —
the repo-type/native-addon layer carries the core checks. On
top of it, repo specifics the shared layers don't cover:

- **Toolchain is pnpm + oxlint + oxfmt** (`pnpm check` runs
  type-check + lint + format check). Advice referencing
  npm/eslint/prettier doesn't apply here.
- **No build tolerance** — `pnpm check` type-checks cleanly;
  flag type errors as real findings.
- **RocksDB prebuild bumps** (the `update-rocksdb.yml` flow, which
  only changes `rocksdb.version` in `package.json`; RocksDB is a
  prebuilt library fetched at configure time into the git-ignored
  `deps/rocksdb`, not vendored in-tree) are mechanical unless the
  diff goes beyond that one line (for example `src/binding/`,
  `binding.gyp` or `scripts/`) — a pure `rocksdb.version` bump with
  green CI is the no-log mechanical-diff case.
- **Runtime deps are exactly three** (`msgpackr`,
  `ordered-binary`, `@harperfast/extended-iterable`); a new one
  needs explicit justification in the PR description.
- **Downstream consumer is Harper core**
  (`resources/PrimaryRocksDatabase.ts` et al.). A behavior change
  here that alters the MaybePromise contract or key encoding needs a
  matching harper-core issue/PR reference.
