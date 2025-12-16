/**
 * rocksdb-js platform specific bindings publish script.
 *
 * This script will publish all bindings in the `artifacts` directory as
 * separate target-specific packages to npm.
 *
 * Required environment variables:
 * - NODE_AUTH_TOKEN: The npm token to use for authentication.
 * - TAG: The tag to use for the packages: `latest` or `next`.
 *
 * @example
 * NODE_AUTH_TOKEN=... TAG=latest node scripts/publish-bindings.mjs
 */

// 1. Read package.json to get name, version, and homepage
// 2. Find and check all artifacts
// 3. For each artifact:
//   a. Initialize empty temp directory with boilerplate (README, LICENSE,
//      package.json, etc.)
//   b. Update package.json with name and version
//   b. Copy artifact to temp directory
//   c. Publish package to npm

// Package names:
// @harperdb/rocksdb-js-macos-arm64
// @harperdb/rocksdb-js-macos-x64
// @harperdb/rocksdb-js-linux-arm64
// @harperdb/rocksdb-js-linux-x64
// @harperdb/rocksdb-js-windows-arm64
// @harperdb/rocksdb-js-windows-x64
