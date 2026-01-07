#!/bin/bash
set -e

ITERATIONS=${1:-50}

echo "================================"
echo "Bundle Load Performance Comparison"
echo "Iterations: $ITERATIONS"
echo "================================"
echo ""

# Build minified
echo "Building minified bundle..."
pnpm build:bundle > /dev/null 2>&1
MINIFIED_SIZE=$(stat -f%z dist/index.mjs 2>/dev/null || stat -c%s dist/index.mjs)
[ -f dist/index.mjs ] && cp dist/index.mjs dist/index.minified.mjs

echo "Minified size: $(echo "scale=2; $MINIFIED_SIZE / 1024" | bc) KB"
echo ""

# Benchmark minified
echo "Benchmarking minified bundle..."
node benchmark-bundle-fast.mjs $ITERATIONS > /tmp/minified-results.txt
cat /tmp/minified-results.txt
echo ""

# Build unminified
echo "Building unminified bundle..."
SKIP_MINIFY=1 pnpm build:bundle > /dev/null 2>&1
UNMINIFIED_SIZE=$(stat -f%z dist/index.mjs 2>/dev/null || stat -c%s dist/index.mjs)
cp dist/index.mjs dist/index.unminified.mjs

echo "Unminified size: $(echo "scale=2; $UNMINIFIED_SIZE / 1024" | bc) KB"
echo ""

# Benchmark unminified
echo "Benchmarking unminified bundle..."
node benchmark-bundle-fast.mjs $ITERATIONS > /tmp/unminified-results.txt
cat /tmp/unminified-results.txt
echo ""

# Extract median times for comparison
MINIFIED_MEDIAN=$(grep "Median:" /tmp/minified-results.txt | awk '{print $2}' | sed 's/ms//')
UNMINIFIED_MEDIAN=$(grep "Median:" /tmp/unminified-results.txt | awk '{print $2}' | sed 's/ms//')

echo "================================"
echo "COMPARISON"
echo "================================"
echo "Size difference: $(echo "scale=2; ($UNMINIFIED_SIZE - $MINIFIED_SIZE) / 1024" | bc) KB ($(echo "scale=1; $UNMINIFIED_SIZE * 100 / $MINIFIED_SIZE - 100" | bc)% larger)"
echo ""

if (( $(echo "$MINIFIED_MEDIAN < $UNMINIFIED_MEDIAN" | bc -l) )); then
  DIFF=$(echo "scale=3; $UNMINIFIED_MEDIAN - $MINIFIED_MEDIAN" | bc)
  PERCENT=$(echo "scale=1; ($UNMINIFIED_MEDIAN - $MINIFIED_MEDIAN) * 100 / $MINIFIED_MEDIAN" | bc)
  echo "✓ Minified is FASTER by ${DIFF} ms (${PERCENT}% improvement)"
else
  DIFF=$(echo "scale=3; $MINIFIED_MEDIAN - $UNMINIFIED_MEDIAN" | bc)
  PERCENT=$(echo "scale=1; ($MINIFIED_MEDIAN - $UNMINIFIED_MEDIAN) * 100 / $UNMINIFIED_MEDIAN" | bc)
  echo "✗ Unminified is faster by ${DIFF} ms (${PERCENT}% improvement)"
  echo ""
  echo "This suggests V8 parse/compile time dominates over network transfer."
fi

# Restore minified version
[ -f dist/index.minified.mjs ] && mv dist/index.minified.mjs dist/index.mjs

echo ""
echo "Results saved in /tmp/minified-results.txt and /tmp/unminified-results.txt"
[ -f dist/index.mjs ] && echo "Restored minified bundle to dist/index.mjs"
