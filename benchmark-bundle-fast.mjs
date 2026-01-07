#!/usr/bin/env node
import { promises as fs } from 'fs';

// In-process benchmark (faster but measures cached loads after first)
async function benchmarkInProcess(iterations = 1000) {
  const times = [];

  for (let i = 0; i < iterations; i++) {
    const start = performance.now();
    // Dynamic import with cache busting
    await import(`./dist/index.mjs?t=${Date.now()}_${i}`);
    const end = performance.now();
    times.push(end - start);
  }

  return times;
}

// First load benchmark (most important)
async function benchmarkFirstLoad(iterations = 100) {
  const { execSync } = await import('child_process');
  const times = [];

  for (let i = 0; i < iterations; i++) {
    try {
      const output = execSync(
        `node --eval "const start = performance.now(); import('./dist/index.mjs').then(() => console.log(performance.now() - start));"`,
        { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] }
      );
      times.push(parseFloat(output.trim()));
    } catch (e) {
      console.error('Error running benchmark:', e.message);
    }

    if ((i + 1) % 10 === 0) {
      process.stdout.write(`\rProgress: ${i + 1}/${iterations}`);
    }
  }

  process.stdout.write('\n');
  return times;
}

// Calculate statistics
function calculateStats(times) {
  const sorted = times.slice().sort((a, b) => a - b);
  const mean = times.reduce((a, b) => a + b, 0) / times.length;
  const median = sorted[Math.floor(sorted.length / 2)];
  const min = sorted[0];
  const max = sorted[sorted.length - 1];

  const variance = times.reduce((sum, val) => sum + Math.pow(val - mean, 2), 0) / times.length;
  const stddev = Math.sqrt(variance);

  const p95 = sorted[Math.floor(sorted.length * 0.95)];
  const p99 = sorted[Math.floor(sorted.length * 0.99)];

  return { mean, median, min, max, stddev, p95, p99 };
}

async function main() {
  console.log('Bundle Load Performance Benchmark (Fast)\n');

  // Check bundle size
  try {
    const stats = await fs.stat('./dist/index.mjs');
    const sizeKB = (stats.size / 1024).toFixed(2);
    console.log(`Bundle size: ${sizeKB} KB\n`);
  } catch (e) {
    console.error('Error: dist/index.mjs not found. Run `pnpm build:bundle` first.');
    process.exit(1);
  }

  const iterations = parseInt(process.argv[2]) || 50;
  console.log(`Measuring first-load performance (${iterations} fresh processes)...\n`);

  const times = await benchmarkFirstLoad(iterations);
  const stats = calculateStats(times);

  console.log('\n=== First Load Results ===');
  console.log(`  Mean:   ${stats.mean.toFixed(3)} ms`);
  console.log(`  Median: ${stats.median.toFixed(3)} ms`);
  console.log(`  Min:    ${stats.min.toFixed(3)} ms`);
  console.log(`  Max:    ${stats.max.toFixed(3)} ms`);
  console.log(`  StdDev: ${stats.stddev.toFixed(3)} ms`);
  console.log(`  P95:    ${stats.p95.toFixed(3)} ms`);
  console.log(`  P99:    ${stats.p99.toFixed(3)} ms`);
}

main().catch(console.error);
