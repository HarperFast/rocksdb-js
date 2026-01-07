#!/usr/bin/env node
import { spawn } from 'child_process';
import { promises as fs } from 'fs';

// Benchmark by spawning fresh Node processes to avoid cache effects
async function benchmarkLoad(iterations = 100) {
  const times = [];

  for (let i = 0; i < iterations; i++) {
    const start = performance.now();

    const result = await new Promise((resolve, reject) => {
      const child = spawn('node', ['--eval', `
        const start = performance.now();
        import('./dist/index.mjs').then(() => {
          const end = performance.now();
          console.log(end - start);
        });
      `], {
        stdio: ['ignore', 'pipe', 'pipe']
      });

      let stdout = '';
      child.stdout.on('data', (data) => {
        stdout += data.toString();
      });

      child.on('close', (code) => {
        if (code === 0) {
          resolve(parseFloat(stdout.trim()));
        } else {
          reject(new Error(`Process exited with code ${code}`));
        }
      });
    });

    times.push(result);

    // Progress indicator
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

  // Standard deviation
  const variance = times.reduce((sum, val) => sum + Math.pow(val - mean, 2), 0) / times.length;
  const stddev = Math.sqrt(variance);

  // P95
  const p95 = sorted[Math.floor(sorted.length * 0.95)];

  return { mean, median, min, max, stddev, p95 };
}

async function main() {
  console.log('Bundle Load Performance Benchmark\n');

  // Check bundle size
  try {
    const stats = await fs.stat('./dist/index.mjs');
    console.log(`Bundle size: ${(stats.size / 1024).toFixed(2)} KB`);
  } catch (e) {
    console.error('Error: dist/index.mjs not found. Run `pnpm build:bundle` first.');
    process.exit(1);
  }

  const iterations = parseInt(process.argv[2]) || 100;
  console.log(`Running ${iterations} iterations with fresh processes...\n`);

  const times = await benchmarkLoad(iterations);
  const stats = calculateStats(times);

  console.log('\nResults:');
  console.log(`  Mean:   ${stats.mean.toFixed(3)} ms`);
  console.log(`  Median: ${stats.median.toFixed(3)} ms`);
  console.log(`  Min:    ${stats.min.toFixed(3)} ms`);
  console.log(`  Max:    ${stats.max.toFixed(3)} ms`);
  console.log(`  StdDev: ${stats.stddev.toFixed(3)} ms`);
  console.log(`  P95:    ${stats.p95.toFixed(3)} ms`);

  // Throughput
  console.log(`\nLoad time per KB: ${(stats.median / (await fs.stat('./dist/index.mjs')).size * 1024).toFixed(3)} ms/KB`);
}

main().catch(console.error);
