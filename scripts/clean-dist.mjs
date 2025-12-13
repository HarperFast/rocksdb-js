#!/usr/bin/env node

import { rmSync } from 'fs';
import { join } from 'path';
import { fileURLToPath } from 'url';

const __dirname = fileURLToPath(new URL('.', import.meta.url));
const distPath = join(__dirname, '..', 'dist');

try {
  rmSync(distPath, { recursive: true, force: true });
} catch (error) {
  // Ignore if directory doesn't exist
  if (error.code !== 'ENOENT') {
    throw error;
  }
}
