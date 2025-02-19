import nodeGypBuild from 'node-gyp-build/node-gyp-build.js';

const binding = nodeGypBuild();

export const Database = binding.Database;
export const version = binding.version;
