import nodeGypBuild from 'node-gyp-build';

const binding = nodeGypBuild();

export class RocksDB {
  constructor() {
    console.log('RocksDB constructor');
  }
}

export default RocksDB;
