import { bench, run } from 'mitata'
import { RocksLevel } from '../index.js'
import { LRUCache } from 'lru-cache'

const values = []
for (let x = 0; x < 1e3; x++) {
  values.push(Buffer.from(Math.random().toString(36).repeat(4)))
}

const db = new RocksLevel('./tmp', {
  keyEncoding: 'buffer',
  valueEncoding: 'buffer',
  parallelism: 4,
  pipelinedWrite: false,
  // unorderedWrite: true,
  columns: {
    default: {
      cacheSize: 128e6,
      memtableMemoryBudget: 128e6,
      compaction: 'level'
      // optimize: 'point-lookup',
    }
  }
})
await db.open()

const map = new Map()
const lru = new LRUCache({ max: 10e3 })

for (const value of values) {
  db.put(value, value)
  map.set(value.toString(), value.toString())
  lru.set(value.toString(), value)
}
console.log('setup done')
let x = 0

const getOpts = {
  keyEncoding: 'buffer',
  valueEncoding: 'buffer',
  fillCache: true
}

x = db._getMany(values, getOpts).length

bench('rocks async', async () => {
  x += (await db._getMany(values, getOpts)).length
})

bench('rocks sync', async () => {
  x += db._getManySync(values, getOpts).length
})

bench('map', () => {
  for (const value of values) {
    x += map.get(value.toString()).length
  }
})

bench('lru', () => {
  for (const value of values) {
    x += lru.get(value.toString()).length
  }
})

await run()

console.log(x)
