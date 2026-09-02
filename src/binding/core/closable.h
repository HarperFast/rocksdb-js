#ifndef __CORE_CLOSABLE_H__
#define __CORE_CLOSABLE_H__

namespace rocksdb_js {

struct Closable {
	virtual ~Closable() = default;
	virtual void close() = 0;

	/**
	 * Publishes cancellation for work that `close()` will later wait on but
	 * that cannot poll a flag from where it runs -- today only a manual
	 * RocksDB compaction, which abandons its range solely through the
	 * `CompactRangeOptions::canceled` pointer it was handed. Teardown calls
	 * this on every attached closable before its first step that can block on
	 * such work, which is earlier than the closables sweep that closes them.
	 * Default no-op: most closables have nothing that outlives a flag check.
	 */
	virtual void cancelBlockingWork() {}
};

} // namespace rocksdb_js

#endif
