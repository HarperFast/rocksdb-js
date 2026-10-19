#ifndef __CORE_COMMIT_KEY_RESERVATION_H__
#define __CORE_COMMIT_KEY_RESERVATION_H__

#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <span>
#include <unordered_set>
#include <vector>

namespace rocksdb_js {

/**
 * Per-database exclusion of concurrent in-process writers to the same keys for
 * the duration of a commit (see `src/binding/transaction/DESIGN.md`).
 *
 * A logged optimistic commit validates its write set before writing the
 * transaction-log batch and only then runs `txn->Commit()`, which validates
 * again. The reservation is what makes the first verdict still hold at the
 * second: every writer of the database — transactional commits, logged or
 * not, and direct `Put`/`Delete` — holds its keys here while it writes, so
 * nothing can land between the two.
 *
 * Keys are 64-bit hashes of (column family, key); a hash collision only makes
 * a writer wait. Acquisition is all-or-nothing under one mutex, so there is no
 * lock order to respect and no deadlock: a holder is always inside a log write
 * or a RocksDB write, neither of which waits on a reservation. Memory is
 * proportional to the keys in flight, which is why this is a set rather than
 * a stripe array — at four commit threads with 64-key transactions, stripes
 * need about 2^16 entries (2.5 MiB per database) before collisions stop
 * serializing unrelated commits.
 */
class CommitKeyReservation final {
public:
	void acquire(std::span<const uint64_t> hashes) {
		std::unique_lock<std::mutex> lock(this->mutex);
		while (this->anyHeld(hashes)) {
			++this->waiters;
			this->released.wait(lock);
			--this->waiters;
		}
		for (uint64_t hash : hashes) {
			this->held.insert(hash);
		}
	}

	void release(std::span<const uint64_t> hashes) {
		{
			std::lock_guard<std::mutex> lock(this->mutex);
			for (uint64_t hash : hashes) {
				this->held.erase(hash);
			}
			if (this->waiters == 0) {
				return;
			}
		}
		this->released.notify_all();
	}

	size_t heldCount() {
		std::lock_guard<std::mutex> lock(this->mutex);
		return this->held.size();
	}

private:
	bool anyHeld(std::span<const uint64_t> hashes) const {
		for (uint64_t hash : hashes) {
			if (this->held.count(hash) != 0) {
				return true;
			}
		}
		return false;
	}

	std::mutex mutex;
	std::condition_variable released;
	std::unordered_set<uint64_t> held;
	uint32_t waiters = 0;
};

/**
 * Holds one writer's reservation; `release()` is idempotent so a commit can
 * release at the right point and still rely on the destructor on error paths.
 */
struct CommitKeyReservationScope final {
	CommitKeyReservation* reservation = nullptr;
	std::vector<uint64_t> hashes;

	CommitKeyReservationScope() = default;
	CommitKeyReservationScope(const CommitKeyReservationScope&) = delete;
	CommitKeyReservationScope& operator=(const CommitKeyReservationScope&) = delete;

	~CommitKeyReservationScope() {
		this->release();
	}

	void acquire(CommitKeyReservation& target, std::vector<uint64_t> keyHashes) {
		this->release();
		this->hashes = std::move(keyHashes);
		target.acquire(this->hashes);
		this->reservation = &target;
	}

	void release() noexcept {
		if (this->reservation == nullptr) {
			return;
		}
		CommitKeyReservation* target = this->reservation;
		this->reservation = nullptr;
		target->release(this->hashes);
		this->hashes.clear();
	}

	bool active() const {
		return this->reservation != nullptr;
	}
};

} // namespace rocksdb_js

#endif
