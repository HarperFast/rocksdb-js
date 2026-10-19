#ifndef __CORE_COMMIT_KEY_RESERVATION_H__
#define __CORE_COMMIT_KEY_RESERVATION_H__

#include <algorithm>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <span>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace rocksdb_js {

/**
 * Per-database exclusion of concurrent in-process writers to the same keys
 * (see `src/binding/transaction/DESIGN.md`).
 *
 * A logged optimistic commit validates its write set before writing the
 * transaction-log batch and only then runs `txn->Commit()`, which validates
 * again. A **transient** hold is what makes the first verdict still hold at
 * the second: every writer of the database — transactional commits, logged or
 * not, and direct `Put`/`Delete` — holds its keys while it writes, so nothing
 * can land between the two. A **durable** hold is what a transaction keeps on
 * its keys once its log batch is written but its commit still has to be
 * retried: log order would otherwise diverge from data order, since a later
 * writer could log and commit those keys before the retry lands. A durable
 * hold is not waited for; a colliding writer is refused with `Busy` and takes
 * its ordinary conflict retry, which keeps the owning thread free to drive the
 * retry that releases it.
 *
 * Keys are 64-bit hashes of (column family, key); a hash collision only makes
 * a writer wait or retry. Transient acquisition is all-or-nothing under one
 * mutex, so there is no lock order to respect and no deadlock: a transient
 * holder is always inside a log write or a RocksDB write, neither of which
 * waits on a reservation. Memory is proportional to the keys in flight, which
 * is why this is a set rather than a stripe array — at four commit threads
 * with 64-key transactions, stripes need about 2^16 entries (2.5 MiB per
 * database) before collisions stop serializing unrelated commits.
 */
class CommitKeyReservation final {
public:
	/**
	 * Takes a transient hold on every hash, waiting out other transient holders.
	 * Returns false, holding nothing, when another owner's durable hold covers
	 * one of them. Strong exception guarantee.
	 */
	[[nodiscard]] bool acquire(std::span<const uint64_t> hashes, const void* owner) {
		std::unique_lock<std::mutex> lock(this->mutex);
		while (this->anyHeld(hashes)) {
			++this->waiters;
			this->released.wait(lock);
			--this->waiters;
		}
		if (this->anyDurablyHeldByOther(hashes, owner)) {
			return false;
		}
		this->held.reserve(this->held.size() + hashes.size());
		size_t inserted = 0;
		try {
			for (; inserted < hashes.size(); inserted++) {
				this->held.insert(hashes[inserted]);
			}
		} catch (...) {
			for (size_t i = 0; i < inserted; i++) {
				this->held.erase(hashes[i]);
			}
			throw;
		}
		return true;
	}

	void release(std::span<const uint64_t> hashes) noexcept {
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

	/**
	 * Converts the caller's transient holds into durable holds owned by `owner`.
	 * Strong exception guarantee: a failed insertion leaves the transient holds
	 * in place and no new durable entry behind.
	 */
	void promote(std::span<const uint64_t> hashes, const void* owner) {
		std::unique_lock<std::mutex> lock(this->mutex);
		this->durablyHeld.reserve(this->durablyHeld.size() + hashes.size());
		size_t inserted = 0;
		try {
			for (; inserted < hashes.size(); inserted++) {
				this->durablyHeld.try_emplace(hashes[inserted], owner);
			}
		} catch (...) {
			for (size_t i = 0; i < inserted; i++) {
				auto it = this->durablyHeld.find(hashes[i]);
				if (it != this->durablyHeld.end() && it->second == owner && this->held.count(hashes[i]) != 0) {
					this->durablyHeld.erase(it);
				}
			}
			throw;
		}
		for (uint64_t hash : hashes) {
			this->held.erase(hash);
		}
		const bool wake = this->waiters > 0;
		lock.unlock();
		if (wake) {
			this->released.notify_all();
		}
	}

	void releaseDurable(std::span<const uint64_t> hashes, const void* owner) noexcept {
		std::lock_guard<std::mutex> lock(this->mutex);
		for (uint64_t hash : hashes) {
			auto it = this->durablyHeld.find(hash);
			if (it != this->durablyHeld.end() && it->second == owner) {
				this->durablyHeld.erase(it);
			}
		}
	}

	size_t heldCount() {
		std::lock_guard<std::mutex> lock(this->mutex);
		return this->held.size() + this->durablyHeld.size();
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

	bool anyDurablyHeldByOther(std::span<const uint64_t> hashes, const void* owner) const {
		if (this->durablyHeld.empty()) {
			return false;
		}
		for (uint64_t hash : hashes) {
			auto it = this->durablyHeld.find(hash);
			if (it != this->durablyHeld.end() && it->second != owner) {
				return true;
			}
		}
		return false;
	}

	std::mutex mutex;
	std::condition_variable released;
	std::unordered_set<uint64_t> held;
	std::unordered_map<uint64_t, const void*> durablyHeld;
	uint32_t waiters = 0;
};

/**
 * One writer's transient hold; `release()` is idempotent so a commit can
 * release at the right point and still rely on the destructor on error paths.
 * A single key is held inline, so a direct write allocates nothing of its own.
 */
struct CommitKeyReservationScope final {
	CommitKeyReservation* reservation = nullptr;
	std::vector<uint64_t> hashes;
	uint64_t single = 0;
	bool isSingle = false;

	CommitKeyReservationScope() = default;
	CommitKeyReservationScope(const CommitKeyReservationScope&) = delete;
	CommitKeyReservationScope& operator=(const CommitKeyReservationScope&) = delete;

	~CommitKeyReservationScope() {
		this->release();
	}

	[[nodiscard]] bool acquire(CommitKeyReservation& target, std::vector<uint64_t> keyHashes, const void* owner) {
		this->release();
		this->hashes = std::move(keyHashes);
		this->isSingle = false;
		if (!target.acquire(this->hashes, owner)) {
			return false;
		}
		this->reservation = &target;
		return true;
	}

	[[nodiscard]] bool acquireOne(CommitKeyReservation& target, uint64_t hash, const void* owner) {
		this->release();
		this->single = hash;
		this->isSingle = true;
		if (!target.acquire(std::span<const uint64_t>(&this->single, 1), owner)) {
			return false;
		}
		this->reservation = &target;
		return true;
	}

	std::span<const uint64_t> span() const {
		return this->isSingle ? std::span<const uint64_t>(&this->single, 1) : std::span<const uint64_t>(this->hashes);
	}

	void release() noexcept {
		if (this->reservation == nullptr) {
			return;
		}
		CommitKeyReservation* target = this->reservation;
		this->reservation = nullptr;
		target->release(this->span());
		if (!this->isSingle) {
			this->hashes.clear();
		}
	}

	bool active() const {
		return this->reservation != nullptr;
	}
};

/**
 * The durable hold a transaction keeps between a failed post-log commit and
 * the retry that lands it, released by that commit or by `close()`.
 */
struct DurableKeyHold final {
	CommitKeyReservation* reservation = nullptr;
	std::vector<uint64_t> hashes;

	/**
	 * Takes over `scope`'s transient hold as a durable hold owned by `owner`,
	 * keeping any keys an earlier adoption already holds.
	 */
	void adopt(CommitKeyReservationScope& scope, const void* owner) {
		if (!scope.active()) {
			return;
		}
		CommitKeyReservation* target = scope.reservation;
		std::vector<uint64_t> merged(this->hashes);
		for (uint64_t hash : scope.span()) {
			if (std::find(merged.begin(), merged.end(), hash) == merged.end()) {
				merged.push_back(hash);
			}
		}
		target->promote(scope.span(), owner);
		scope.reservation = nullptr;
		scope.hashes.clear();
		this->reservation = target;
		this->hashes = std::move(merged);
	}

	void release(const void* owner) noexcept {
		if (this->reservation == nullptr) {
			return;
		}
		this->reservation->releaseDurable(this->hashes, owner);
		this->reservation = nullptr;
		this->hashes.clear();
	}
};

} // namespace rocksdb_js

#endif
