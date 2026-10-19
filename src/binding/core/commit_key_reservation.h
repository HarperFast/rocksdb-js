#ifndef __CORE_COMMIT_KEY_RESERVATION_H__
#define __CORE_COMMIT_KEY_RESERVATION_H__

#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <span>
#include <unordered_map>
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
 * One map holds both kinds: a transient entry has no owner, a durable entry
 * names its owner. Promoting a transient hold therefore allocates nothing,
 * which matters because it happens after the log batch is already durable.
 *
 * Keys are 64-bit hashes of (column family, key); a hash collision only makes
 * a writer wait or retry. Transient acquisition is all-or-nothing under one
 * mutex, so there is no lock order to respect and no deadlock: a transient
 * holder is always inside a log write or a RocksDB write, neither of which
 * waits on a reservation. Memory is proportional to the keys in flight, which
 * is why this is a map rather than a stripe array — at four commit threads
 * with 64-key transactions, stripes need about 2^16 entries (2.5 MiB per
 * database) before collisions stop serializing unrelated commits.
 */
class CommitKeyReservation final {
public:
	/**
	 * Takes a transient hold on every hash the caller does not already hold
	 * durably, waiting out other transient holders. Returns false, holding
	 * nothing new, when another owner's durable hold covers one of them.
	 * Strong exception guarantee.
	 */
	[[nodiscard]] bool acquire(std::span<const uint64_t> hashes, const void* owner) {
		std::unique_lock<std::mutex> lock(this->mutex);
		while (this->anyTransientlyHeld(hashes)) {
			++this->waiters;
			this->released.wait(lock);
			--this->waiters;
		}
		if (this->anyDurablyHeldByOther(hashes, owner)) {
			return false;
		}
		this->held.reserve(this->held.size() + hashes.size());
		std::vector<uint64_t> inserted;
		try {
			inserted.reserve(hashes.size());
			for (uint64_t hash : hashes) {
				if (this->held.try_emplace(hash, nullptr).second) {
					inserted.push_back(hash);
				}
			}
		} catch (...) {
			for (uint64_t hash : inserted) {
				this->held.erase(hash);
			}
			throw;
		}
		return true;
	}

	/** Drops the transient holds among `hashes`; durable entries are untouched. */
	void release(std::span<const uint64_t> hashes) noexcept {
		{
			std::lock_guard<std::mutex> lock(this->mutex);
			for (uint64_t hash : hashes) {
				auto it = this->held.find(hash);
				if (it != this->held.end() && it->second == nullptr) {
					this->held.erase(it);
				}
			}
			if (this->waiters == 0) {
				return;
			}
		}
		this->released.notify_all();
	}

	/** Turns the caller's transient holds into durable holds owned by `owner`; allocates nothing. */
	void promote(std::span<const uint64_t> hashes, const void* owner) noexcept {
		{
			std::lock_guard<std::mutex> lock(this->mutex);
			for (uint64_t hash : hashes) {
				auto it = this->held.find(hash);
				if (it != this->held.end() && it->second == nullptr) {
					it->second = owner;
				}
			}
			if (this->waiters == 0) {
				return;
			}
		}
		this->released.notify_all();
	}

	void releaseDurable(std::span<const uint64_t> hashes, const void* owner) noexcept {
		std::lock_guard<std::mutex> lock(this->mutex);
		for (uint64_t hash : hashes) {
			auto it = this->held.find(hash);
			if (it != this->held.end() && it->second == owner) {
				this->held.erase(it);
			}
		}
	}

	size_t heldCount() {
		std::lock_guard<std::mutex> lock(this->mutex);
		return this->held.size();
	}

private:
	bool anyTransientlyHeld(std::span<const uint64_t> hashes) const {
		for (uint64_t hash : hashes) {
			auto it = this->held.find(hash);
			if (it != this->held.end() && it->second == nullptr) {
				return true;
			}
		}
		return false;
	}

	bool anyDurablyHeldByOther(std::span<const uint64_t> hashes, const void* owner) const {
		for (uint64_t hash : hashes) {
			auto it = this->held.find(hash);
			if (it != this->held.end() && it->second != nullptr && it->second != owner) {
				return true;
			}
		}
		return false;
	}

	std::mutex mutex;
	std::condition_variable released;
	std::unordered_map<uint64_t, const void*> held;
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
 * the retry that lands it, released by that commit or by `close()`. Adoption
 * allocates nothing: the key set is the write set's, fixed once durable, so
 * the first adoption takes the scope's vector and later ones keep it.
 */
struct DurableKeyHold final {
	CommitKeyReservation* reservation = nullptr;
	std::vector<uint64_t> hashes;

	void adopt(CommitKeyReservationScope& scope, const void* owner) noexcept {
		if (!scope.active()) {
			return;
		}
		CommitKeyReservation* target = scope.reservation;
		target->promote(scope.span(), owner);
		scope.reservation = nullptr;
		if (this->reservation == nullptr) {
			this->reservation = target;
			this->hashes = std::move(scope.hashes);
		}
		scope.hashes.clear();
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
