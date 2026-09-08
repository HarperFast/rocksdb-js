#ifndef __CORE_COLUMN_FAMILY_GATE_H__
#define __CORE_COLUMN_FAMILY_GATE_H__

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "rocksdb/status.h"

namespace rocksdb_js {

/**
 * Per-column-family admission gate that serializes `DropColumnFamily` against
 * transaction commits naming the family (HarperFast/rocksdb-js#806, #726;
 * AGENTS.md invariant 20).
 *
 * A commit admitted past optimistic validation (or any pessimistic commit)
 * that reaches the memtable inserter after the family was dropped fails with
 * "Invalid column family specified in write batch", and RocksDB latches that as
 * a FATAL background error on the whole database. The gate keeps that batch out
 * of `DBImpl::Write`: a commit is admitted only while no drop has begun, and a
 * drop waits for every admitted commit to release before it removes the family.
 *
 * One 64-bit word: bit 63 = dropping, bit 62 = dropped, low bits = admitted
 * commits. Admission is a single read-modify-write, so the two sides cannot
 * both miss each other: either the commit's increment precedes the drop's flag
 * (the dropper then sees a non-zero count in the value `fetch_or` returns and
 * waits) or the flag precedes the increment (the commit sees it and backs out).
 *
 * Node-free so `test/native/column_family_gate_test.cc` can drive it. A gate is
 * a token with no RocksDB state: transactions may hold one past the family's
 * (and even the database's) lifetime without touching RocksDB on release.
 */
class ColumnFamilyGate final {
public:
	ColumnFamilyGate(uint32_t id, std::string name) : id(id), name(std::move(name)) {}

	ColumnFamilyGate(const ColumnFamilyGate&) = delete;
	ColumnFamilyGate& operator=(const ColumnFamilyGate&) = delete;

	const uint32_t id;
	const std::string name;

	/**
	 * Commit side. Returns false once a drop has begun; the caller must then
	 * keep the batch out of RocksDB. On success the caller owes exactly one
	 * `release()`.
	 */
	bool tryAdmit() {
		const uint64_t previous = this->state.fetch_add(1, std::memory_order_acq_rel);
		if (previous & kDropping) {
			this->release();
			return false;
		}
		return true;
	}

	void release() {
		const uint64_t now = this->state.fetch_sub(1, std::memory_order_acq_rel) - 1;
		if ((now & kDropping) && (now & kCountMask) == 0) {
			this->state.notify_all();
		}
	}

	/**
	 * Idempotent, and never reopened: a drop that fails after this point leaves
	 * the family refusing commits until a later drop succeeds, because reopening
	 * would race a concurrent second dropper past `waitForAdmitted`.
	 */
	bool beginDrop() {
		const uint64_t previous = this->state.fetch_or(kDropping, std::memory_order_acq_rel);
		return (previous & kDropping) == 0;
	}

	/** Only meaningful after `beginDrop()`: without the flag a new admission could land right after this returns. */
	void waitForAdmitted() {
		for (;;) {
			const uint64_t current = this->state.load(std::memory_order_acquire);
			if ((current & kCountMask) == 0) {
				return;
			}
			this->state.wait(current, std::memory_order_acquire);
		}
	}

	void markDropped() {
		this->state.fetch_or(kDropped, std::memory_order_acq_rel);
	}

	bool isDropping() const {
		return (this->state.load(std::memory_order_acquire) & kDropping) != 0;
	}

	bool isDropped() const {
		return (this->state.load(std::memory_order_acquire) & kDropped) != 0;
	}

	uint32_t admitted() const {
		return static_cast<uint32_t>(this->state.load(std::memory_order_acquire) & kCountMask);
	}

private:
	static constexpr uint64_t kDropping = uint64_t{1} << 63;
	static constexpr uint64_t kDropped = uint64_t{1} << 62;
	static constexpr uint64_t kCountMask = kDropped - 1;

	std::atomic<uint64_t> state{0};
};

/**
 * The status a commit is refused with; the wording is part of the public error
 * contract. Never throws: under allocation failure it degrades to the bare
 * status code, so a refusal can be produced on the commit lane, which has no
 * exception boundary of its own.
 */
inline rocksdb::Status columnFamilyDroppedStatus(const ColumnFamilyGate& gate) noexcept {
	try {
		return rocksdb::Status::ColumnFamilyDropped(
			"column family \"" + gate.name + (gate.isDropped() ? "\" was dropped" : "\" is being dropped")
		);
	} catch (...) {
		return rocksdb::Status::ColumnFamilyDropped();
	}
}

/**
 * The distinct droppable column families a transaction has staged writes
 * into: gate tokens deduplicated by pointer in an inline array, with a heap
 * vector allocated only once a transaction touches a ninth distinct family.
 * `note()` must run BEFORE the corresponding `txn->Put`/`Delete`: it is the
 * one place staging can allocate, and a throw there must leave the batch
 * untouched rather than leave a written family untracked.
 */
class StagedColumnFamilies final {
public:
	static constexpr size_t kInline = 8;

	/** Returns whether `gate` was newly recorded (so a failed write can `forgetLast()` it). */
	bool note(const std::shared_ptr<ColumnFamilyGate>& gate) {
		if (!gate) {
			return false;
		}
		for (size_t i = 0; i < this->inlineCount; ++i) {
			if (this->inlineGates[i].get() == gate.get()) {
				return false;
			}
		}
		if (this->inlineCount < kInline) {
			this->inlineGates[this->inlineCount++] = gate;
			return true;
		}
		for (const auto& held : this->overflow) {
			if (held.get() == gate.get()) {
				return false;
			}
		}
		this->overflow.push_back(gate);
		return true;
	}

	/** Undoes the most recent `note()` that returned true; a family whose write failed holds no gate. */
	void forgetLast() {
		if (!this->overflow.empty()) {
			this->overflow.pop_back();
		} else if (this->inlineCount > 0) {
			this->inlineGates[--this->inlineCount].reset();
		}
	}

	void clear() {
		for (size_t i = 0; i < this->inlineCount; ++i) {
			this->inlineGates[i].reset();
		}
		this->inlineCount = 0;
		this->overflow.clear();
	}

	size_t size() const { return this->inlineCount + this->overflow.size(); }

	ColumnFamilyGate& at(size_t index) const {
		return index < kInline ? *this->inlineGates[index] : *this->overflow[index - kInline];
	}

	/** Advisory: only admission is authoritative. */
	ColumnFamilyGate* firstDropping() const {
		for (size_t i = 0; i < this->size(); ++i) {
			ColumnFamilyGate& gate = this->at(i);
			if (gate.isDropping()) {
				return &gate;
			}
		}
		return nullptr;
	}

private:
	std::array<std::shared_ptr<ColumnFamilyGate>, kInline> inlineGates{};
	size_t inlineCount = 0;
	std::vector<std::shared_ptr<ColumnFamilyGate>> overflow;
};

/**
 * RAII holder for a commit's admissions: all-or-nothing across every family
 * the batch names, released together at scope exit (or immediately by the
 * first refusal). Non-blocking, so no acquisition order is needed to stay
 * deadlock-free. Holds raw gate pointers: the caller keeps every admitted
 * gate alive for the holder's lifetime.
 */
class ColumnFamilyAdmission final {
public:
	ColumnFamilyAdmission() = default;
	~ColumnFamilyAdmission() { this->release(); }

	ColumnFamilyAdmission(const ColumnFamilyAdmission&) = delete;
	ColumnFamilyAdmission& operator=(const ColumnFamilyAdmission&) = delete;

	void reserve(size_t total) {
		if (total > kInline) {
			this->overflow.reserve(total - kInline);
		}
	}

	bool admit(ColumnFamilyGate& gate) {
		if (!gate.tryAdmit()) {
			this->release();
			this->refusedGate = &gate;
			return false;
		}
		if (this->inlineCount < kInline) {
			this->inlineGates[this->inlineCount++] = &gate;
			return true;
		}
		try {
			this->overflow.push_back(&gate);
		} catch (...) {
			// An admission the holder cannot remember would wedge every later drop
			// of this family; give it back before propagating.
			gate.release();
			throw;
		}
		return true;
	}

	void release() {
		for (size_t i = 0; i < this->inlineCount; ++i) {
			this->inlineGates[i]->release();
		}
		this->inlineCount = 0;
		for (ColumnFamilyGate* gate : this->overflow) {
			gate->release();
		}
		this->overflow.clear();
	}

	ColumnFamilyGate* refused() const { return this->refusedGate; }

	size_t size() const { return this->inlineCount + this->overflow.size(); }

private:
	static constexpr size_t kInline = StagedColumnFamilies::kInline;

	std::array<ColumnFamilyGate*, kInline> inlineGates{};
	size_t inlineCount = 0;
	std::vector<ColumnFamilyGate*> overflow;
	ColumnFamilyGate* refusedGate = nullptr;
};

} // namespace rocksdb_js

#endif
