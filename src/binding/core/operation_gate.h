#ifndef __OPERATION_GATE_H__
#define __OPERATION_GATE_H__

#include <atomic>
#include <cstdint>
#include <memory>
#include <utility>

namespace rocksdb_js {

class OperationGate;

/**
 * Move-only RAII claim on an OperationGate. Borrowed claims are for call sites
 * that already retain the gate's owner. Shared claims keep the gate alive for
 * foreign work that can outlive that owner.
 */
class OperationClaim final {
public:
	OperationClaim() = default;
	~OperationClaim();

	OperationClaim(const OperationClaim&) = delete;
	OperationClaim& operator=(const OperationClaim&) = delete;

	OperationClaim(OperationClaim&& other) noexcept;
	OperationClaim& operator=(OperationClaim&& other) noexcept;

	explicit operator bool() const { return this->gate != nullptr; }

	static OperationClaim acquireBorrowed(OperationGate& gate);
	static OperationClaim acquireShared(std::shared_ptr<OperationGate> gate);

private:
	OperationClaim(OperationGate* gate, std::shared_ptr<OperationGate> owner) :
		gate(gate),
		owner(std::move(owner)) {}

	void reset();

	OperationGate* gate = nullptr;
	std::shared_ptr<OperationGate> owner;
};

/**
 * Close/drain fence shared by JavaScript operations and foreign storage users.
 * A successful claim is visible before closing is checked. Once closing wins,
 * every later claim rolls itself back and teardown waits for earlier claims.
 */
class alignas(64) OperationGate final {
public:
	bool isClosing() const { return this->closing.load(); }
	bool beginClose() { return !this->closing.exchange(true); }

	void waitForDrain() const {
		uint32_t current;
		while ((current = this->active.load()) != 0) {
			this->active.wait(current);
		}
	}

	uint32_t activeCount() const { return this->active.load(); }

#ifdef ROCKSDB_JS_NATIVE_TESTS
	using BeforeNotifyHook = void (*)();
	static void setBeforeNotifyHookForTest(BeforeNotifyHook hook) {
		beforeNotifyHook.store(hook);
	}
#endif

private:
	friend class OperationClaim;

	bool tryAcquire() {
		this->active.fetch_add(1);
		if (this->closing.load()) {
			this->release();
			return false;
		}
		return true;
	}

	void release() {
		uint32_t previous = this->active.fetch_sub(1);
		if (previous == 1 && this->closing.load()) {
#ifdef ROCKSDB_JS_NATIVE_TESTS
			if (auto hook = beforeNotifyHook.load()) {
				hook();
			}
#endif
			this->active.notify_all();
		}
	}

	std::atomic<bool> closing{false};
	std::atomic<uint32_t> active{0};

#ifdef ROCKSDB_JS_NATIVE_TESTS
	static inline std::atomic<BeforeNotifyHook> beforeNotifyHook{nullptr};
#endif
};

inline OperationClaim::~OperationClaim() {
	this->reset();
}

inline OperationClaim::OperationClaim(OperationClaim&& other) noexcept :
	gate(std::exchange(other.gate, nullptr)),
	owner(std::move(other.owner)) {}

inline OperationClaim& OperationClaim::operator=(OperationClaim&& other) noexcept {
	if (this != &other) {
		this->reset();
		this->gate = std::exchange(other.gate, nullptr);
		this->owner = std::move(other.owner);
	}
	return *this;
}

inline OperationClaim OperationClaim::acquireBorrowed(OperationGate& gate) {
	return gate.tryAcquire() ? OperationClaim(&gate, {}) : OperationClaim();
}

inline OperationClaim OperationClaim::acquireShared(std::shared_ptr<OperationGate> gate) {
	if (!gate || !gate->tryAcquire()) {
		return {};
	}
	return OperationClaim(gate.get(), std::move(gate));
}

inline void OperationClaim::reset() {
	if (this->gate) {
		this->gate->release();
		this->gate = nullptr;
		this->owner.reset();
	}
}

} // namespace rocksdb_js

#endif
