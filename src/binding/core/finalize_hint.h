#ifndef __CORE_FINALIZE_HINT_H__
#define __CORE_FINALIZE_HINT_H__

#include <atomic>
#include <cstdint>
#include <memory>
#include <utility>

namespace rocksdb_js {

/**
 * Finalize hint that keeps `value` alive for the external memory handed to one N-API create call.
 *
 * A failed create may already have run the finalizer, may run it later, or may never have registered
 * it: Node's `NAPI_PREAMBLE` refuses before taking ownership, while `node::Buffer::New` runs the
 * finalizer on some of its own failures. The status does not say which, so the creator and the
 * finalizer each hold a reference, and `failed()` releases `value` itself. When the finalizer was
 * never registered, only this block leaks. Use a new hint for every create attempt.
 */
template <typename T>
class SharedFinalizeHint final {
public:
	explicit SharedFinalizeHint(std::shared_ptr<T> value) : value(std::move(value)) {}

	SharedFinalizeHint(const SharedFinalizeHint&) = delete;
	SharedFinalizeHint& operator=(const SharedFinalizeHint&) = delete;

	T* get() const noexcept { return value.get(); }

	/** The create succeeded; the finalizer now owns `value`. */
	void created() noexcept { release(); }

	/** The create failed; the returned value is unreachable from JS, so `value` can go now. */
	void failed() noexcept {
		value.reset();
		release();
	}

	/** Body of the N-API finalizer; safe on any thread. */
	static void finalize(void* hint) noexcept {
		static_cast<SharedFinalizeHint*>(hint)->release();
	}

private:
	~SharedFinalizeHint() = default;

	void release() noexcept {
		if (references.fetch_sub(1, std::memory_order_acq_rel) == 1) {
			delete this;
		}
	}

	std::shared_ptr<T> value;
	std::atomic<uint32_t> references{ 2 };
};

} // namespace rocksdb_js

#endif
