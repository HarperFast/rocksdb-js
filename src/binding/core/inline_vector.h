#ifndef __CORE_INLINE_VECTOR_H__
#define __CORE_INLINE_VECTOR_H__

#include <cstddef>
#include <utility>
#include <vector>

namespace rocksdb_js {

/**
 * A vector that holds its first `N` elements inline and spills the rest into
 * heap storage. Both users — the set of column-family generations a
 * transaction has staged a write to, and a commit's claim over that set — are
 * rebuilt per transaction on paths that run for every staged write, and a
 * transaction almost always names one or two families, so the inline slots
 * keep the common case allocation-free.
 *
 * `overflow` is public so a caller can `reserve()` it before publishing
 * anything: `ColumnFamilyCommitClaim::admit` secures storage for every claim
 * up front, so recording one can never fail after its count was taken
 * (invariant 24).
 */
template <typename T, size_t N>
struct InlineVector final {
	static constexpr size_t inlineCapacity = N;
	T inlineSlots[N];
	std::vector<T> overflow;
	size_t count = 0;

	T& add(T value) {
		if (this->count < N) {
			this->inlineSlots[this->count] = std::move(value);
			this->count++;
			return this->inlineSlots[this->count - 1];
		}
		this->overflow.push_back(std::move(value));
		this->count++;
		return this->overflow.back();
	}

	const T* last() const noexcept {
		if (this->count == 0) {
			return nullptr;
		}
		return this->count <= N
			? &this->inlineSlots[this->count - 1]
			: &this->overflow.back();
	}

	void popBack() {
		if (this->count == 0) {
			return;
		}
		if (this->count <= N) {
			this->inlineSlots[this->count - 1] = T();
		} else {
			this->overflow.pop_back();
		}
		this->count--;
	}

	template <typename Fn>
	void forEach(Fn&& fn) const {
		const size_t inlineCount = this->count < N ? this->count : N;
		for (size_t i = 0; i < inlineCount; i++) {
			fn(this->inlineSlots[i]);
		}
		for (const auto& entry : this->overflow) {
			fn(entry);
		}
	}

	void clear() {
		const size_t inlineCount = this->count < N ? this->count : N;
		for (size_t i = 0; i < inlineCount; i++) {
			this->inlineSlots[i] = T();
		}
		this->overflow.clear();
		this->count = 0;
	}

	size_t size() const {
		return this->count;
	}
};

} // namespace rocksdb_js

#endif
