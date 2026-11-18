#ifndef __CORE_ASSERT_H__
#define __CORE_ASSERT_H__

#include <atomic>

/**
 * Internal consistency checks for the native binding. Never use the libc
 * `assert()`: nothing defines `NDEBUG`, so it aborts the host process in the
 * published release builds.
 *
 * `ROCKSDB_JS_ASSERT(condition, message)` is fatal (report, then
 * `std::abort()`) when `DEBUG` or `ROCKSDB_JS_FATAL_ASSERTS` is defined, which
 * CI builds set. Otherwise it reports once per call site to stderr and
 * continues, so the code after it must leave the process in a safe state.
 *
 * `ROCKSDB_JS_CHECK(condition, message)` is fatal in every build. Use it only
 * where continuing past the failure is unsafe in a way the code cannot repair.
 *
 * Both are callable from any thread, destructor, or teardown path: the report
 * is `noexcept`, allocates nothing and takes no locks. It still performs a
 * blocking stderr write, so call them after releasing any lifecycle lock.
 */

#if defined(DEBUG) || defined(ROCKSDB_JS_FATAL_ASSERTS)
	#define ROCKSDB_JS_ASSERTS_ARE_FATAL true
#else
	#define ROCKSDB_JS_ASSERTS_ARE_FATAL false
#endif

#if defined(__GNUC__) || defined(__clang__)
	#define ROCKSDB_JS_COLD __attribute__((cold, noinline))
#elif defined(_MSC_VER)
	#define ROCKSDB_JS_COLD __declspec(noinline)
#else
	#define ROCKSDB_JS_COLD
#endif

namespace rocksdb_js {

/**
 * Reports a failed check. Fatal: always writes, then aborts. Otherwise writes
 * only if `reported` was unset, and returns.
 */
ROCKSDB_JS_COLD void assertionFailed(
	const char* file,
	int line,
	const char* expression,
	const char* message,
	bool fatal,
	std::atomic<bool>& reported
) noexcept;

} // namespace rocksdb_js

#define ROCKSDB_JS_ASSERT_MODE(condition, message, fatal) \
	do { \
		if (!(condition)) [[unlikely]] { \
			static std::atomic<bool> rocksdbJsAssertReported{ false }; \
			::rocksdb_js::assertionFailed(__FILE__, __LINE__, #condition, message, fatal, rocksdbJsAssertReported); \
		} \
	} while (0)

#define ROCKSDB_JS_ASSERT(condition, message) ROCKSDB_JS_ASSERT_MODE(condition, message, ROCKSDB_JS_ASSERTS_ARE_FATAL)

#define ROCKSDB_JS_CHECK(condition, message) ROCKSDB_JS_ASSERT_MODE(condition, message, true)

#endif
