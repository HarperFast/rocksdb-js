#ifndef __CORE_TEST_SEAM_H__
#define __CORE_TEST_SEAM_H__

#include <cstdlib>

// Deterministic test seams that widen a race window are gated on a millisecond
// delay read once from an environment variable (0 = disabled). They are inert in
// production where the env var is unset.
//
// DEFINE_TEST_DELAY_MS(funcName, envName) defines a `static int funcName()` that
// returns that delay, reading `envName` exactly once into a function-local static.
// Each seam keeps its own named accessor (and single cached read) at the call site;
// see EventEmitter::notify and TransactionHandle::close for usage.
#define DEFINE_TEST_DELAY_MS(funcName, envName) \
	static int funcName() { \
		static const int delayMs = []() -> int { \
			const char* value = ::getenv(envName); \
			return value ? ::atoi(value) : 0; \
		}(); \
		return delayMs; \
	}

#endif
