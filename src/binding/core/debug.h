#ifndef __CORE_DEBUG_H__
#define __CORE_DEBUG_H__

#include <cstdio>
#include <cstddef>

namespace rocksdb_js {

void debugLog(const bool showTimestamp, const char* msg, ...);

} // namespace rocksdb_js

#ifdef DEBUG
	#define DEBUG_LOG(msg, ...) \
		rocksdb_js::debugLog(true, msg, ##__VA_ARGS__)
	#define DEBUG_LOG_KEY(key) \
		do { \
			bool isPrintable = true; \
			for (size_t i = 0; i < key.size() && isPrintable; i++) { \
				unsigned char c = key.data()[i]; \
				isPrintable = (c >= 32 && c <= 126); \
			} \
			if (isPrintable && key.size() > 0) { \
				::fprintf(stderr, " \"%.*s\"", (int)key.size(), key.data()); \
			} else { \
				for (size_t i = 0; i < key.size(); i++) { \
					::fprintf(stderr, " %02x", (unsigned char)key.data()[i]); \
				} \
			} \
		} while (0)
	#define DEBUG_LOG_KEY_LN(key) \
		do { \
			DEBUG_LOG_KEY(key); \
			::fprintf(stderr, "\n"); \
		} while (0)
	#define DEBUG_LOG_MSG(msg, ...) \
		rocksdb_js::debugLog(false, msg, ##__VA_ARGS__)
#else
	#define DEBUG_LOG(msg, ...) do { ; } while (0)
	#define DEBUG_LOG_KEY(key) do { ; } while (0)
	#define DEBUG_LOG_KEY_LN(key) do { ; } while (0)
	#define DEBUG_LOG_MSG(msg, ...) do { ; } while (0)
#endif

#endif
