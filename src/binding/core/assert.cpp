#include <cerrno>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include "core/assert.h"
#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

namespace rocksdb_js {

namespace {

// Not the CRT's `_write`: on a closed fd 2 its invalid-parameter handler terminates the process.
void writeToStderr(const char* data, size_t length) noexcept {
#ifdef _WIN32
	HANDLE handle = ::GetStdHandle(STD_ERROR_HANDLE);
	if (handle == nullptr || handle == INVALID_HANDLE_VALUE) {
		return;
	}
	DWORD written = 0;
	::WriteFile(handle, data, static_cast<DWORD>(length), &written, nullptr);
#else
	while (length > 0) {
		const ssize_t written = ::write(STDERR_FILENO, data, length);
		if (written < 0) {
			if (errno == EINTR) {
				continue;
			}
			return;
		}
		data += written;
		length -= static_cast<size_t>(written);
	}
#endif
}

} // namespace

void assertionFailed(
	const char* file,
	int line,
	const char* expression,
	const char* message,
	bool fatal,
	std::atomic<bool>& reported
) noexcept {
	if (!fatal && reported.exchange(true, std::memory_order_relaxed)) {
		return;
	}

	const int savedErrno = errno;
#ifdef _WIN32
	const DWORD savedLastError = ::GetLastError();
#endif

	char buffer[1024];
	const int formatted = std::snprintf(
		buffer,
		sizeof(buffer),
		"rocksdb-js: %s: %s (%s) at %s:%d\n",
		fatal ? "fatal internal check failed" : "internal check failed, continuing",
		message,
		expression,
		file,
		line
	);
	if (formatted > 0) {
		size_t length = static_cast<size_t>(formatted);
		if (length >= sizeof(buffer)) {
			length = sizeof(buffer) - 1;
			buffer[length - 1] = '\n';
		}
		writeToStderr(buffer, length);
	}

#ifdef _WIN32
	::SetLastError(savedLastError);
#endif
	errno = savedErrno;

	if (fatal) {
		std::abort();
	}
}

} // namespace rocksdb_js
