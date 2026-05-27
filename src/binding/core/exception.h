#ifndef __CORE_EXCEPTION_H__
#define __CORE_EXCEPTION_H__

#include <exception>
#include <string>

namespace rocksdb_js {

/**
 * Exception class used throughout the native binding layer.
 *
 * std::runtime_error stores its message via MSVC's std::exception internal
 * char* buffer (_Copy_str / _Mywhat). When native addon code is called by
 * runtimes other than Node.js (e.g. Bun on Windows, which wraps N-API calls
 * with its own SEH / Zig exception frames), that buffer can become
 * disassociated from the exception object before what() is called in the
 * catch block, producing an empty string.
 *
 * DBException avoids this by storing the message in a plain std::string
 * member. what() returns message.c_str(), which is always valid for the full
 * lifetime of the exception object regardless of the surrounding runtime.
 */
class DBException final : public std::exception {
	std::string message;
public:
	explicit DBException(std::string msg) noexcept : message(std::move(msg)) {}
	explicit DBException(const char* msg) : message(msg ? msg : "") {}
	const char* what() const noexcept override { return message.c_str(); }
};

} // namespace rocksdb_js

#endif
