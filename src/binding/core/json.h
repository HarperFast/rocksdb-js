#ifndef __CORE_JSON_H__
#define __CORE_JSON_H__

#include <cstdio>
#include <string>
#include <string_view>

namespace rocksdb_js {

/**
 * Appends a JSON string literal (including surrounding double quotes) for
 * `s` to `out`, escaping per RFC 8259: `"`, `\`, and control characters
 * 0x00–0x1F. Bytes >= 0x20 are passed through verbatim, which means a
 * valid UTF-8 input produces a valid UTF-8 JSON literal without
 * re-encoding multi-byte sequences as `\uXXXX`.
 *
 * Inline so it can be used both from the N-API layer and from native
 * GoogleTest binaries that don't link the N-API code.
 */
inline void appendJsonString(std::string& out, std::string_view s) {
	out.reserve(out.size() + s.size() + 2);
	out += '"';
	for (unsigned char c : s) {
		switch (c) {
			case '"':  out += "\\\""; break;
			case '\\': out += "\\\\"; break;
			case '\b': out += "\\b";  break;
			case '\f': out += "\\f";  break;
			case '\n': out += "\\n";  break;
			case '\r': out += "\\r";  break;
			case '\t': out += "\\t";  break;
			default:
				if (c < 0x20) {
					char buf[7];
					std::snprintf(buf, sizeof(buf), "\\u%04x", c);
					out.append(buf, 6);
				} else {
					out += static_cast<char>(c);
				}
		}
	}
	out += '"';
}

/**
 * Convenience wrapper around `appendJsonString` that returns a freshly
 * built JSON string literal.
 */
inline std::string jsonString(std::string_view s) {
	std::string out;
	appendJsonString(out, s);
	return out;
}

} // namespace rocksdb_js

#endif
