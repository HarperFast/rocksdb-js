#include <gtest/gtest.h>
#include <string>
#include "core/json.h"

using rocksdb_js::appendJsonString;
using rocksdb_js::jsonString;

TEST(Json, EmptyString) {
	EXPECT_EQ(jsonString(""), "\"\"");
}

TEST(Json, PlainAscii) {
	EXPECT_EQ(jsonString("hello world"), "\"hello world\"");
}

TEST(Json, EscapesDoubleQuote) {
	EXPECT_EQ(jsonString("a\"b"), "\"a\\\"b\"");
}

TEST(Json, EscapesBackslash) {
	// Mirrors a Windows path that previously broke the ad-hoc escaper:
	// C:\foo\bar.log
	EXPECT_EQ(jsonString("C:\\foo\\bar.log"), "\"C:\\\\foo\\\\bar.log\"");
}

TEST(Json, EscapesNamedControlChars) {
	EXPECT_EQ(jsonString("\b\f\n\r\t"), "\"\\b\\f\\n\\r\\t\"");
}

TEST(Json, EscapesOtherControlCharsAsUnicode) {
	// 0x01 has no named escape and must be emitted as .
	EXPECT_EQ(jsonString(std::string("\x01", 1)), "\"\\u0001\"");
	// 0x1f is the upper bound of the control range.
	EXPECT_EQ(jsonString(std::string("\x1f", 1)), "\"\\u001f\"");
}

TEST(Json, PassesThroughHighBytesVerbatim) {
	// UTF-8 multi-byte sequences (>= 0x80) round-trip without re-encoding.
	// "ñ" is 0xC3 0xB1.
	const std::string input = "\xC3\xB1";
	const std::string expected = "\"\xC3\xB1\"";
	EXPECT_EQ(jsonString(input), expected);
}

TEST(Json, AppendPreservesExistingOutput) {
	std::string out = "prefix:";
	appendJsonString(out, "x");
	EXPECT_EQ(out, "prefix:\"x\"");
}

TEST(Json, EscapeCombination) {
	// Mix of all escape categories in one input.
	const std::string input = std::string("a\"b\\c\nd\x01" "e", 9);
	EXPECT_EQ(jsonString(input), "\"a\\\"b\\\\c\\nd\\u0001e\"");
}
