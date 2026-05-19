// Standalone unit test for TransactionLogFile::writevAll.
//
// Forces a partial writev by writing into a pipe with a reduced buffer and
// verifies the helper retries until all bytes have been transferred. Bug
// context: HarperFast/rocksdb-js#572 / HarperFast/harper#610 — short writev
// returns silently dropped iovec tails, producing audit-log framing corruption.
//
// This file is intentionally not part of the binding.gyp build; it is compiled
// and run by `scripts/test-writev-partial.mjs`, which links it against the
// stripped-down writev_all.cpp. Keeps the test free of N-API / RocksDB deps.

#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <pthread.h>
#include <sys/uio.h>
#include <unistd.h>
#include <vector>

// forward declaration of the helper under test
namespace rocksdb_js {
struct TransactionLogFile {
	static int64_t writevAll(int fd, const iovec* iovecs, int iovcnt);
};
} // namespace rocksdb_js

namespace {

#define EXPECT(cond) do { \
	if (!(cond)) { \
		fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond); \
		std::exit(1); \
	} \
} while (0)

struct ReaderArgs {
	int fd;
	std::vector<char>* sink;
	size_t expectedBytes;
};

void* drainPipe(void* raw) {
	auto* args = static_cast<ReaderArgs*>(raw);
	args->sink->reserve(args->expectedBytes);
	char buf[4096];
	while (args->sink->size() < args->expectedBytes) {
		ssize_t n = ::read(args->fd, buf, sizeof(buf));
		if (n <= 0) break;
		args->sink->insert(args->sink->end(), buf, buf + n);
	}
	return nullptr;
}

// writevAll handles short writev returns when the pipe buffer fills. We send
// 1 MiB across 256 iovecs through a pipe whose default capacity is well under
// the total, so the kernel will return short counts.
void testPartialPipeWrite() {
	int pipefds[2];
	EXPECT(::pipe(pipefds) == 0);

	const size_t entrySize = 4096;
	const int iovcnt = 256;
	const size_t totalBytes = entrySize * iovcnt;

	std::vector<std::vector<char>> buffers;
	buffers.reserve(iovcnt);
	std::vector<iovec> iovecs;
	iovecs.reserve(iovcnt);
	for (int i = 0; i < iovcnt; ++i) {
		std::vector<char> chunk(entrySize);
		for (size_t j = 0; j < entrySize; ++j) {
			chunk[j] = static_cast<char>((i * 31 + j) & 0xff);
		}
		buffers.push_back(std::move(chunk));
		iovec iv = { buffers.back().data(), entrySize };
		iovecs.push_back(iv);
	}

	std::vector<char> sink;
	ReaderArgs args = { pipefds[0], &sink, totalBytes };
	pthread_t reader;
	EXPECT(::pthread_create(&reader, nullptr, drainPipe, &args) == 0);

	int64_t written = rocksdb_js::TransactionLogFile::writevAll(pipefds[1], iovecs.data(), iovcnt);
	::close(pipefds[1]);
	::pthread_join(reader, nullptr);
	::close(pipefds[0]);

	EXPECT(written == static_cast<int64_t>(totalBytes));
	EXPECT(sink.size() == totalBytes);

	for (int i = 0; i < iovcnt; ++i) {
		for (size_t j = 0; j < entrySize; ++j) {
			char expected = static_cast<char>((i * 31 + j) & 0xff);
			char actual = sink[i * entrySize + j];
			if (expected != actual) {
				fprintf(stderr, "FAIL byte mismatch at iovec=%d byte=%zu (expected=0x%02x actual=0x%02x)\n",
					i, j, static_cast<unsigned char>(expected), static_cast<unsigned char>(actual));
				std::exit(1);
			}
		}
	}
}

// writevAll above MAX_IOVS (1024) still writes all bytes — exercises the
// chunking loop with small iovecs into a regular file.
void testAboveMaxIovs() {
	char tmpl[] = "/tmp/rocksdb-js-writev-test-XXXXXX";
	int fd = ::mkstemp(tmpl);
	EXPECT(fd >= 0);
	::unlink(tmpl);

	const int iovcnt = 1500;
	const size_t entrySize = 17;
	const size_t totalBytes = entrySize * iovcnt;

	std::vector<std::vector<char>> buffers;
	buffers.reserve(iovcnt);
	std::vector<iovec> iovecs;
	iovecs.reserve(iovcnt);
	for (int i = 0; i < iovcnt; ++i) {
		std::vector<char> chunk(entrySize);
		for (size_t j = 0; j < entrySize; ++j) {
			chunk[j] = static_cast<char>((i * 13 + j) & 0xff);
		}
		buffers.push_back(std::move(chunk));
		iovec iv = { buffers.back().data(), entrySize };
		iovecs.push_back(iv);
	}

	int64_t written = rocksdb_js::TransactionLogFile::writevAll(fd, iovecs.data(), iovcnt);
	EXPECT(written == static_cast<int64_t>(totalBytes));

	EXPECT(::lseek(fd, 0, SEEK_SET) == 0);
	std::vector<char> readBack(totalBytes);
	size_t off = 0;
	while (off < totalBytes) {
		ssize_t n = ::read(fd, readBack.data() + off, totalBytes - off);
		EXPECT(n > 0);
		off += static_cast<size_t>(n);
	}
	::close(fd);

	for (int i = 0; i < iovcnt; ++i) {
		for (size_t j = 0; j < entrySize; ++j) {
			char expected = static_cast<char>((i * 13 + j) & 0xff);
			char actual = readBack[i * entrySize + j];
			if (expected != actual) {
				fprintf(stderr, "FAIL byte mismatch at iovec=%d byte=%zu\n", i, j);
				std::exit(1);
			}
		}
	}
}

// Zero iovcnt returns 0 without invoking writev.
void testZeroIovcnt() {
	int64_t written = rocksdb_js::TransactionLogFile::writevAll(-1, nullptr, 0);
	EXPECT(written == 0);
}

// Bad fd returns -1.
void testBadFd() {
	char data[4] = { 'a', 'b', 'c', 'd' };
	iovec iv = { data, 4 };
	int64_t written = rocksdb_js::TransactionLogFile::writevAll(-1, &iv, 1);
	EXPECT(written == -1);
}

} // anonymous namespace

int main() {
	testZeroIovcnt();
	testBadFd();
	testAboveMaxIovs();
	testPartialPipeWrite();
	printf("OK writev_partial_test\n");
	return 0;
}
