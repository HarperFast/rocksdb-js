#include "core/background_error.h"

namespace rocksdb_js {

const char* backgroundErrorSeverityName(int severity) {
	switch (severity) {
		case rocksdb::Status::Severity::kNoError:
			return "none";
		case rocksdb::Status::Severity::kSoftError:
			return "soft";
		case rocksdb::Status::Severity::kHardError:
			return "hard";
		case rocksdb::Status::Severity::kFatalError:
			return "fatal";
		case rocksdb::Status::Severity::kUnrecoverableError:
			return "unrecoverable";
		default:
			return "unknown";
	}
}

const char* backgroundErrorReasonName(int reason) {
	switch (reason) {
		case static_cast<int>(rocksdb::BackgroundErrorReason::kFlush):
			return "flush";
		case static_cast<int>(rocksdb::BackgroundErrorReason::kCompaction):
			return "compaction";
		case static_cast<int>(rocksdb::BackgroundErrorReason::kWriteCallback):
			return "writeCallback";
		case static_cast<int>(rocksdb::BackgroundErrorReason::kMemTable):
			return "memtable";
		case static_cast<int>(rocksdb::BackgroundErrorReason::kManifestWrite):
			return "manifestWrite";
		case static_cast<int>(rocksdb::BackgroundErrorReason::kFlushNoWAL):
			return "flushNoWAL";
		case static_cast<int>(rocksdb::BackgroundErrorReason::kManifestWriteNoWAL):
			return "manifestWriteNoWAL";
		case static_cast<int>(rocksdb::BackgroundErrorReason::kAsyncFileOpen):
			return "asyncFileOpen";
		default:
			return "unknown";
	}
}

} // namespace rocksdb_js
