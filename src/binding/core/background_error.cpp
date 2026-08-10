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

bool backgroundErrorIsReadOnly(int severity) {
	return severity >= static_cast<int>(rocksdb::Status::Severity::kHardError);
}

void BackgroundErrorMirror::latch(int reason, const rocksdb::Status& status) {
	// Format the message OUTSIDE the lock: this runs on a RocksDB flush/
	// compaction/write thread and Status::ToString() allocates, so the mutex is
	// held only for the assignment.
	BackgroundErrorInfo next;
	next.latched = true;
	next.message = status.ToString();
	next.severity = static_cast<int>(status.severity());
	next.reason = reason;

	std::lock_guard<std::mutex> lock(this->mutex_);
	this->info_ = std::move(next);
	++this->generation_;
}

bool BackgroundErrorMirror::clearIfUnchanged(uint64_t expectedGeneration) {
	std::lock_guard<std::mutex> lock(this->mutex_);
	if (this->generation_ != expectedGeneration) {
		return false; // a newer error latched since the caller observed it — keep it
	}
	if (this->info_.latched) {
		this->info_ = BackgroundErrorInfo{};
		++this->generation_;
	}
	return true;
}

void BackgroundErrorMirror::onRecoveryBegin() {
	std::lock_guard<std::mutex> lock(this->mutex_);
	this->recoveryGeneration_ = this->generation_;
	this->recoveryActive_ = true;
}

void BackgroundErrorMirror::reconcileRecoveryEnd(
	const std::string& recoveredMessage,
	int recoveredSeverity,
	bool recovered
) {
	std::lock_guard<std::mutex> lock(this->mutex_);
	// One begin per end: consume the recovery window so a later end with no begin
	// (or a duplicate end) cannot clear on this stale generation.
	const bool active = this->recoveryActive_;
	const uint64_t recoveryGeneration = this->recoveryGeneration_;
	this->recoveryActive_ = false;
	if (recovered) {
		// Clear only if the latched error IS the exact one recovery worked on:
		// its generation is unchanged since onRecoveryBegin(). A newer error
		// latched in between bumped `generation_`, so it survives here — even
		// when it carries the same message as the recovered one (message
		// equality is not identity; see the header). Require a paired begin so
		// an end-only path never clears on a stale generation.
		if (active && this->info_.latched && this->generation_ == recoveryGeneration) {
			this->info_ = BackgroundErrorInfo{};
			++this->generation_;
		}
	} else {
		// Recovery failed. RocksDB retains old_bg_error as the read-only latch;
		// seed it only when nothing is currently latched, so a newer error is
		// never clobbered by the recovery-attempt's original error.
		if (!this->info_.latched) {
			this->info_.latched = true;
			this->info_.message = recoveredMessage;
			this->info_.severity = recoveredSeverity;
			this->info_.reason = -1;
			++this->generation_;
		}
	}
}

bool BackgroundErrorMirror::get(BackgroundErrorInfo& out, uint64_t* generation) const {
	std::lock_guard<std::mutex> lock(this->mutex_);
	out = this->info_;
	if (generation != nullptr) {
		*generation = this->generation_;
	}
	return this->info_.latched;
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
