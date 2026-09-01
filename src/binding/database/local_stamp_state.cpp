#include "database/local_stamp_state.h"

#include <cmath>
#include <cstring>
#include <filesystem>
#include <fstream>
#include "rocksdb/iterator.h"
#include "rocksdb/write_batch.h"
#include "core/encoding.h"
#include "core/exception.h"
#include "core/platform.h"
#include "core/test_seam.h"

namespace rocksdb_js {

namespace {

double readStampRow(const std::string& value, const char* rowName) {
	if (value.size() != 8) {
		throw rocksdb_js::DBException(
			std::string("Invalid local-stamp metadata row \"") + rowName + "\": wrong size");
	}
	double parsed = readDoubleBE(value.data());
	if (!std::isfinite(parsed) || parsed < 0.0 || parsed >= LOCAL_STAMP_MAX) {
		throw rocksdb_js::DBException(
			std::string("Invalid local-stamp metadata row \"") + rowName + "\": out of domain");
	}
	return parsed;
}

} // namespace

double LocalStampState::claim(double candidate, bool candidateIsReceiverTime) {
	for (;;) {
		StampClaim result = tryClaimLocalStamp(
			this->watermark, this->reserve, candidate, candidateIsReceiverTime,
			&getMonotonicTimestamp);
		switch (result.status) {
			case StampClaimStatus::Claimed:
				return result.value;
			case StampClaimStatus::NeedsReserve:
				this->extendReserve(result.value);
				break;
			case StampClaimStatus::Exhausted:
				throw rocksdb_js::DBException(
					"Local mutation stamp domain exhausted; the stamp reserve ceiling has "
					"reached the maximum timestamp — repair the database's clock metadata "
					"before writing");
		}
	}
}

StampClaim LocalStampState::tryClaimNoExtend(double candidate, bool candidateIsReceiverTime) {
	return tryClaimLocalStamp(
		this->watermark, this->reserve, candidate, candidateIsReceiverTime,
		&getMonotonicTimestamp);
}

void LocalStampState::ensureHeadroom(double candidate) {
	const double now = getMonotonicTimestamp();
	const double needed = (candidate > now ? candidate : now);
	const double ceiling = localStampFromBits(this->reserve.load(std::memory_order_acquire));
	if (needed >= ceiling) {
		// Out of headroom: extend synchronously before the caller takes any log
		// append lock.
		this->extendReserve(needed);
		return;
	}
	if (needed + STAMP_RESERVE_MARGIN_MS >= ceiling &&
		!this->extensionScheduled.exchange(true, std::memory_order_acq_rel)) {
		// Near the margin: extend proactively off the claim path (single-flight,
		// state-owned thread). Failure here is not a commit failure — the claim
		// path re-extends synchronously when headroom actually runs out.
		auto self = this->shared_from_this();
		try {
			std::lock_guard<std::mutex> lock(this->extendMutex);
			if (this->closed.load(std::memory_order_acquire)) {
				this->extensionScheduled.store(false, std::memory_order_release);
				return;
			}
			if (this->extenderThread.joinable()) {
				this->extenderThread.join();
			}
			this->extenderThread = std::thread([self] {
				try {
					self->extendReserve(getMonotonicTimestamp());
				} catch (...) {
				}
				self->extensionScheduled.store(false, std::memory_order_release);
			});
		} catch (...) {
			this->extensionScheduled.store(false, std::memory_order_release);
		}
	}
}

void LocalStampState::extendReserve(double target) {
	std::lock_guard<std::mutex> lock(this->extendMutex);
	if (this->closed.load(std::memory_order_acquire)) {
		throw rocksdb_js::DBException("Database is closing; cannot extend the stamp reserve");
	}
	const double current = localStampFromBits(this->reserve.load(std::memory_order_acquire));
	if (target < current) {
		return; // another extension already covered it
	}
	const double newCeiling =
		localStampReserveTarget(target, getMonotonicTimestamp(), STAMP_RESERVE_WINDOW_MS);
	if (newCeiling < target) {
		throw rocksdb_js::DBException(
			"Local mutation stamp domain exhausted; cannot reserve past the maximum "
			"timestamp — repair the database's clock metadata before writing");
	}
	char value[8];
	writeDoubleBE(value, newCeiling);
	rocksdb::WriteOptions writeOptions;
	writeOptions.sync = true;
	rocksdb::Status status = this->db->Put(
		writeOptions, this->metaCf.get(),
		rocksdb::Slice(STAMP_META_KEY_RESERVE), rocksdb::Slice(value, sizeof value));
	if (!status.ok()) {
		throw rocksdb_js::DBException(
			"Failed to persist the local-stamp reserve ceiling: " + status.ToString());
	}
	crashIfArmed("after-reserve-extend");
	// Publish only after the ceiling is durable (the reserve invariant).
	this->reserve.store(localStampToBits(newCeiling), std::memory_order_release);
}

void LocalStampState::persistCfMarker(uint32_t cfId, const std::string& name) {
	rocksdb::WriteOptions writeOptions;
	writeOptions.sync = true;
	const std::string key = std::string(STAMP_META_KEY_CF_PREFIX) + std::to_string(cfId);
	rocksdb::Status status = this->db->Put(
		writeOptions, this->metaCf.get(), rocksdb::Slice(key), rocksdb::Slice(name));
	if (!status.ok()) {
		throw rocksdb_js::DBException(
			"Failed to persist the commit-stamping marker for column family \"" + name +
			"\": " + status.ToString());
	}
}

void LocalStampState::persistLogGeneration(const std::string& storeName, uint64_t generation) {
	char value[8];
	writeUint64BE(value, generation);
	const std::string key = std::string(STAMP_META_KEY_LOG_PREFIX) + storeName;
	rocksdb::Status status = this->db->Put(
		rocksdb::WriteOptions(), this->metaCf.get(),
		rocksdb::Slice(key), rocksdb::Slice(value, sizeof value));
	if (!status.ok()) {
		throw rocksdb_js::DBException(
			"Failed to persist the log key-domain generation for store \"" + storeName +
			"\": " + status.ToString());
	}
}

void LocalStampState::persistCleanCloseFloor() {
	std::lock_guard<std::mutex> lock(this->extendMutex);
	if (this->closed.load(std::memory_order_acquire)) return;
	const double floor = localStampFromBits(this->watermark.load(std::memory_order_acquire));
	if (floor <= 0.0) return;
	char value[8];
	writeDoubleBE(value, floor);
	rocksdb::WriteOptions writeOptions;
	writeOptions.sync = true;
	// Best-effort: a failed clean-floor write only costs one reserve window of
	// logical skew on the next open (the crash path), never correctness.
	this->db->Put(
		writeOptions, this->metaCf.get(),
		rocksdb::Slice(STAMP_META_KEY_CLEAN_FLOOR), rocksdb::Slice(value, sizeof value));
}

void LocalStampState::shutdown() {
	this->closed.store(true, std::memory_order_release);
	// Drain any in-flight extension before the caller destroys RocksDB state.
	std::thread extender;
	{
		std::lock_guard<std::mutex> lock(this->extendMutex);
		if (this->extenderThread.joinable()) {
			extender = std::move(this->extenderThread);
		}
	}
	if (extender.joinable()) {
		extender.join();
	}
}

double readStampFloorArtifacts(const std::vector<std::string>& logDirs) {
	double best = 0.0;
	for (const auto& dir : logDirs) {
		if (dir.empty()) continue;
		for (const char* name : { STAMP_FLOOR_ARTIFACT_NAME, STAMP_FLOOR_ARTIFACT_PENDING_NAME }) {
			const std::filesystem::path path = std::filesystem::path(dir) / name;
			std::error_code ec;
			if (!std::filesystem::exists(path, ec) || ec) continue;
			std::ifstream in(path, std::ios::binary);
			char bytes[STAMP_FLOOR_ARTIFACT_SIZE];
			if (!in || !in.read(bytes, sizeof bytes)) {
				throw rocksdb_js::DBException(
					"Corrupt local-stamp floor artifact (short read): " + path.string());
			}
			if (std::memcmp(bytes, STAMP_FLOOR_ARTIFACT_TOKEN, 4) != 0) {
				throw rocksdb_js::DBException(
					"Corrupt local-stamp floor artifact (bad token): " + path.string());
			}
			const double ceiling = readDoubleBE(bytes + 4);
			const uint64_t complement = readUint64BE(bytes + 12);
			if (complement != ~localStampToBits(ceiling) || !std::isfinite(ceiling) ||
				ceiling < 0.0 || ceiling >= LOCAL_STAMP_MAX) {
				throw rocksdb_js::DBException(
					"Corrupt local-stamp floor artifact: " + path.string());
			}
			if (ceiling > best) best = ceiling;
		}
	}
	return best;
}

StampMetaContents loadStampMeta(rocksdb::DB* db, rocksdb::ColumnFamilyHandle* metaCf) {
	StampMetaContents contents;
	rocksdb::ReadOptions readOptions;
	std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(readOptions, metaCf));
	const std::string cfPrefix = STAMP_META_KEY_CF_PREFIX;
	const std::string logPrefix = STAMP_META_KEY_LOG_PREFIX;
	for (it->SeekToFirst(); it->Valid(); it->Next()) {
		contents.empty = false;
		const std::string key = it->key().ToString();
		const std::string value = it->value().ToString();
		if (key == STAMP_META_KEY_SCHEMA) {
			contents.schemaPresent = true;
			contents.schemaValid = value == STAMP_META_SCHEMA_VALUE;
		} else if (key == STAMP_META_KEY_RESERVE) {
			contents.reserve = readStampRow(value, STAMP_META_KEY_RESERVE);
		} else if (key == STAMP_META_KEY_CLEAN_FLOOR) {
			contents.cleanFloor = readStampRow(value, STAMP_META_KEY_CLEAN_FLOOR);
		} else if (key == STAMP_META_KEY_LOG_DOMAIN) {
			if (value.size() != 8) {
				throw rocksdb_js::DBException(
					"Invalid local-stamp metadata row \"stamp.logDomain\": wrong size");
			}
			contents.logDomainGeneration = readUint64BE(value.data());
		} else if (key.rfind(cfPrefix, 0) == 0) {
			const std::string idPart = key.substr(cfPrefix.size());
			if (idPart.empty() || idPart.size() > 10 ||
				idPart.find_first_not_of("0123456789") != std::string::npos) {
				throw rocksdb_js::DBException(
					"Invalid local-stamp metadata row \"" + key + "\": malformed column family id");
			}
			unsigned long long id = std::stoull(idPart);
			if (id > UINT32_MAX || value.size() > 4096) {
				throw rocksdb_js::DBException(
					"Invalid local-stamp metadata row \"" + key + "\": out of bounds");
			}
			contents.stampedCfIds.emplace(static_cast<uint32_t>(id), value);
		} else if (key.rfind(logPrefix, 0) == 0) {
			const std::string storeName = key.substr(logPrefix.size());
			if (storeName.empty() || storeName.size() > 4096 || value.size() != 8) {
				throw rocksdb_js::DBException(
					"Invalid local-stamp metadata row \"" + key + "\": malformed log generation");
			}
			contents.logGenerations.emplace(storeName, readUint64BE(value.data()));
		}
		// Unknown keys are tolerated (forward compatibility within the schema
		// version); the schema row gates structural changes.
	}
	if (!it->status().ok()) {
		throw rocksdb_js::DBException(
			"Failed to read local-stamp metadata: " + it->status().ToString());
	}
	return contents;
}

} // namespace rocksdb_js
