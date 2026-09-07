#include "native_storage/native_storage_lease.h"

#ifdef ROCKSDB_JS_NATIVE_STORAGE_LEASE

#include "database/database.h"
#include "database/db_descriptor.h"
#include "database/db_handle.h"
#include "napi/macros.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/version.h"
#include "rocksdb/write_batch.h"
#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <new>
#include <string>
#include <utility>
#include <vector>

namespace rocksdb_js {
namespace {

constexpr uint64_t kMaxKeyBytes = 64 * 1024;
constexpr uint64_t kMaxOwnedBytes = 64 * 1024 * 1024;
constexpr uint64_t kMaxBatchMutations = 65'536;
constexpr uint64_t kMaxBatchBytes = 64 * 1024 * 1024;
constexpr uint64_t kMaxScanEntries = 4'096;
constexpr uint64_t kReadTimeoutMicros = 100'000;
constexpr char kProviderBuildIdentity[] = "rocksdb-js/2.8.0-phase0";
const uint8_t imageIdentity = 0;

struct StorageLeaseState final : Closable {
	std::atomic<DBDescriptor*> descriptor;
	std::shared_ptr<OperationGate> gate;
	std::weak_ptr<ColumnFamilyDescriptor> column;
	const uint64_t databaseIncarnation;
	const uint64_t columnIncarnation;
	std::atomic<bool> revoked{false};

	std::atomic<uint64_t> getOperations{0};
	std::atomic<uint64_t> scanOperations{0};
	std::atomic<uint64_t> batchOperations{0};
	std::atomic<uint64_t> requestedBytes{0};
	std::atomic<uint64_t> returnedBytes{0};
	std::atomic<uint64_t> copiedBytes{0};
	std::atomic<uint64_t> liveOwnedBuffers{0};
	std::atomic<uint64_t> providerErrors{0};

	StorageLeaseState(DBDescriptor* descriptor, std::shared_ptr<ColumnFamilyDescriptor> column) :
		descriptor(descriptor),
		gate(descriptor->operationGate),
		column(column),
		databaseIncarnation(descriptor->vtEpoch),
		columnIncarnation(column->incarnation) {}

	~StorageLeaseState() override {
		if (auto liveColumn = this->column.lock()) {
			liveColumn->unregisterNativeStorageLease();
		}

		// Close rejects this claim before its closable sweep can release the last state reference.
		auto claim = OperationClaim::acquireShared(this->gate);
		DBDescriptor* liveDescriptor = this->descriptor.load(std::memory_order_acquire);
		if (claim && liveDescriptor) {
			liveDescriptor->detach(this);
		}
	}

	void close() override {
		this->descriptor.store(nullptr, std::memory_order_release);
		this->revoked.store(true, std::memory_order_release);
	}
};

struct LeaseContext {
	std::atomic<uint32_t> references{1};
	std::shared_ptr<StorageLeaseState> state;
	rocksdb_js_storage_lease_v1 lease{};

	explicit LeaseContext(std::shared_ptr<StorageLeaseState> state) : state(std::move(state)) {}
};

struct Admission {
	// claim must outlive column so the RocksDB handle is released before close drains.
	OperationClaim claim;
	std::shared_ptr<ColumnFamilyDescriptor> column;
	DBDescriptor* descriptor = nullptr;
};

struct OwnedAllocation {
	LeaseContext* context;
	uint8_t* data;
};

bool validStatusBuffer(const rocksdb_js_status_buffer* status) {
	return status && status->struct_size >= sizeof(rocksdb_js_status_buffer);
}

void writeStatus(rocksdb_js_status_buffer* status, const char* message) noexcept {
	if (!validStatusBuffer(status)) return;
	status->length = 0;
	if (!status->data || status->capacity == 0 || !message) return;
	uint64_t length = static_cast<uint64_t>(std::strlen(message));
	uint64_t copied = std::min(length, status->capacity - 1);
	std::memcpy(status->data, message, static_cast<size_t>(copied));
	status->data[copied] = '\0';
	status->length = copied;
}

uint32_t fail(StorageLeaseState* state, uint32_t code, rocksdb_js_status_buffer* status, const char* message) noexcept {
	if (state && code != ROCKSDB_JS_STORAGE_NOT_FOUND && code != ROCKSDB_JS_STORAGE_BUSY) {
		state->providerErrors.fetch_add(1, std::memory_order_relaxed);
	}
	writeStatus(status, message);
	return code;
}

uint32_t mapStatus(StorageLeaseState* state, const rocksdb::Status& status, rocksdb_js_status_buffer* out) noexcept {
	if (status.ok()) {
		writeStatus(out, nullptr);
		return ROCKSDB_JS_STORAGE_OK;
	}
	if (status.IsNotFound()) return fail(state, ROCKSDB_JS_STORAGE_NOT_FOUND, out, "not found");
	if (status.IsBusy() || status.IsIncomplete() || status.IsTryAgain() || status.IsTimedOut()) {
		return fail(state, ROCKSDB_JS_STORAGE_BUSY, out, "storage is busy");
	}
	if (status.IsInvalidArgument()) {
		return fail(state, ROCKSDB_JS_STORAGE_INVALID_ARGUMENT, out, "invalid RocksDB operation");
	}
	if (status.IsIOError()) return fail(state, ROCKSDB_JS_STORAGE_IO_ERROR, out, "RocksDB I/O error");
	if (status.IsNotSupported()) {
		return fail(state, ROCKSDB_JS_STORAGE_UNSUPPORTED, out, "RocksDB operation is unsupported");
	}
	return fail(state, ROCKSDB_JS_STORAGE_INTERNAL, out, "RocksDB operation failed");
}

uint32_t acquire(LeaseContext* context, Admission& admission, rocksdb_js_status_buffer* status) noexcept {
	if (!context || !context->state) {
		return fail(nullptr, ROCKSDB_JS_STORAGE_CLOSED, status, "storage lease is closed");
	}
	auto* state = context->state.get();
	DBDescriptor* descriptor = state->descriptor.load(std::memory_order_acquire);
	if (!descriptor) return fail(state, ROCKSDB_JS_STORAGE_CLOSED, status, "storage lease is closed");

	auto claim = OperationClaim::acquireShared(state->gate);
	if (!claim) return fail(state, ROCKSDB_JS_STORAGE_CLOSED, status, "database is closing");
	if (state->descriptor.load(std::memory_order_acquire) != descriptor || state->revoked.load(std::memory_order_acquire)) {
		return fail(state, ROCKSDB_JS_STORAGE_CLOSED, status, "storage lease was revoked");
	}

	auto column = state->column.lock();
	if (!column || column->revoked.load() || column->incarnation != state->columnIncarnation) {
		return fail(state, ROCKSDB_JS_STORAGE_STALE_COLUMN_FAMILY, status, "column family is stale");
	}
	if (descriptor->vtEpoch != state->databaseIncarnation || !descriptor->db) {
		return fail(state, ROCKSDB_JS_STORAGE_CLOSED, status, "database incarnation is closed");
	}

	admission.claim = std::move(claim);
	admission.column = std::move(column);
	admission.descriptor = descriptor;
	return ROCKSDB_JS_STORAGE_OK;
}

bool retainContext(LeaseContext* context) noexcept {
	if (!context) return false;
	uint32_t current = context->references.load(std::memory_order_relaxed);
	while (current != 0 && current != UINT32_MAX) {
		if (context->references.compare_exchange_weak(current, current + 1, std::memory_order_acquire)) return true;
	}
	return false;
}

void releaseContext(void* rawContext) noexcept {
	auto* context = static_cast<LeaseContext*>(rawContext);
	if (context && context->references.fetch_sub(1, std::memory_order_acq_rel) == 1) {
		delete context;
	}
}

void releaseOwned(void* rawAllocation) noexcept {
	auto* allocation = static_cast<OwnedAllocation*>(rawAllocation);
	if (!allocation) return;
	allocation->context->state->liveOwnedBuffers.fetch_sub(1, std::memory_order_relaxed);
	delete[] allocation->data;
	LeaseContext* context = allocation->context;
	delete allocation;
	releaseContext(context);
}

bool prepareOwned(
	LeaseContext* context,
	rocksdb_js_owned_bytes* result,
	rocksdb_js_status_buffer* status
) noexcept {
	if (!result || result->struct_size < sizeof(rocksdb_js_owned_bytes)) {
		fail(context ? context->state.get() : nullptr, ROCKSDB_JS_STORAGE_INVALID_ARGUMENT, status, "invalid owned-bytes result");
		return false;
	}
	if (result->reserved != 0) {
		fail(context ? context->state.get() : nullptr, ROCKSDB_JS_STORAGE_INVALID_ARGUMENT, status, "owned-bytes reserved field must be zero");
		return false;
	}
	result->data = nullptr;
	result->length = 0;
	result->release_context = nullptr;
	result->release = nullptr;
	return true;
}

uint32_t setOwned(
	LeaseContext* context,
	const uint8_t* source,
	uint64_t length,
	rocksdb_js_owned_bytes* result,
	rocksdb_js_status_buffer* status
) noexcept {
	if (length > kMaxOwnedBytes) {
		return fail(context->state.get(), ROCKSDB_JS_STORAGE_LIMIT, status, "owned value exceeds limit");
	}
	if (length == 0) {
		result->release = releaseOwned;
		writeStatus(status, nullptr);
		return ROCKSDB_JS_STORAGE_OK;
	}
	if (!retainContext(context)) {
		return fail(context->state.get(), ROCKSDB_JS_STORAGE_CLOSED, status, "storage lease is closed");
	}
	uint8_t* copy = new (std::nothrow) uint8_t[static_cast<size_t>(length)];
	if (!copy) {
		releaseContext(context);
		return fail(context->state.get(), ROCKSDB_JS_STORAGE_LIMIT, status, "owned value allocation failed");
	}
	std::memcpy(copy, source, static_cast<size_t>(length));
	auto* allocation = new (std::nothrow) OwnedAllocation{context, copy};
	if (!allocation) {
		delete[] copy;
		releaseContext(context);
		return fail(context->state.get(), ROCKSDB_JS_STORAGE_LIMIT, status, "owned value allocation failed");
	}
	context->state->liveOwnedBuffers.fetch_add(1, std::memory_order_relaxed);
	context->state->returnedBytes.fetch_add(length, std::memory_order_relaxed);
	context->state->copiedBytes.fetch_add(length, std::memory_order_relaxed);
	result->data = copy;
	result->length = length;
	result->release_context = allocation;
	result->release = releaseOwned;
	writeStatus(status, nullptr);
	return ROCKSDB_JS_STORAGE_OK;
}

rocksdb::ReadOptions readOptions(DBDescriptor* descriptor) {
	rocksdb::ReadOptions options;
	options.deadline = std::chrono::microseconds(descriptor->db->GetEnv()->NowMicros() + kReadTimeoutMicros);
	options.io_timeout = std::chrono::microseconds(kReadTimeoutMicros);
	return options;
}

uint32_t leaseRetain(void* rawContext, rocksdb_js_status_buffer* status) noexcept {
	auto* context = static_cast<LeaseContext*>(rawContext);
	if (!retainContext(context)) return fail(nullptr, ROCKSDB_JS_STORAGE_CLOSED, status, "storage lease is closed");
	writeStatus(status, nullptr);
	return ROCKSDB_JS_STORAGE_OK;
}

uint32_t pollState(void* rawContext) noexcept {
	auto* context = static_cast<LeaseContext*>(rawContext);
	if (!context || !context->state || context->state->revoked.load(std::memory_order_acquire)) {
		return ROCKSDB_JS_STORAGE_STATE_REVOKED;
	}
	if (context->state->gate->isClosing()) return ROCKSDB_JS_STORAGE_STATE_CLOSING;
	auto column = context->state->column.lock();
	if (!column || column->revoked.load()) return ROCKSDB_JS_STORAGE_STATE_REVOKED;
	return ROCKSDB_JS_STORAGE_STATE_ACTIVE;
}

uint32_t getOwned(
	void* rawContext,
	rocksdb_js_byte_span key,
	rocksdb_js_owned_bytes* result,
	rocksdb_js_status_buffer* status
) noexcept {
	auto* context = static_cast<LeaseContext*>(rawContext);
	try {
		if (!prepareOwned(context, result, status)) return ROCKSDB_JS_STORAGE_INVALID_ARGUMENT;
		if (!context || !context->state || key.length == 0 || (key.length != 0 && key.data == nullptr)) {
			return fail(context ? context->state.get() : nullptr, ROCKSDB_JS_STORAGE_INVALID_ARGUMENT, status, "invalid key");
		}
		if (key.length > kMaxKeyBytes) {
			return fail(context->state.get(), ROCKSDB_JS_STORAGE_LIMIT, status, "key exceeds limit");
		}
		Admission admission;
		uint32_t admitted = acquire(context, admission, status);
		if (admitted != ROCKSDB_JS_STORAGE_OK) return admitted;
		context->state->getOperations.fetch_add(1, std::memory_order_relaxed);
		context->state->requestedBytes.fetch_add(key.length, std::memory_order_relaxed);

		rocksdb::PinnableSlice value;
		rocksdb::Slice keySlice(reinterpret_cast<const char*>(key.data), static_cast<size_t>(key.length));
		rocksdb::Status rocksStatus = admission.descriptor->db->Get(
			readOptions(admission.descriptor), admission.column->column.get(), keySlice, &value
		);
		if (!rocksStatus.ok()) return mapStatus(context->state.get(), rocksStatus, status);
		return setOwned(context, reinterpret_cast<const uint8_t*>(value.data()), value.size(), result, status);
	} catch (...) {
		return fail(context ? context->state.get() : nullptr, ROCKSDB_JS_STORAGE_INTERNAL, status, "provider exception");
	}
}

uint32_t writeBatch(
	void* rawContext,
	const rocksdb_js_storage_mutation* mutations,
	uint64_t mutationCount,
	uint64_t mutationStride,
	uint32_t policy,
	rocksdb_js_status_buffer* status
) noexcept {
	auto* context = static_cast<LeaseContext*>(rawContext);
	try {
		if (!context || !context->state || mutationCount == 0 || mutationCount > kMaxBatchMutations || !mutations ||
			mutationStride != sizeof(rocksdb_js_storage_mutation)
		) {
			return fail(context ? context->state.get() : nullptr, ROCKSDB_JS_STORAGE_INVALID_ARGUMENT, status, "invalid mutation batch");
		}
		rocksdb::WriteOptions options;
		options.no_slowdown = true;
		options.ignore_missing_column_families = false;
		switch (policy) {
			case ROCKSDB_JS_STORAGE_WAL: break;
			case ROCKSDB_JS_STORAGE_WAL_SYNC: options.sync = true; break;
			case ROCKSDB_JS_STORAGE_NO_WAL: options.disableWAL = true; break;
			default:
				return fail(context->state.get(), ROCKSDB_JS_STORAGE_INVALID_ARGUMENT, status, "invalid write policy");
		}

		Admission admission;
		uint32_t admitted = acquire(context, admission, status);
		if (admitted != ROCKSDB_JS_STORAGE_OK) return admitted;

		uint64_t totalBytes = 0;
		rocksdb::WriteBatch batch;
		for (uint64_t i = 0; i < mutationCount; ++i) {
			const auto mutation = mutations[i];
			if (mutation.struct_size < sizeof(rocksdb_js_storage_mutation) || mutation.key.length == 0 ||
				(mutation.key.length != 0 && mutation.key.data == nullptr) ||
				(mutation.value.length != 0 && mutation.value.data == nullptr) ||
				(mutation.kind == ROCKSDB_JS_STORAGE_DELETE && mutation.value.length != 0) ||
				(mutation.kind != ROCKSDB_JS_STORAGE_PUT && mutation.kind != ROCKSDB_JS_STORAGE_DELETE)
			) {
				return fail(context->state.get(), ROCKSDB_JS_STORAGE_INVALID_ARGUMENT, status, "invalid mutation");
			}
			if (mutation.key.length > kMaxKeyBytes || mutation.value.length > kMaxOwnedBytes ||
				mutation.key.length > kMaxBatchBytes - totalBytes ||
				mutation.value.length > kMaxBatchBytes - totalBytes - mutation.key.length
			) {
				return fail(context->state.get(), ROCKSDB_JS_STORAGE_LIMIT, status, "mutation batch exceeds limits");
			}
			totalBytes += mutation.key.length + mutation.value.length;
			rocksdb::Slice key(reinterpret_cast<const char*>(mutation.key.data), static_cast<size_t>(mutation.key.length));
			rocksdb::Status batchStatus;
			if (mutation.kind == ROCKSDB_JS_STORAGE_PUT) {
				rocksdb::Slice value(reinterpret_cast<const char*>(mutation.value.data), static_cast<size_t>(mutation.value.length));
				batchStatus = batch.Put(admission.column->column.get(), key, value);
			} else {
				batchStatus = batch.Delete(admission.column->column.get(), key);
			}
			if (!batchStatus.ok()) return mapStatus(context->state.get(), batchStatus, status);
		}
		context->state->batchOperations.fetch_add(1, std::memory_order_relaxed);
		context->state->requestedBytes.fetch_add(totalBytes, std::memory_order_relaxed);
		return mapStatus(context->state.get(), admission.descriptor->db->Write(options, &batch), status);
	} catch (...) {
		return fail(context ? context->state.get() : nullptr, ROCKSDB_JS_STORAGE_INTERNAL, status, "provider exception");
	}
}

void appendU32(std::vector<uint8_t>& bytes, uint32_t value) {
	for (unsigned shift = 0; shift < 32; shift += 8) bytes.push_back(static_cast<uint8_t>(value >> shift));
}

void appendU64(std::vector<uint8_t>& bytes, uint64_t value) {
	for (unsigned shift = 0; shift < 64; shift += 8) bytes.push_back(static_cast<uint8_t>(value >> shift));
}

uint32_t scanPage(
	void* rawContext,
	rocksdb_js_byte_span prefix,
	rocksdb_js_byte_span startAfter,
	uint64_t entryLimit,
	uint64_t byteLimit,
	rocksdb_js_owned_bytes* page,
	rocksdb_js_status_buffer* status
) noexcept {
	auto* context = static_cast<LeaseContext*>(rawContext);
	try {
		if (!prepareOwned(context, page, status)) return ROCKSDB_JS_STORAGE_INVALID_ARGUMENT;
		if (!context || !context->state || (prefix.length != 0 && prefix.data == nullptr) ||
			(startAfter.length != 0 && startAfter.data == nullptr) || entryLimit == 0 ||
			byteLimit < sizeof(uint32_t)
		) {
			return fail(context ? context->state.get() : nullptr, ROCKSDB_JS_STORAGE_INVALID_ARGUMENT, status, "invalid scan request");
		}
		if (prefix.length > kMaxKeyBytes || startAfter.length > kMaxKeyBytes ||
			entryLimit > kMaxScanEntries || byteLimit > kMaxOwnedBytes
		) {
			return fail(context->state.get(), ROCKSDB_JS_STORAGE_LIMIT, status, "scan request exceeds limits");
		}
		if (startAfter.length != 0 &&
			(startAfter.length < prefix.length || std::memcmp(startAfter.data, prefix.data, static_cast<size_t>(prefix.length)) != 0)
		) {
			return fail(context->state.get(), ROCKSDB_JS_STORAGE_INVALID_ARGUMENT, status, "scan cursor is outside prefix");
		}

		Admission admission;
		uint32_t admitted = acquire(context, admission, status);
		if (admitted != ROCKSDB_JS_STORAGE_OK) return admitted;
		context->state->scanOperations.fetch_add(1, std::memory_order_relaxed);
		context->state->requestedBytes.fetch_add(prefix.length + startAfter.length, std::memory_order_relaxed);

		auto options = readOptions(admission.descriptor);
		std::unique_ptr<rocksdb::Iterator> iterator(admission.descriptor->db->NewIterator(options, admission.column->column.get()));
		rocksdb::Slice prefixSlice(reinterpret_cast<const char*>(prefix.data), static_cast<size_t>(prefix.length));
		if (startAfter.length != 0) {
			rocksdb::Slice cursor(reinterpret_cast<const char*>(startAfter.data), static_cast<size_t>(startAfter.length));
			iterator->Seek(cursor);
			if (iterator->Valid() && iterator->key().compare(cursor) == 0) iterator->Next();
		} else if (prefix.length != 0) {
			iterator->Seek(prefixSlice);
		} else {
			iterator->SeekToFirst();
		}

		std::vector<uint8_t> bytes;
		bytes.reserve(static_cast<size_t>(std::min<uint64_t>(byteLimit, 1 << 20)));
		appendU32(bytes, 0);
		uint32_t entries = 0;
		while (iterator->Valid() && entries < entryLimit) {
			if (prefix.length != 0 && !iterator->key().starts_with(prefixSlice)) break;
			uint64_t remaining = byteLimit - bytes.size();
			uint64_t keySize = iterator->key().size();
			uint64_t valueSize = iterator->value().size();
			if (remaining < 16 || keySize > remaining - 16 || valueSize > remaining - 16 - keySize) {
				if (entries == 0) return fail(context->state.get(), ROCKSDB_JS_STORAGE_LIMIT, status, "scan entry exceeds page limit");
				break;
			}
			appendU64(bytes, iterator->key().size());
			appendU64(bytes, iterator->value().size());
			bytes.insert(bytes.end(), iterator->key().data(), iterator->key().data() + iterator->key().size());
			bytes.insert(bytes.end(), iterator->value().data(), iterator->value().data() + iterator->value().size());
			++entries;
			if ((entries & 63) == 0 && context->state->gate->isClosing()) {
				return fail(context->state.get(), ROCKSDB_JS_STORAGE_CLOSED, status, "database is closing");
			}
			if (entries == entryLimit) break;
			iterator->Next();
		}
		if (!iterator->status().ok() && !(entries != 0 && iterator->status().IsTimedOut())) {
			return mapStatus(context->state.get(), iterator->status(), status);
		}
		for (unsigned shift = 0; shift < 32; shift += 8) bytes[shift / 8] = static_cast<uint8_t>(entries >> shift);
		return setOwned(context, bytes.data(), bytes.size(), page, status);
	} catch (...) {
		return fail(context ? context->state.get() : nullptr, ROCKSDB_JS_STORAGE_INTERNAL, status, "provider exception");
	}
}

uint32_t collectStats(
	void* rawContext,
	rocksdb_js_storage_stats* result,
	rocksdb_js_status_buffer* status
) noexcept {
	auto* context = static_cast<LeaseContext*>(rawContext);
	if (!context || !context->state || !result || result->struct_size < sizeof(rocksdb_js_storage_stats)) {
		return fail(context ? context->state.get() : nullptr, ROCKSDB_JS_STORAGE_INVALID_ARGUMENT, status, "invalid statistics result");
	}
	if (result->reserved != 0) {
		return fail(context->state.get(), ROCKSDB_JS_STORAGE_INVALID_ARGUMENT, status, "statistics reserved field must be zero");
	}
	auto& state = *context->state;
	result->get_operations = state.getOperations.load(std::memory_order_relaxed);
	result->scan_operations = state.scanOperations.load(std::memory_order_relaxed);
	result->batch_operations = state.batchOperations.load(std::memory_order_relaxed);
	result->requested_bytes = state.requestedBytes.load(std::memory_order_relaxed);
	result->returned_bytes = state.returnedBytes.load(std::memory_order_relaxed);
	result->copied_bytes = state.copiedBytes.load(std::memory_order_relaxed);
	result->live_owned_buffers = state.liveOwnedBuffers.load(std::memory_order_relaxed);
	result->provider_errors = state.providerErrors.load(std::memory_order_relaxed);
	writeStatus(status, nullptr);
	return ROCKSDB_JS_STORAGE_OK;
}

void initializeLease(LeaseContext& context) {
	auto& lease = context.lease;
	lease.magic = ROCKSDB_JS_STORAGE_LEASE_MAGIC;
	lease.abi_major = ROCKSDB_JS_STORAGE_LEASE_ABI_MAJOR;
	lease.abi_minor = ROCKSDB_JS_STORAGE_LEASE_ABI_MINOR;
	lease.struct_size = sizeof(rocksdb_js_storage_lease_v1);
	lease.status_size = sizeof(rocksdb_js_status_buffer);
	lease.capabilities = ROCKSDB_JS_STORAGE_CAP_GET_OWNED |
		ROCKSDB_JS_STORAGE_CAP_WRITE_BATCH |
		ROCKSDB_JS_STORAGE_CAP_SCAN_PAGE |
		ROCKSDB_JS_STORAGE_CAP_STATS;
	lease.provider_image_token = reinterpret_cast<uintptr_t>(&imageIdentity);
	lease.database_incarnation = context.state->databaseIncarnation;
	lease.column_family_incarnation = context.state->columnIncarnation;
	lease.rocksdb_major = ROCKSDB_MAJOR;
	lease.rocksdb_minor = ROCKSDB_MINOR;
	lease.rocksdb_patch = ROCKSDB_PATCH;
	lease.provider_build_identity = {
		reinterpret_cast<const uint8_t*>(kProviderBuildIdentity),
		sizeof(kProviderBuildIdentity) - 1
	};
	lease.context = &context;
	lease.retain = leaseRetain;
	lease.release = releaseContext;
	lease.poll_state = pollState;
	lease.get_owned = getOwned;
	lease.write_batch = writeBatch;
	lease.scan_page = scanPage;
	lease.collect_stats = collectStats;
}

} // namespace

napi_value Database::NativeStorageLease(napi_env env, napi_callback_info info) {
	NAPI_METHOD();
	UNWRAP_DB_HANDLE_AND_OPEN();

	auto descriptor = (*dbHandle)->descriptor;
	auto operation = descriptor->acquireOperation();
	if (!operation) {
		::napi_throw_error(env, nullptr, "Database is closing");
		return nullptr;
	}
	if (descriptor->readOnly) {
		::napi_throw_error(env, nullptr, "Native storage leases require a writable database");
		return nullptr;
	}
	if (descriptor->mode == DBMode::Pessimistic) {
		::napi_throw_error(env, nullptr, "Native storage leases do not support pessimistic databases");
		return nullptr;
	}
	auto column = (*dbHandle)->columnDescriptor;
	if (!column->registerNativeStorageLease()) {
		::napi_throw_error(env, nullptr, "Column family is dropped or uses the verification table");
		return nullptr;
	}

	std::shared_ptr<StorageLeaseState> state;
	LeaseContext* context = nullptr;
	try {
		state = std::make_shared<StorageLeaseState>(descriptor.get(), column);
		context = new LeaseContext(state);
		initializeLease(*context);
		if (!descriptor->attach(state)) throw DBException("Database is closing");
	} catch (const std::exception& exception) {
		if (context) {
			releaseContext(context);
		} else if (!state) {
			column->unregisterNativeStorageLease();
		}
		::napi_throw_error(env, nullptr, exception.what());
		return nullptr;
	}

	napi_value external;
	napi_status createStatus = ::napi_create_external(
		env,
		&context->lease,
		[](napi_env, void*, void* hint) { releaseContext(hint); },
		context,
		&external
	);
	if (createStatus != napi_ok) {
		releaseContext(context);
		NAPI_STATUS_THROWS(createStatus);
	}

	static const napi_type_tag typeTag = {
		ROCKSDB_JS_STORAGE_LEASE_TYPE_TAG_LOWER,
		ROCKSDB_JS_STORAGE_LEASE_TYPE_TAG_UPPER
	};
	NAPI_STATUS_THROWS(::napi_type_tag_object(env, external, &typeTag));
	return external;
}

} // namespace rocksdb_js

#endif
