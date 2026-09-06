#ifndef ROCKSDB_JS_NATIVE_STORAGE_LEASE_H
#define ROCKSDB_JS_NATIVE_STORAGE_LEASE_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define ROCKSDB_JS_STORAGE_LEASE_MAGIC UINT64_C(0x48524653544c5331)
#define ROCKSDB_JS_STORAGE_LEASE_ABI_MAJOR UINT32_C(1)
#define ROCKSDB_JS_STORAGE_LEASE_ABI_MINOR UINT32_C(0)
#define ROCKSDB_JS_STORAGE_LEASE_TYPE_TAG_LOWER UINT64_C(0x72f7ab4f52774465)
#define ROCKSDB_JS_STORAGE_LEASE_TYPE_TAG_UPPER UINT64_C(0xb654680e7e1e4db9)

#define ROCKSDB_JS_STORAGE_CAP_GET_OWNED (UINT64_C(1) << 0)
#define ROCKSDB_JS_STORAGE_CAP_WRITE_BATCH (UINT64_C(1) << 1)
#define ROCKSDB_JS_STORAGE_CAP_SCAN_PAGE (UINT64_C(1) << 2)
#define ROCKSDB_JS_STORAGE_CAP_STATS (UINT64_C(1) << 3)

typedef enum rocksdb_js_storage_status_code {
	ROCKSDB_JS_STORAGE_OK = 0,
	ROCKSDB_JS_STORAGE_NOT_FOUND = 1,
	ROCKSDB_JS_STORAGE_CLOSED = 2,
	ROCKSDB_JS_STORAGE_STALE_COLUMN_FAMILY = 3,
	ROCKSDB_JS_STORAGE_INVALID_ARGUMENT = 4,
	ROCKSDB_JS_STORAGE_LIMIT = 5,
	ROCKSDB_JS_STORAGE_BUSY = 6,
	ROCKSDB_JS_STORAGE_IO_ERROR = 7,
	ROCKSDB_JS_STORAGE_INTERNAL = 8,
	ROCKSDB_JS_STORAGE_UNSUPPORTED = 9
} rocksdb_js_storage_status_code;

typedef enum rocksdb_js_storage_state_code {
	ROCKSDB_JS_STORAGE_STATE_ACTIVE = 0,
	ROCKSDB_JS_STORAGE_STATE_CLOSING = 1,
	ROCKSDB_JS_STORAGE_STATE_REVOKED = 2
} rocksdb_js_storage_state_code;

typedef enum rocksdb_js_storage_mutation_kind {
	ROCKSDB_JS_STORAGE_PUT = 1,
	ROCKSDB_JS_STORAGE_DELETE = 2
} rocksdb_js_storage_mutation_kind;

typedef enum rocksdb_js_storage_write_policy {
	ROCKSDB_JS_STORAGE_WAL = 1,
	ROCKSDB_JS_STORAGE_WAL_SYNC = 2,
	ROCKSDB_JS_STORAGE_NO_WAL = 3
} rocksdb_js_storage_write_policy;

typedef struct rocksdb_js_byte_span {
	const uint8_t* data;
	uint64_t length;
} rocksdb_js_byte_span;

typedef struct rocksdb_js_status_buffer {
	uint32_t struct_size;
	uint32_t reserved;
	char* data;
	uint64_t capacity;
	uint64_t length;
} rocksdb_js_status_buffer;

typedef struct rocksdb_js_owned_bytes {
	uint32_t struct_size;
	uint32_t reserved;
	const uint8_t* data;
	uint64_t length;
	void* release_context;
	void (*release)(void* release_context);
} rocksdb_js_owned_bytes;

typedef struct rocksdb_js_storage_mutation {
	uint32_t struct_size;
	uint32_t kind;
	rocksdb_js_byte_span key;
	rocksdb_js_byte_span value;
} rocksdb_js_storage_mutation;

typedef struct rocksdb_js_storage_stats {
	uint32_t struct_size;
	uint32_t reserved;
	uint64_t get_operations;
	uint64_t scan_operations;
	uint64_t batch_operations;
	uint64_t requested_bytes;
	uint64_t returned_bytes;
	uint64_t copied_bytes;
	uint64_t live_owned_buffers;
	uint64_t provider_errors;
} rocksdb_js_storage_stats;

typedef struct rocksdb_js_storage_lease_v1 {
	uint64_t magic;
	uint32_t abi_major;
	uint32_t abi_minor;
	uint32_t struct_size;
	uint32_t status_size;
	uint64_t capabilities;
	uint64_t provider_image_token;
	uint64_t database_incarnation;
	uint64_t column_family_incarnation;
	uint32_t rocksdb_major;
	uint32_t rocksdb_minor;
	uint32_t rocksdb_patch;
	uint32_t reserved;
	rocksdb_js_byte_span provider_build_identity;
	void* context;

	uint32_t (*retain)(void* context, rocksdb_js_status_buffer* status);
	void (*release)(void* context);
	uint32_t (*poll_state)(void* context);
	uint32_t (*get_owned)(
		void* context,
		rocksdb_js_byte_span key,
		rocksdb_js_owned_bytes* result,
		rocksdb_js_status_buffer* status
	);
	uint32_t (*write_batch)(
		void* context,
		const rocksdb_js_storage_mutation* mutations,
		uint64_t mutation_count,
		uint32_t policy,
		rocksdb_js_status_buffer* status
	);
	uint32_t (*scan_page)(
		void* context,
		rocksdb_js_byte_span prefix,
		rocksdb_js_byte_span start_after,
		uint64_t entry_limit,
		uint64_t byte_limit,
		rocksdb_js_owned_bytes* page,
		rocksdb_js_status_buffer* status
	);
	uint32_t (*collect_stats)(
		void* context,
		rocksdb_js_storage_stats* result,
		rocksdb_js_status_buffer* status
	);
} rocksdb_js_storage_lease_v1;

#ifdef __cplusplus
}
#endif

#endif
