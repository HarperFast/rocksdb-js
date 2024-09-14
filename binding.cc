#define NAPI_VERSION 8

#include <assert.h>
#include <napi-macros.h>
#include <node_api.h>

#include <rocksdb/cache.h>
#include <rocksdb/comparator.h>
#include <rocksdb/convenience.h>
#include <rocksdb/utilities/transaction_db.h>
#include <rocksdb/db.h>
#include <rocksdb/env.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/merge_operator.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/status.h>
#include <rocksdb/table.h>
#include <rocksdb/write_batch.h>

#include <iostream>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "max_rev_operator.h"
#include "util.h"

class NullLogger : public rocksdb::Logger {
 public:
  using rocksdb::Logger::Logv;
  virtual void Logv(const char* format, va_list ap) override {}
  virtual size_t GetLogFileSize() const override { return 0; }
};

struct Database;
struct Iterator;

struct ColumnFamily {
  rocksdb::ColumnFamilyHandle* handle;
  rocksdb::ColumnFamilyDescriptor descriptor;
};

struct Closable {
  virtual ~Closable() {}
  virtual rocksdb::Status Close() = 0;
};

struct Database final {
  Database(std::string location) : location(std::move(location)) {}
  ~Database() { assert(!db); }

  rocksdb::Status Close() {
    if (!db) {
      return rocksdb::Status::OK();
    }

    std::set<Closable*> closables;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      closables = std::move(closables_);
    }

    for (auto closable : closables) {
      closable->Close();
    }

    db->FlushWAL(true);

    for (auto& [id, column] : columns) {
      db->DestroyColumnFamilyHandle(column.handle);
    }
    columns.clear();

    auto db2 = std::move(db);
    return db2->Close();
  }

  void Attach(Closable* closable) {
    std::lock_guard<std::mutex> lock(mutex_);

    closables_.insert(closable);
  }

  void Detach(Closable* closable) {
    std::lock_guard<std::mutex> lock(mutex_);

    closables_.erase(closable);
  }

  const std::string location;

  std::unique_ptr<rocksdb::TransactionDB> db;
  std::map<int32_t, ColumnFamily> columns;

 private:
  mutable std::mutex mutex_;
  std::set<Closable*> closables_;
};

enum BatchOp { Empty, Put, Delete, Merge, Data };

struct BatchEntry {
  BatchOp op = BatchOp::Empty;
  std::optional<std::string> key = std::nullopt;
  std::optional<std::string> val = std::nullopt;
  std::optional<ColumnFamily> column = std::nullopt;
};

struct BatchIterator : public rocksdb::WriteBatch::Handler {
  BatchIterator(Database* database,
                const bool keys,
                const bool values,
                const bool data,
                const rocksdb::ColumnFamilyHandle* column,
                const Encoding keyEncoding,
                const Encoding valueEncoding)
      : database_(database),
        keys_(keys),
        values_(values),
        data_(data),
        column_(column),
        keyEncoding_(keyEncoding),
        valueEncoding_(valueEncoding) {}

  napi_status Iterate(napi_env env, const rocksdb::WriteBatch& batch, napi_value* result) {
    cache_.reserve(batch.Count());

    ROCKS_STATUS_RETURN_NAPI(batch.Iterate(this));

    napi_value putStr;
    NAPI_STATUS_RETURN(napi_create_string_utf8(env, "put", NAPI_AUTO_LENGTH, &putStr));

    napi_value delStr;
    NAPI_STATUS_RETURN(napi_create_string_utf8(env, "del", NAPI_AUTO_LENGTH, &delStr));

    napi_value mergeStr;
    NAPI_STATUS_RETURN(napi_create_string_utf8(env, "merge", NAPI_AUTO_LENGTH, &mergeStr));

    napi_value dataStr;
    NAPI_STATUS_RETURN(napi_create_string_utf8(env, "data", NAPI_AUTO_LENGTH, &dataStr));

    napi_value nullVal;
    NAPI_STATUS_RETURN(napi_get_null(env, &nullVal));

    NAPI_STATUS_RETURN(napi_create_array_with_length(env, cache_.size() * 4, result));
    for (size_t n = 0; n < cache_.size(); ++n) {
      napi_value op;
      if (cache_[n].op == BatchOp::Put) {
        op = putStr;
      } else if (cache_[n].op == BatchOp::Delete) {
        op = delStr;
      } else if (cache_[n].op == BatchOp::Merge) {
        op = mergeStr;
      } else if (cache_[n].op == BatchOp::Data) {
        op = dataStr;
      } else {
        continue;
      }

      NAPI_STATUS_RETURN(napi_set_element(env, *result, n * 4 + 0, op));

      napi_value key;
      NAPI_STATUS_RETURN(Convert(env, cache_[n].key, keyEncoding_, key));
      NAPI_STATUS_RETURN(napi_set_element(env, *result, n * 4 + 1, key));

      napi_value val;
      NAPI_STATUS_RETURN(Convert(env, cache_[n].val, valueEncoding_, val));
      NAPI_STATUS_RETURN(napi_set_element(env, *result, n * 4 + 2, val));

      // TODO (fix)
      // napi_value column = cache_[n].column ? cache_[n].column->val : nullVal;
      NAPI_STATUS_RETURN(napi_set_element(env, *result, n * 4 + 3, nullVal));
    }

    cache_.clear();

    return napi_ok;
  }

  rocksdb::Status PutCF(uint32_t column_family_id, const rocksdb::Slice& key, const rocksdb::Slice& value) override {
    if (column_ && column_->GetID() != column_family_id) {
      return rocksdb::Status::OK();
    }

    BatchEntry entry = {BatchOp::Put};

    if (keys_) {
      entry.key = key.ToStringView();
    }

    if (values_) {
      entry.val = value.ToStringView();
    }

    // if (database_ && database_->columns.find(column_family_id) != database_->columns.end()) {
    //   entry.column = database_->columns[column_family_id];
    // }

    cache_.push_back(entry);

    return rocksdb::Status::OK();
  }

  rocksdb::Status DeleteCF(uint32_t column_family_id, const rocksdb::Slice& key) override {
    if (column_ && column_->GetID() != column_family_id) {
      return rocksdb::Status::OK();
    }

    BatchEntry entry = {BatchOp::Delete};

    if (keys_) {
      entry.key = key.ToStringView();
    }

    // if (database_ && database_->columns.find(column_family_id) != database_->columns.end()) {
    //   entry.column = database_->columns[column_family_id];
    // }

    cache_.push_back(entry);

    return rocksdb::Status::OK();
  }

  rocksdb::Status MergeCF(uint32_t column_family_id, const rocksdb::Slice& key, const rocksdb::Slice& value) override {
    if (column_ && column_->GetID() != column_family_id) {
      return rocksdb::Status::OK();
    }

    BatchEntry entry = {BatchOp::Merge};

    if (keys_) {
      entry.key = key.ToStringView();
    }

    if (values_) {
      entry.val = value.ToStringView();
    }

    // if (database_ && database_->columns.find(column_family_id) != database_->columns.end()) {
    //   entry.column = database_->columns[column_family_id];
    // }

    cache_.push_back(entry);

    return rocksdb::Status::OK();
  }

  void LogData(const rocksdb::Slice& data) override {
    if (!data_) {
      return;
    }

    BatchEntry entry = {BatchOp::Data};

    entry.val = data.ToStringView();

    cache_.push_back(entry);
  }

  bool Continue() override { return true; }

 private:
  Database* database_;
  const bool keys_;
  const bool values_;
  const bool data_;
  const rocksdb::ColumnFamilyHandle* column_;
  const Encoding keyEncoding_;
  const Encoding valueEncoding_;
  std::vector<BatchEntry> cache_;
};

struct BaseIterator : public Closable {
  BaseIterator(Database* database,
               rocksdb::ColumnFamilyHandle* column,
               const bool reverse,
               const std::optional<std::string>& lt,
               const std::optional<std::string>& lte,
               const std::optional<std::string>& gt,
               const std::optional<std::string>& gte,
               const int limit,
               const bool fillCache,
               std::shared_ptr<const rocksdb::Snapshot> snapshot,
               bool tailing = false)
      : database_(database),
        column_(column),
        snapshot_(snapshot),
        reverse_(reverse),
        limit_(limit),
        fillCache_(fillCache),
        tailing_(tailing) {
    if (lte) {
      upper_bound_ = rocksdb::PinnableSlice();
      *upper_bound_->GetSelf() = std::move(*lte) + '\0';
      upper_bound_->PinSelf();
    } else if (lt) {
      upper_bound_ = rocksdb::PinnableSlice();
      *upper_bound_->GetSelf() = std::move(*lt);
      upper_bound_->PinSelf();
    }

    if (gte) {
      lower_bound_ = rocksdb::PinnableSlice();
      *lower_bound_->GetSelf() = std::move(*gte);
      lower_bound_->PinSelf();
    } else if (gt) {
      lower_bound_ = rocksdb::PinnableSlice();
      *lower_bound_->GetSelf() = std::move(*gt) + '\0';
      lower_bound_->PinSelf();
    }
    database_->Attach(this);
  }

  virtual ~BaseIterator() { assert(!iterator_); }

  bool DidSeek() const { return iterator_ != nullptr; }

  void SeekToRange() {
    if (!iterator_) {
      Init();
    }

    if (reverse_) {
      iterator_->SeekToLast();
    } else {
      iterator_->SeekToFirst();
    }
  }

  virtual void Seek(const rocksdb::Slice& target) {
    if (!iterator_) {
      Init();
    }

    if ((upper_bound_ && target.compare(*upper_bound_) >= 0) || (lower_bound_ && target.compare(*lower_bound_) < 0)) {
      // TODO (fix): Why is this required? Seek should handle it?
      // https://github.com/facebook/rocksdb/issues/9904
      iterator_->SeekToLast();
      if (iterator_->Valid()) {
        iterator_->Next();
      }
    } else if (reverse_) {
      iterator_->SeekForPrev(target);
    } else {
      iterator_->Seek(target);
    }
  }

  virtual rocksdb::Status Close() override {
    snapshot_.reset();
    iterator_.reset();
    database_->Detach(this);
    return rocksdb::Status::OK();
  }

  bool Valid() const {
    assert(iterator_);
    return iterator_->Valid();
  }

  bool Increment() {
    assert(iterator_);
    return limit_ < 0 || ++count_ <= limit_;
  }

  void Next() {
    assert(iterator_);

    if (reverse_)
      iterator_->Prev();
    else
      iterator_->Next();
  }

  rocksdb::Slice CurrentKey() const {
    assert(iterator_);
    return iterator_->key();
  }

  rocksdb::Slice CurrentValue() const {
    assert(iterator_);
    return iterator_->value();
  }

  rocksdb::Status Status() const {
    assert(iterator_);
    return iterator_->status();
  }

  Database* database_;
  rocksdb::ColumnFamilyHandle* column_;
  std::shared_ptr<const rocksdb::Snapshot> snapshot_;

 private:
  void Init() {
    rocksdb::ReadOptions readOptions;
    if (upper_bound_) {
      readOptions.iterate_upper_bound = &*upper_bound_;
    }
    if (lower_bound_) {
      readOptions.iterate_lower_bound = &*lower_bound_;
    }
    readOptions.fill_cache = fillCache_;
    readOptions.snapshot = snapshot_.get();
    readOptions.async_io = true;
    readOptions.adaptive_readahead = true;
    readOptions.tailing = tailing_;

    iterator_.reset(database_->db->NewIterator(readOptions, column_));
  }

  int count_ = 0;
  std::optional<rocksdb::PinnableSlice> lower_bound_;
  std::optional<rocksdb::PinnableSlice> upper_bound_;
  std::unique_ptr<rocksdb::Iterator> iterator_;
  const bool reverse_;
  const int limit_;
  const bool fillCache_;
  const bool tailing_;
};

struct Iterator final : public BaseIterator {
  Iterator(Database* database,
           rocksdb::ColumnFamilyHandle* column,
           const bool reverse,
           const bool keys,
           const bool values,
           const int limit,
           const std::optional<std::string>& lt,
           const std::optional<std::string>& lte,
           const std::optional<std::string>& gt,
           const std::optional<std::string>& gte,
           const bool fillCache,
           const size_t highWaterMarkBytes,
           std::shared_ptr<const rocksdb::Snapshot> snapshot,
           bool tailing = false)
      : BaseIterator(database, column, reverse, lt, lte, gt, gte, limit, fillCache, snapshot, tailing),
        keys_(keys),
        values_(values),
        highWaterMarkBytes_(highWaterMarkBytes) {}

  void Seek(const rocksdb::Slice& target) override {
    first_ = true;
    return BaseIterator::Seek(target);
  }

  const bool keys_;
  const bool values_;
  const size_t highWaterMarkBytes_;
  bool first_ = true;
};

/**
 * Hook for when the environment exits. This hook will be called after
 * already-scheduled napi_async_work items have finished, which gives us
 * the guarantee that no db operations will be in-flight at this time.
 */
static void env_cleanup_hook(void* data) {
  auto database = reinterpret_cast<Database*>(data);

  // Do everything that db_close() does but synchronously. We're expecting that GC
  // did not (yet) collect the database because that would be a user mistake (not
  // closing their db) made during the lifetime of the environment. That's different
  // from an environment being torn down (like the main process or a worker thread)
  // where it's our responsibility to clean up. Note also, the following code must
  // be a safe noop if called before db_open() or after db_close().
  if (database) {
    database->Close();
  }
}

static void FinalizeDatabase(napi_env env, void* data, void* hint) {
  auto database = reinterpret_cast<Database*>(data);
  if (database) {
    napi_remove_env_cleanup_hook(env, env_cleanup_hook, database);
    database->Close();
  }
}

NAPI_METHOD(db_init) {
  NAPI_ARGV(1);

  Database* database = nullptr;

  napi_valuetype type;
  NAPI_STATUS_THROWS(napi_typeof(env, argv[0], &type));

  napi_value result;

  if (type == napi_string) {
    std::string location;
    size_t length = 0;
    NAPI_STATUS_THROWS(napi_get_value_string_utf8(env, argv[0], nullptr, 0, &length));
    location.resize(length, '\0');
    NAPI_STATUS_THROWS(napi_get_value_string_utf8(env, argv[0], &location[0], length + 1, &length));

    database = new Database(location);
    napi_add_env_cleanup_hook(env, env_cleanup_hook, database);
    NAPI_STATUS_THROWS(napi_create_external(env, database, FinalizeDatabase, nullptr, &result));
  } else if (type == napi_bigint) {
    int64_t value;
    bool lossless;
    NAPI_STATUS_THROWS(napi_get_value_bigint_int64(env, argv[0], &value, &lossless));

    database = reinterpret_cast<Database*>(value);
    NAPI_STATUS_THROWS(napi_create_external(env, database, nullptr, nullptr, &result));

    // We should have an env_cleanup_hook for closing iterators...
  } else {
    NAPI_STATUS_THROWS(napi_invalid_arg);
  }

  return result;
}

NAPI_METHOD(db_get_handle) {
  NAPI_ARGV(1);

  Database* database;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&database)));

  napi_value result;
  NAPI_STATUS_THROWS(napi_create_bigint_int64(env, reinterpret_cast<intptr_t>(database), &result));

  return result;
}

NAPI_METHOD(db_get_location) {
  NAPI_ARGV(1);

  Database* database;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&database)));

  napi_value result;
  NAPI_STATUS_THROWS(Convert(env, &database->location, Encoding::String, result));

  return result;
}

template <typename T, typename U>
napi_status InitOptions(napi_env env, T& columnOptions, const U& options) {
  rocksdb::ConfigOptions configOptions;

  uint64_t memtable_memory_budget = 256 * 1024 * 1024;
  NAPI_STATUS_RETURN(GetProperty(env, options, "memtableMemoryBudget", memtable_memory_budget));

  std::optional<std::string> compactionOpt;
  NAPI_STATUS_RETURN(GetProperty(env, options, "compaction", compactionOpt));
  if (compactionOpt) {
    if (*compactionOpt == "universal") {
      columnOptions.write_buffer_size = memtable_memory_budget / 4;
      // merge two memtables when flushing to L0
      columnOptions.min_write_buffer_number_to_merge = 2;
      // this means we'll use 50% extra memory in the worst case, but will reduce
      // write stalls.
      columnOptions.max_write_buffer_number = 6;
      // universal style compaction
      columnOptions.compaction_style = rocksdb::kCompactionStyleUniversal;
      columnOptions.compaction_options_universal.compression_size_percent = 80;
    } else if (*compactionOpt == "level") {
      // merge two memtables when flushing to L0
      columnOptions.min_write_buffer_number_to_merge = 2;
      // this means we'll use 50% extra memory in the worst case, but will reduce
      // write stalls.
      columnOptions.max_write_buffer_number = 6;
      // start flushing L0->L1 as soon as possible. each file on level0 is
      // (memtable_memory_budget / 2). This will flush level 0 when it's bigger than
      // memtable_memory_budget.
      columnOptions.level0_file_num_compaction_trigger = 2;
      // doesn't really matter much, but we don't want to create too many files
      columnOptions.target_file_size_base = memtable_memory_budget / 8;
      // make Level1 size equal to Level0 size, so that L0->L1 compactions are fast
      columnOptions.max_bytes_for_level_base = memtable_memory_budget;

      // level style compaction
      columnOptions.compaction_style = rocksdb::kCompactionStyleLevel;

      // only compress levels >= 2
      columnOptions.compression_per_level.resize(columnOptions.num_levels);
      for (int i = 0; i < columnOptions.num_levels; ++i) {
        if (i < 2) {
          columnOptions.compression_per_level[i] = rocksdb::kNoCompression;
        } else {
          columnOptions.compression_per_level[i] = rocksdb::kZSTD;
        }
      }
    } else {
      return napi_invalid_arg;
    }
    columnOptions.enable_blob_files = true;
    columnOptions.min_blob_size = 1000;
	columnOptions.enable_blob_garbage_collection = true;
  }

  bool compression = true;
  NAPI_STATUS_RETURN(GetProperty(env, options, "compression", compression));

  if (compression) {
    columnOptions.compression = rocksdb::kZSTD;
    columnOptions.compression_opts.max_dict_bytes = 16 * 1024;
    columnOptions.compression_opts.zstd_max_train_bytes = 16 * 1024 * 100;
    // TODO (perf): compression_opts.parallel_threads
  }

  std::optional<std::string> prefixExtractorOpt;
  NAPI_STATUS_RETURN(GetProperty(env, options, "prefixExtractor", prefixExtractorOpt));
  if (prefixExtractorOpt) {
    ROCKS_STATUS_RETURN_NAPI(
        rocksdb::SliceTransform::CreateFromString(configOptions, *prefixExtractorOpt, &columnOptions.prefix_extractor));
  }

  std::optional<std::string> comparatorOpt;
  NAPI_STATUS_RETURN(GetProperty(env, options, "comparator", comparatorOpt));
  if (comparatorOpt) {
    ROCKS_STATUS_RETURN_NAPI(
        rocksdb::Comparator::CreateFromString(configOptions, *comparatorOpt, &columnOptions.comparator));
  }

  std::optional<std::string> mergeOperatorOpt;
  NAPI_STATUS_RETURN(GetProperty(env, options, "mergeOperator", mergeOperatorOpt));
  if (mergeOperatorOpt) {
    if (*mergeOperatorOpt == "maxRev") {
      columnOptions.merge_operator = std::make_shared<MaxRevOperator>();
    } else {
      ROCKS_STATUS_RETURN_NAPI(
          rocksdb::MergeOperator::CreateFromString(configOptions, *mergeOperatorOpt, &columnOptions.merge_operator));
    }
  }

  std::optional<std::string> compactionPriority;
  NAPI_STATUS_RETURN(GetProperty(env, options, "compactionPriority", compactionPriority));
  if (compactionPriority) {
    if (compactionPriority == "byCompensatedSize") {
      columnOptions.compaction_pri = rocksdb::kByCompensatedSize;
    } else if (compactionPriority == "oldestLargestSeqFirst") {
      columnOptions.compaction_pri = rocksdb::kOldestLargestSeqFirst;
    } else if (compactionPriority == "smallestSeqFirst") {
      columnOptions.compaction_pri = rocksdb::kOldestSmallestSeqFirst;
    } else if (compactionPriority == "overlappingRatio") {
      columnOptions.compaction_pri = rocksdb::kMinOverlappingRatio;
    } else if (compactionPriority == "roundRobin") {
      columnOptions.compaction_pri = rocksdb::kRoundRobin;
    } else {
      // Throw?
    }
  }

  uint32_t cacheSize = 8 << 20;
  NAPI_STATUS_RETURN(GetProperty(env, options, "cacheSize", cacheSize));

  rocksdb::BlockBasedTableOptions tableOptions;

  if (cacheSize) {
    tableOptions.block_cache = rocksdb::HyperClockCacheOptions(cacheSize, 0).MakeSharedCache();
    NAPI_STATUS_RETURN(
        GetProperty(env, options, "cacheIndexAndFilterBlocks", tableOptions.cache_index_and_filter_blocks));
  } else {
    tableOptions.no_block_cache = true;
    tableOptions.cache_index_and_filter_blocks = false;
  }

  std::string optimize = "";
  NAPI_STATUS_RETURN(GetProperty(env, options, "optimize", optimize));

  if (optimize == "point-lookup") {
    tableOptions.data_block_index_type = rocksdb::BlockBasedTableOptions::kDataBlockBinaryAndHash;
    tableOptions.data_block_hash_table_util_ratio = 0.75;
    tableOptions.filter_policy.reset(rocksdb::NewRibbonFilterPolicy(10, 1));

    columnOptions.memtable_prefix_bloom_size_ratio = 0.02;
    columnOptions.memtable_whole_key_filtering = true;
  } else if (optimize == "range-lookup") {
    // TODO?
  } else {
    tableOptions.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10));
  }

  std::optional<std::string> filterPolicyOpt;
  NAPI_STATUS_RETURN(GetProperty(env, options, "filterPolicy", filterPolicyOpt));
  if (filterPolicyOpt) {
    ROCKS_STATUS_RETURN_NAPI(
        rocksdb::FilterPolicy::CreateFromString(configOptions, *filterPolicyOpt, &tableOptions.filter_policy));
  }

  NAPI_STATUS_RETURN(GetProperty(env, options, "blockSize", tableOptions.block_size));

  NAPI_STATUS_RETURN(GetProperty(env, options, "blockRestartInterval", tableOptions.block_restart_interval));

  tableOptions.format_version = 5;
  tableOptions.checksum = rocksdb::kXXH3;

  tableOptions.optimize_filters_for_memory = true;
  NAPI_STATUS_RETURN(GetProperty(env, options, "optimizeFiltersForMemory", tableOptions.optimize_filters_for_memory));

  columnOptions.table_factory.reset(rocksdb::NewBlockBasedTableFactory(tableOptions));

  return napi_ok;
}

NAPI_METHOD(db_get_merge_operands) {
  NAPI_ARGV(4);

  Database* database;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&database)));

  std::string key;
  NAPI_STATUS_THROWS(GetValue(env, argv[1], key));

  const auto options = argv[2];

  Encoding valueEncoding = Encoding::String;
  NAPI_STATUS_THROWS(GetProperty(env, options, "valueEncoding", valueEncoding));

  rocksdb::ColumnFamilyHandle* column = database->db->DefaultColumnFamily();
  NAPI_STATUS_THROWS(GetProperty(env, options, "column", column));

  auto callback = argv[3];

  runAsync<std::vector<rocksdb::PinnableSlice>>(
      "leveldown.get.mergeOperands", env, callback,
      [=, key = std::move(key)](auto& values) {
        rocksdb::ReadOptions readOptions;

        values.resize(16);  // TODO (fix): Make option

        rocksdb::GetMergeOperandsOptions mergeOperandsOptions;
        mergeOperandsOptions.expected_max_number_of_operands = values.size();

        int size = 0;
        const auto status =
            database->db->GetMergeOperands(readOptions, column, key, values.data(), &mergeOperandsOptions, &size);

        values.resize(size);

        return status;
      },
      [=](auto& values, auto env, auto& argv) {
        argv.resize(2);

        NAPI_STATUS_RETURN(napi_create_array_with_length(env, values.size(), &argv[1]));

        for (size_t idx = 0; idx < values.size(); idx++) {
          napi_value element;
          if (values[idx].GetSelf()) {
            NAPI_STATUS_RETURN(Convert(env, &values[idx], valueEncoding, element));
          } else {
            NAPI_STATUS_RETURN(napi_get_undefined(env, &element));
          }
          NAPI_STATUS_RETURN(napi_set_element(env, argv[1], static_cast<uint32_t>(idx), element));
        }

        return napi_ok;
      });

  return 0;
}

NAPI_METHOD(db_get_identity) {
  NAPI_ARGV(1);

  Database* database;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&database)));

  std::string identity;
  ROCKS_STATUS_THROWS_NAPI(database->db->GetDbIdentity(identity));

  napi_value result;
  NAPI_STATUS_THROWS(Convert(env, &identity, Encoding::String, result));

  return result;
}

NAPI_METHOD(db_open) {
  NAPI_ARGV(3);

  Database* database;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&database)));

  if (database->db) {
    napi_value columns;
    NAPI_STATUS_THROWS(napi_create_object(env, &columns));
    for (auto& [id, column] : database->columns) {
      napi_value val;
      NAPI_STATUS_THROWS(napi_create_external(env, column.handle, nullptr, nullptr, &val));
      NAPI_STATUS_THROWS(napi_set_named_property(env, columns, column.descriptor.name.c_str(), val));
    }
    return columns;
  } else {
    rocksdb::Options dbOptions;

    const auto options = argv[1];

    int parallelism = std::max<int>(1, std::thread::hardware_concurrency() / 2);
    NAPI_STATUS_THROWS(GetProperty(env, options, "parallelism", parallelism));
    dbOptions.IncreaseParallelism(parallelism);
	dbOptions.OptimizeLevelStyleCompaction();
	//dbOptions.allow_mmap_reads = true; // slower
	dbOptions.use_direct_reads = true;
	dbOptions.use_direct_io_for_flush_and_compaction = true;

    uint32_t walTTL = 0;
    NAPI_STATUS_THROWS(GetProperty(env, options, "walTTL", walTTL));
    dbOptions.WAL_ttl_seconds = walTTL / 1e3;

    uint32_t walSizeLimit = 0;
    NAPI_STATUS_THROWS(GetProperty(env, options, "walSizeLimit", walSizeLimit));
    dbOptions.WAL_size_limit_MB = walSizeLimit / 1e6;

    uint32_t maxTotalWalSize = 0;
    NAPI_STATUS_THROWS(GetProperty(env, options, "walTotalSizeLimit", walSizeLimit));
    dbOptions.max_total_wal_size = maxTotalWalSize / 1e6;

    bool walCompression = false;
    NAPI_STATUS_THROWS(GetProperty(env, options, "walCompression", walCompression));
    dbOptions.wal_compression =
        walCompression ? rocksdb::CompressionType::kZSTD : rocksdb::CompressionType::kNoCompression;

    dbOptions.avoid_unnecessary_blocking_io = true;
    dbOptions.write_dbid_to_manifest = true;
    dbOptions.create_missing_column_families = true;
    dbOptions.fail_if_options_file_error = true;
	dbOptions.listeners.push_back(std::shared_ptr(std::make_shared<rocksdb::EventListener>()));

    NAPI_STATUS_THROWS(GetProperty(env, options, "createIfMissing", dbOptions.create_if_missing));
    NAPI_STATUS_THROWS(GetProperty(env, options, "errorIfExists", dbOptions.error_if_exists));
    NAPI_STATUS_THROWS(GetProperty(env, options, "pipelinedWrite", dbOptions.enable_pipelined_write));

    // TODO (feat): dbOptions.listeners

    std::string infoLogLevel;
    NAPI_STATUS_THROWS(GetProperty(env, options, "infoLogLevel", infoLogLevel));
    if (infoLogLevel.size() > 0) {
      rocksdb::InfoLogLevel lvl = {};

      if (infoLogLevel == "debug")
        lvl = rocksdb::InfoLogLevel::DEBUG_LEVEL;
      else if (infoLogLevel == "info")
        lvl = rocksdb::InfoLogLevel::INFO_LEVEL;
      else if (infoLogLevel == "warn")
        lvl = rocksdb::InfoLogLevel::WARN_LEVEL;
      else if (infoLogLevel == "error")
        lvl = rocksdb::InfoLogLevel::ERROR_LEVEL;
      else if (infoLogLevel == "fatal")
        lvl = rocksdb::InfoLogLevel::FATAL_LEVEL;
      else if (infoLogLevel == "header")
        lvl = rocksdb::InfoLogLevel::HEADER_LEVEL;
      else
        napi_throw_error(env, nullptr, "invalid log level");

      dbOptions.info_log_level = lvl;
    } else {
      // In some places RocksDB checks this option to see if it should prepare
      // debug information (ahead of logging), so set it to the highest level.
      dbOptions.info_log_level = rocksdb::InfoLogLevel::HEADER_LEVEL;
      dbOptions.info_log.reset(new NullLogger());
    }

    NAPI_STATUS_THROWS(InitOptions(env, dbOptions, options));

    std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;

    bool hasColumns;
    NAPI_STATUS_THROWS(napi_has_named_property(env, options, "columns", &hasColumns));

    if (hasColumns) {
      napi_value columns;
      NAPI_STATUS_THROWS(napi_get_named_property(env, options, "columns", &columns));

      napi_value keys;
      NAPI_STATUS_THROWS(napi_get_property_names(env, columns, &keys));

      uint32_t len;
      NAPI_STATUS_THROWS(napi_get_array_length(env, keys, &len));

      descriptors.resize(len);
      for (uint32_t n = 0; n < len; ++n) {
        napi_value key;
        NAPI_STATUS_THROWS(napi_get_element(env, keys, n, &key));

        napi_value column;
        NAPI_STATUS_THROWS(napi_get_property(env, columns, key, &column));

        NAPI_STATUS_THROWS(InitOptions(env, descriptors[n].options, column));

        NAPI_STATUS_THROWS(GetValue(env, key, descriptors[n].name));
      }
    }

    auto callback = argv[2];

    std::vector<rocksdb::ColumnFamilyHandle*> handles;
          assert(!database->db);

          rocksdb::TransactionDB* db = nullptr;
			rocksdb::TransactionDBOptions txndb_options;
          const auto status = descriptors.empty()
                                  ? rocksdb::TransactionDB::Open(dbOptions, txndb_options, database->location, &db)
                                  : rocksdb::TransactionDB::Open(dbOptions, txndb_options, database->location, descriptors, &handles, &db);

          database->db.reset(db);

          //return status;
          //argv.resize(2);



          for (size_t n = 0; n < handles.size(); ++n) {
            ColumnFamily column;
            column.handle = handles[n];
            column.descriptor = descriptors[n];
            database->columns[column.handle->GetID()] = column;
          }

          napi_value columns;
			NAPI_STATUS_THROWS(napi_create_object(env, &columns));
          for (auto& [id, column] : database->columns) {
            napi_value val;
			  NAPI_STATUS_THROWS(napi_create_external(env, column.handle, nullptr, nullptr, &val));
			  NAPI_STATUS_THROWS(napi_set_named_property(env, columns, column.descriptor.name.c_str(), val));
          }

          return 0;
  }

  return 0;
}

napi_value noop_callback(napi_env env, napi_callback_info info) {
  return 0;
}

NAPI_METHOD(db_close) {
  NAPI_ARGV(2);

  Database* database;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&database)));

  auto callback = argv[1];

  struct State {};
  runAsync<State>(
      "leveldown.close", env, callback, [=](auto& state) { return database->Close(); },
      [](auto& state, auto env, auto& argv) { return napi_ok; });

  return 0;
}

NAPI_METHOD(db_get_many) {
  NAPI_ARGV(4);

  Database* database;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&database)));

  uint32_t count;
  NAPI_STATUS_THROWS(napi_get_array_length(env, argv[1], &count));

  const auto options = argv[2];

  bool fillCache = true;
  NAPI_STATUS_THROWS(GetProperty(env, options, "fillCache", fillCache));

  bool ignoreRangeDeletions = false;
  NAPI_STATUS_THROWS(GetProperty(env, options, "ignoreRangeDeletions", ignoreRangeDeletions));

  rocksdb::ColumnFamilyHandle* column = database->db->DefaultColumnFamily();
  NAPI_STATUS_THROWS(GetProperty(env, options, "column", column));

  bool takeSnapshot = true;
  NAPI_STATUS_THROWS(GetProperty(env, options, "snapshot", takeSnapshot));

  auto callback = argv[3];

  std::shared_ptr<const rocksdb::Snapshot> snapshot;
  if (takeSnapshot) {
    snapshot.reset(database->db->GetSnapshot(), [=](const auto ptr) { database->db->ReleaseSnapshot(ptr); });
  }

  std::vector<rocksdb::PinnableSlice> keys{count};

  for (uint32_t n = 0; n < count; n++) {
    napi_value element;
    NAPI_STATUS_THROWS(napi_get_element(env, argv[1], n, &element));
    NAPI_STATUS_THROWS(GetValue(env, element, keys[n]));
  }

  struct State {
    std::vector<uint8_t> data;
    std::vector<int32_t> sizes;
  };

  runAsync<State>(
      "leveldown.get.many", env, callback,
      [=, keys = std::move(keys), snapshot = std::move(snapshot)](auto& state) {
        rocksdb::ReadOptions readOptions;
        readOptions.fill_cache = fillCache;
        readOptions.snapshot = snapshot.get();
        readOptions.async_io = true;
        readOptions.ignore_range_deletions = ignoreRangeDeletions;
        readOptions.optimize_multiget_for_io = true;

        std::vector<rocksdb::Status> statuses{count};
        std::vector<rocksdb::PinnableSlice> values{count};
        std::vector<rocksdb::Slice> keys2{count};

        for (auto n = 0; n < count; n++) {
          keys2[n] = keys[n];
        }

        database->db->MultiGet(readOptions, column, count, keys2.data(), values.data(), statuses.data());

        auto size = 0;
        for (auto n = 0; n < count; n++) {
          const auto valueSize = values[n].size();
          size += valueSize & 0x7 ? (valueSize | 0x7) + 1 : valueSize;
        }

        state.data.reserve(size);

        auto push = [&](rocksdb::Slice* slice){
          if (slice) {
            state.sizes.push_back(static_cast<int32_t>(slice->size()));
            std::copy_n(slice->data(), slice->size(), std::back_inserter(state.data));

            if (state.data.size() & 0x7) {
              state.data.resize((state.data.size() | 0x7) + 1);
            }
          } else {
            state.sizes.push_back(-1);
          }
        };

        for (auto n = 0; n < count; n++) {
          push(statuses[n].ok() ? &values[n] : nullptr);
        }

        return rocksdb::Status::OK();
      },
      [=](auto& state, auto env, auto& argv) {
        argv.resize(3);

        if (state.sizes.size() > 0) {
          auto sizes = std::make_unique<std::vector<int32_t>>(std::move(state.sizes));
          NAPI_STATUS_RETURN(napi_create_external_buffer(env, sizes->size() * 4, sizes->data(), Finalize<std::vector<int32_t>>, sizes.get(), &argv[1]));
          sizes.release();
        } else {
          NAPI_STATUS_RETURN(napi_get_undefined(env, &argv[1]));
        }

        if (state.data.size() > 0) {
          auto data = std::make_unique<std::vector<uint8_t>>(std::move(state.data));
          NAPI_STATUS_RETURN(napi_create_external_buffer(env, data->size(), data->data(), Finalize<std::vector<uint8_t>>, data.get(), &argv[2]));
          data.release();
        } else {
          NAPI_STATUS_RETURN(napi_get_undefined(env, &argv[2]));
        }

        return napi_ok;
      });

  return 0;
}

NAPI_METHOD(db_clear) {
  NAPI_ARGV(2);

  Database* database;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&database)));

  const auto options = argv[1];

  bool reverse = false;
  NAPI_STATUS_THROWS(GetProperty(env, options, "reverse", reverse));

  int32_t limit = -1;
  NAPI_STATUS_THROWS(GetProperty(env, options, "limit", limit));

  rocksdb::ColumnFamilyHandle* column = database->db->DefaultColumnFamily();
  NAPI_STATUS_THROWS(GetProperty(env, options, "column", column));

  std::optional<std::string> lt;
  NAPI_STATUS_THROWS(GetProperty(env, options, "lt", lt));

  std::optional<std::string> lte;
  NAPI_STATUS_THROWS(GetProperty(env, options, "lte", lte));

  std::optional<std::string> gt;
  NAPI_STATUS_THROWS(GetProperty(env, options, "gt", gt));

  std::optional<std::string> gte;
  NAPI_STATUS_THROWS(GetProperty(env, options, "gte", gte));

  if (limit == -1) {
    rocksdb::PinnableSlice begin;
    if (gte) {
      *begin.GetSelf() = std::move(*gte);
    } else if (gt) {
      *begin.GetSelf() = std::move(*gt) + '\0';
    }
    begin.PinSelf();

    rocksdb::PinnableSlice end;
    if (lte) {
      *end.GetSelf() = std::move(*lte) + '\0';
    } else if (lt) {
      *end.GetSelf() = std::move(*lt);
    } else {
      // HACK: Assume no key that starts with 0xFF is larger than 1MiB.
      end.GetSelf()->resize(1e6);
      memset(end.GetSelf()->data(), 255, end.GetSelf()->size());
    }
    end.PinSelf();

    if (begin.compare(end) < 0) {
      rocksdb::WriteOptions writeOptions;
      ROCKS_STATUS_THROWS_NAPI(database->db->DeleteRange(writeOptions, column, begin, end));
    }

    return 0;
  } else {
    // TODO (fix): Error handling.
    // TODO (fix): This should be async...

    std::shared_ptr<const rocksdb::Snapshot> snapshot(database->db->GetSnapshot(),
                                                      [=](const auto ptr) { database->db->ReleaseSnapshot(ptr); });
    BaseIterator it(database, column, reverse, lt, lte, gt, gte, limit, false, snapshot);

    it.SeekToRange();

    rocksdb::WriteBatch batch;
    rocksdb::WriteOptions writeOptions;
    rocksdb::Status status;

    while (true) {
      size_t bytesRead = 0;

      while (bytesRead <= 16 * 1024 && it.Valid() && it.Increment()) {
        const auto key = it.CurrentKey();
        batch.Delete(column, key);
        bytesRead += key.size();
        it.Next();
      }

      status = it.Status();
      if (!status.ok() || bytesRead == 0) {
        break;
      }

      status = database->db->Write(writeOptions, &batch);
      if (!status.ok()) {
        break;
      }

      batch.Clear();
    }

    it.Close();

    if (!status.ok()) {
      ROCKS_STATUS_THROWS_NAPI(status);
    }

    return 0;
  }
}

NAPI_METHOD(db_get_property) {
  NAPI_ARGV(2);

  Database* database;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&database)));

  rocksdb::PinnableSlice property;
  NAPI_STATUS_THROWS(GetValue(env, argv[1], property));

  std::string value;
  database->db->GetProperty(property, &value);

  napi_value result;
  NAPI_STATUS_THROWS(napi_create_string_utf8(env, value.data(), value.size(), &result));

  return result;
}

NAPI_METHOD(db_get_latest_sequence) {
  NAPI_ARGV(1);

  Database* database;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&database)));

  const auto seq = database->db->GetLatestSequenceNumber();

  napi_value result;
  NAPI_STATUS_THROWS(napi_create_int64(env, seq, &result));

  return result;
}

NAPI_METHOD(db_put) {
		NAPI_ARGV(4);

		Database* database;
		NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&database)));

		rocksdb::Slice key;
		NAPI_STATUS_THROWS(GetValue(env, argv[1], key));

		rocksdb::Slice val;
		NAPI_STATUS_THROWS(GetValue(env, argv[2], val));

		const auto options = argv[3];

		rocksdb::ColumnFamilyHandle* column = nullptr;
		NAPI_STATUS_THROWS(GetProperty(env, options, "column", column));
		rocksdb::WriteOptions writeOptions;
		writeOptions.disableWAL = true;
		writeOptions.sync = false;
		rocksdb::TransactionOptions txn_options;
		rocksdb::Transaction* txn = database->db->BeginTransaction(writeOptions, txn_options);
		if (column) {
			ROCKS_STATUS_THROWS_NAPI(txn->Put(column, key, val));
		} else {
			ROCKS_STATUS_THROWS_NAPI(txn->Put(key, val));
		}
		ROCKS_STATUS_THROWS_NAPI(txn->Commit());
		delete txn;

		return 0;
}
NAPI_METHOD(db_get) {
		NAPI_ARGV(4);

		Database* database;
		NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&database)));

		rocksdb::Slice key;
		NAPI_STATUS_THROWS(GetValue(env, argv[1], key));

		std::string val;

		const auto options = argv[3];

		rocksdb::ColumnFamilyHandle* column = nullptr;
		//NAPI_STATUS_THROWS(GetProperty(env, options, "column", column));
		rocksdb::ReadOptions read_options;
		read_options.read_tier = rocksdb::kBlockCacheTier;
		if (column) {
			ROCKS_STATUS_THROWS_NAPI(database->db->Get(read_options, column, key, &val));
		} else {
			rocksdb::Status status = database->db->Get(read_options, key, &val);
			if (!status.ok()) {
				napi_value return_status;
				if (status.IsNotFound()) {
					napi_create_int32(env, 1, &return_status);
					return return_status;
				}
				if (status.IsIncomplete()) {
					read_options.read_tier = rocksdb::kReadAllTier;
					status = database->db->Get(read_options, key, &val);
					napi_create_int32(env, 2, &return_status);
					return return_status;
				}
				ROCKS_STATUS_THROWS_NAPI(status);
			}
		}

		return 0;
}


NAPI_METHOD(iterator_init) {
  NAPI_ARGV(2);

  Database* database;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&database)));

  const auto options = argv[1];

  bool reverse = false;
  NAPI_STATUS_THROWS(GetProperty(env, options, "reverse", reverse));

  bool keys = true;
  NAPI_STATUS_THROWS(GetProperty(env, options, "keys", keys));

  bool values = true;
  NAPI_STATUS_THROWS(GetProperty(env, options, "values", values));

  bool tailing = false;
  NAPI_STATUS_THROWS(GetProperty(env, options, "tailing", tailing));

  bool fillCache = false;
  NAPI_STATUS_THROWS(GetProperty(env, options, "fillCache", fillCache));

  int32_t limit = -1;
  NAPI_STATUS_THROWS(GetProperty(env, options, "limit", limit));

  int32_t highWaterMarkBytes = 64 * 1024;
  NAPI_STATUS_THROWS(GetProperty(env, options, "highWaterMarkBytes", highWaterMarkBytes));

  std::optional<std::string> lt;
  NAPI_STATUS_THROWS(GetProperty(env, options, "lt", lt));

  std::optional<std::string> lte;
  NAPI_STATUS_THROWS(GetProperty(env, options, "lte", lte));

  std::optional<std::string> gt;
  NAPI_STATUS_THROWS(GetProperty(env, options, "gt", gt));

  std::optional<std::string> gte;
  NAPI_STATUS_THROWS(GetProperty(env, options, "gte", gte));

  rocksdb::ColumnFamilyHandle* column = database->db->DefaultColumnFamily();
  NAPI_STATUS_THROWS(GetProperty(env, options, "column", column));

  bool takeSnapshot = !tailing;
  NAPI_STATUS_THROWS(GetProperty(env, options, "snapshot", takeSnapshot));

  std::shared_ptr<const rocksdb::Snapshot> snapshot;
  if (takeSnapshot) {
    snapshot.reset(database->db->GetSnapshot(), [=](const auto ptr) { database->db->ReleaseSnapshot(ptr); });
  }

  auto iterator = std::unique_ptr<Iterator>(new Iterator(database, column, reverse, keys, values, limit, lt, lte, gt,
                                                         gte, fillCache, highWaterMarkBytes,
                                                         snapshot, tailing));

  napi_value result;
  NAPI_STATUS_THROWS(napi_create_external(env, iterator.get(), Finalize<Iterator>, iterator.get(), &result));
  iterator.release();

  return result;
}

NAPI_METHOD(iterator_seek) {
  NAPI_ARGV(2);

  Iterator* iterator;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&iterator)));

  rocksdb::PinnableSlice target;
  NAPI_STATUS_THROWS(GetValue(env, argv[1], target));

  iterator->Seek(target);

  return 0;
}

NAPI_METHOD(iterator_close) {
  NAPI_ARGV(1);

  Iterator* iterator;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&iterator)));

  ROCKS_STATUS_THROWS_NAPI(iterator->Close());

  return 0;
}

NAPI_METHOD(iterator_nextv) {
  NAPI_ARGV(3);

  Iterator* iterator;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&iterator)));

  uint32_t count;
  NAPI_STATUS_THROWS(napi_get_value_uint32(env, argv[1], &count));

  auto callback = argv[2];

  struct State {
    std::vector<uint8_t> data;
    std::vector<int32_t> sizes;
    bool finished = false;
  };

  runAsync<State>(
      std::string("leveldown.iterator.next"), env, callback,
      [=](auto& state) {
        if (!iterator->DidSeek()) {
          iterator->SeekToRange();
        }

        state.sizes.reserve(count * 2);
        state.data.reserve(iterator->highWaterMarkBytes_);

        auto bytesRead = 0;

        auto push = [&](const std::optional<rocksdb::Slice>& slice){
          if (slice) {
            state.sizes.push_back(static_cast<int32_t>(slice->size()));
            std::copy_n(slice->data(), slice->size(), std::back_inserter(state.data));

            if (state.data.size() & 0x7) {
              state.data.resize((state.data.size() | 0x7) + 1);
            }

            bytesRead += slice->size();
          } else {
            state.sizes.push_back(-1);
          }
        };

        while (true) {
          if (!iterator->first_) {
            iterator->Next();
          } else {
            iterator->first_ = false;
          }

          if (!iterator->Valid() || !iterator->Increment()) {
            state.finished = true;
            return iterator->Status();
          }

          if (iterator->keys_ && iterator->values_) {
            push(iterator->CurrentKey());
            push(iterator->CurrentValue());
          } else if (iterator->keys_) {
            push(iterator->CurrentKey());
            push(std::nullopt);
          } else if (iterator->values_) {
            push(std::nullopt);
            push(iterator->CurrentValue());
          }

          if (bytesRead > iterator->highWaterMarkBytes_ || state.sizes.size() / 2 >= count) {
            state.finished = false;
            return rocksdb::Status::OK();
          }
        }
      },
      [=](auto& state, auto env, auto& argv) {
        argv.resize(4);

        if (state.sizes.size() > 0) {
          auto sizes = std::make_unique<std::vector<int32_t>>(std::move(state.sizes));
          NAPI_STATUS_RETURN(napi_create_external_buffer(env, sizes->size() * 4, sizes->data(), Finalize<std::vector<int32_t>>, sizes.get(), &argv[1]));
          sizes.release();
        } else {
          NAPI_STATUS_RETURN(napi_get_undefined(env, &argv[1]));
        }

        if (state.data.size() > 0) {
          auto data = std::make_unique<std::vector<uint8_t>>(std::move(state.data));
          NAPI_STATUS_RETURN(napi_create_external_buffer(env, data->size(), data->data(), Finalize<std::vector<uint8_t>>, data.get(), &argv[2]));
          data.release();
        } else {
          NAPI_STATUS_RETURN(napi_get_undefined(env, &argv[2]));
        }

        NAPI_STATUS_RETURN(napi_get_boolean(env, state.finished, &argv[3]));

        return napi_ok;
      });

  return 0;
}

NAPI_METHOD(batch_init) {
  auto batch = new rocksdb::WriteBatch();

  napi_value result;
  NAPI_STATUS_THROWS(napi_create_external(env, batch, Finalize<rocksdb::WriteBatch>, batch, &result));

  return result;
}

NAPI_METHOD(batch_put) {
  NAPI_ARGV(4);

  rocksdb::WriteBatch* batch;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&batch)));

  rocksdb::Slice key;
  NAPI_STATUS_THROWS(GetValue(env, argv[1], key));

  rocksdb::Slice val;
  NAPI_STATUS_THROWS(GetValue(env, argv[2], val));

  const auto options = argv[3];

  rocksdb::ColumnFamilyHandle* column = nullptr;
  NAPI_STATUS_THROWS(GetProperty(env, options, "column", column));

  if (column) {
    ROCKS_STATUS_THROWS_NAPI(batch->Put(column, key, val));
  } else {
    ROCKS_STATUS_THROWS_NAPI(batch->Put(key, val));
  }

  return 0;
}

NAPI_METHOD(batch_del) {
  NAPI_ARGV(3);

  rocksdb::WriteBatch* batch;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&batch)));

  rocksdb::Slice key;
  NAPI_STATUS_THROWS(GetValue(env, argv[1], key));

  const auto options = argv[2];

  rocksdb::ColumnFamilyHandle* column = nullptr;
  NAPI_STATUS_THROWS(GetProperty(env, options, "column", column));

  if (column) {
    ROCKS_STATUS_THROWS_NAPI(batch->Delete(column, key));
  } else {
    ROCKS_STATUS_THROWS_NAPI(batch->Delete(key));
  }

  return 0;
}

NAPI_METHOD(batch_merge) {
  NAPI_ARGV(4);

  rocksdb::WriteBatch* batch;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&batch)));

  rocksdb::Slice key;
  NAPI_STATUS_THROWS(GetValue(env, argv[1], key));

  rocksdb::Slice val;
  NAPI_STATUS_THROWS(GetValue(env, argv[2], val));

  const auto options = argv[3];

  rocksdb::ColumnFamilyHandle* column = nullptr;
  NAPI_STATUS_THROWS(GetProperty(env, options, "column", column));

  if (column) {
    ROCKS_STATUS_THROWS_NAPI(batch->Merge(column, key, val));
  } else {
    ROCKS_STATUS_THROWS_NAPI(batch->Merge(key, val));
  }

  return 0;
}

NAPI_METHOD(batch_clear) {
  NAPI_ARGV(1);

  rocksdb::WriteBatch* batch;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&batch)));

  batch->Clear();

  return 0;
}

NAPI_METHOD(batch_write) {
  NAPI_ARGV(4);

  napi_value result = 0;

  Database* database;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&database)));

  rocksdb::WriteBatch* batch;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[1], reinterpret_cast<void**>(&batch)));

  auto options = argv[2];
  auto callback = argv[3];

  bool sync = false;
  NAPI_STATUS_THROWS(GetProperty(env, options, "sync", sync));

  bool lowPriority = false;
  NAPI_STATUS_THROWS(GetProperty(env, options, "lowPriority", lowPriority));

  /*runAsync<int64_t>(
      "leveldown.batch.write", env, callback,
      [=](int64_t& seq) {
        rocksdb::WriteOptions writeOptions;
        writeOptions.sync = sync;
        writeOptions.low_pri = lowPriority;
        return database->db->Write(writeOptions, batch);
      },
      [=](int64_t& seq, auto env, auto& argv) { return napi_ok; });*/
	rocksdb::WriteOptions writeOptions;
	writeOptions.sync = sync;
	writeOptions.disableWAL = true;
	writeOptions.low_pri = lowPriority;
	rocksdb::Status status = database->db->Write(writeOptions, batch);
	napi_get_boolean(env, status.ok(), &result);
  return result;
}

NAPI_METHOD(batch_count) {
  NAPI_ARGV(1);

  rocksdb::WriteBatch* batch;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&batch)));

  napi_value result;
  NAPI_STATUS_THROWS(napi_create_int64(env, batch->Count(), &result));

  return result;
}

NAPI_METHOD(batch_iterate) {
  NAPI_ARGV(3);

  Database* database;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], reinterpret_cast<void**>(&database)));

  rocksdb::WriteBatch* batch;
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[1], reinterpret_cast<void**>(&batch)));

  const auto options = argv[2];

  bool keys = true;
  NAPI_STATUS_THROWS(GetProperty(env, options, "keys", keys));

  bool values = true;
  NAPI_STATUS_THROWS(GetProperty(env, options, "values", values));

  bool data = true;
  NAPI_STATUS_THROWS(GetProperty(env, options, "data", data));

  Encoding keyEncoding = Encoding::String;
  NAPI_STATUS_THROWS(GetProperty(env, options, "keyEncoding", keyEncoding));

  Encoding valueEncoding = Encoding::String;
  NAPI_STATUS_THROWS(GetProperty(env, options, "valueEncoding", valueEncoding));

  rocksdb::ColumnFamilyHandle* column = nullptr;
  NAPI_STATUS_THROWS(GetProperty(env, options, "column", column));

  BatchIterator iterator(nullptr, keys, values, data, column, keyEncoding, valueEncoding);

  napi_value result;
  NAPI_STATUS_THROWS(iterator.Iterate(env, *batch, &result));

  return result;
}

NAPI_INIT() {
  NAPI_EXPORT_FUNCTION(db_init);
  NAPI_EXPORT_FUNCTION(db_open);
  NAPI_EXPORT_FUNCTION(db_get_identity);
  NAPI_EXPORT_FUNCTION(db_get_handle);
  NAPI_EXPORT_FUNCTION(db_get_location);
  NAPI_EXPORT_FUNCTION(db_close);
  NAPI_EXPORT_FUNCTION(db_get_many);
  NAPI_EXPORT_FUNCTION(db_clear);
  NAPI_EXPORT_FUNCTION(db_get_property);
  NAPI_EXPORT_FUNCTION(db_get_latest_sequence);
  NAPI_EXPORT_FUNCTION(db_get_merge_operands);
  NAPI_EXPORT_FUNCTION(db_put);
	NAPI_EXPORT_FUNCTION(db_get);

  NAPI_EXPORT_FUNCTION(iterator_init);
  NAPI_EXPORT_FUNCTION(iterator_seek);
  NAPI_EXPORT_FUNCTION(iterator_close);
  NAPI_EXPORT_FUNCTION(iterator_nextv);

  NAPI_EXPORT_FUNCTION(batch_init);
  NAPI_EXPORT_FUNCTION(batch_put);
  NAPI_EXPORT_FUNCTION(batch_del);
  NAPI_EXPORT_FUNCTION(batch_clear);
  NAPI_EXPORT_FUNCTION(batch_write);
  NAPI_EXPORT_FUNCTION(batch_merge);
  NAPI_EXPORT_FUNCTION(batch_count);
  NAPI_EXPORT_FUNCTION(batch_iterate);
}
