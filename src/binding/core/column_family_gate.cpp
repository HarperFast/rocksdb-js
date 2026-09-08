#include "core/column_family_gate.h"
#include <algorithm>

namespace rocksdb_js {

namespace {

// Every record kind carries a column family id; the batch is never mutated.
// Prepare/commit markers are accepted (not refused, as the base class does) so
// a 2PC-shaped batch still yields its families rather than an error.
class ColumnFamilyIdCollector final : public rocksdb::WriteBatch::Handler {
public:
	explicit ColumnFamilyIdCollector(std::vector<uint32_t>& ids) : ids(ids) {}

	rocksdb::Status PutCF(uint32_t cf, const rocksdb::Slice&, const rocksdb::Slice&) override { return this->note(cf); }
	rocksdb::Status TimedPutCF(uint32_t cf, const rocksdb::Slice&, const rocksdb::Slice&, uint64_t) override { return this->note(cf); }
	rocksdb::Status PutEntityCF(uint32_t cf, const rocksdb::Slice&, const rocksdb::Slice&) override { return this->note(cf); }
	rocksdb::Status DeleteCF(uint32_t cf, const rocksdb::Slice&) override { return this->note(cf); }
	rocksdb::Status SingleDeleteCF(uint32_t cf, const rocksdb::Slice&) override { return this->note(cf); }
	rocksdb::Status DeleteRangeCF(uint32_t cf, const rocksdb::Slice&, const rocksdb::Slice&) override { return this->note(cf); }
	rocksdb::Status MergeCF(uint32_t cf, const rocksdb::Slice&, const rocksdb::Slice&) override { return this->note(cf); }
	rocksdb::Status PutBlobIndexCF(uint32_t cf, const rocksdb::Slice&, const rocksdb::Slice&) override { return this->note(cf); }
	rocksdb::Status MarkBeginPrepare(bool) override { return rocksdb::Status::OK(); }
	rocksdb::Status MarkEndPrepare(const rocksdb::Slice&) override { return rocksdb::Status::OK(); }
	rocksdb::Status MarkNoop(bool) override { return rocksdb::Status::OK(); }
	rocksdb::Status MarkRollback(const rocksdb::Slice&) override { return rocksdb::Status::OK(); }
	rocksdb::Status MarkCommit(const rocksdb::Slice&) override { return rocksdb::Status::OK(); }
	rocksdb::Status MarkCommitWithTimestamp(const rocksdb::Slice&, const rocksdb::Slice&) override { return rocksdb::Status::OK(); }

private:
	rocksdb::Status note(uint32_t cf) {
		if (std::find(this->ids.begin(), this->ids.end(), cf) == this->ids.end()) {
			this->ids.push_back(cf);
		}
		return rocksdb::Status::OK();
	}

	std::vector<uint32_t>& ids;
};

} // namespace

rocksdb::Status collectColumnFamilyIds(const rocksdb::WriteBatch& batch, std::vector<uint32_t>& ids) {
	ColumnFamilyIdCollector collector(ids);
	return batch.Iterate(&collector);
}

} // namespace rocksdb_js
