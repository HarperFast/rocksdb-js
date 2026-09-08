#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>
#include "core/column_family_gate.h"
#include "rocksdb/write_batch.h"

using rocksdb_js::ColumnFamilyAdmission;
using rocksdb_js::ColumnFamilyGate;
using rocksdb_js::StagedColumnFamilies;

namespace {

std::shared_ptr<ColumnFamilyGate> gate(uint32_t id) {
	return std::make_shared<ColumnFamilyGate>(id, "cf" + std::to_string(id));
}

} // namespace

TEST(ColumnFamilyGate, AdmitsAndReleasesWhileLive) {
	ColumnFamilyGate g(1, "cf1");
	EXPECT_TRUE(g.tryAdmit());
	EXPECT_TRUE(g.tryAdmit());
	EXPECT_EQ(g.admitted(), 2u);
	g.release();
	g.release();
	EXPECT_EQ(g.admitted(), 0u);
	EXPECT_FALSE(g.isDropping());
}

TEST(ColumnFamilyGate, BeginDropRefusesLaterAdmissionsAndIsIdempotent) {
	ColumnFamilyGate g(1, "cf1");
	EXPECT_TRUE(g.beginDrop());
	EXPECT_FALSE(g.beginDrop()); // second dropper: already flipped
	EXPECT_TRUE(g.isDropping());
	EXPECT_FALSE(g.tryAdmit());
	EXPECT_EQ(g.admitted(), 0u); // the refused attempt left no count behind
	EXPECT_FALSE(g.isDropped());
	g.markDropped();
	EXPECT_TRUE(g.isDropped());
}

TEST(ColumnFamilyGate, DropWaitsForAdmittedCommitsThenProceeds) {
	ColumnFamilyGate g(1, "cf1");
	ASSERT_TRUE(g.tryAdmit());
	ASSERT_TRUE(g.tryAdmit());

	std::atomic<bool> dropPassed{false};
	std::thread dropper([&] {
		g.beginDrop();
		g.waitForAdmitted();
		dropPassed.store(true);
	});

	// The drop cannot pass while two admissions are outstanding, and once it
	// has begun no new admission may land.
	std::this_thread::sleep_for(std::chrono::milliseconds(50));
	EXPECT_FALSE(dropPassed.load());
	EXPECT_FALSE(g.tryAdmit());

	g.release();
	std::this_thread::sleep_for(std::chrono::milliseconds(50));
	EXPECT_FALSE(dropPassed.load()); // one still admitted
	g.release();
	dropper.join();
	EXPECT_TRUE(dropPassed.load());
}

TEST(ColumnFamilyGate, WaitReturnsImmediatelyWithNothingAdmitted) {
	ColumnFamilyGate g(1, "cf1");
	g.beginDrop();
	g.waitForAdmitted(); // must not block
	EXPECT_TRUE(g.isDropping());
}

// Concurrent admitters against a dropper: once the dropper is past its wait,
// no admitter may still be inside its admitted window, and no admitter is
// admitted afterwards.
TEST(ColumnFamilyGate, NoAdmissionOverlapsACompletedDrop) {
	ColumnFamilyGate g(7, "cf7");
	std::atomic<bool> stop{false};
	std::atomic<int> insideAfterDrop{0};
	std::atomic<bool> dropDone{false};
	std::atomic<uint64_t> admittedTotal{0};

	std::vector<std::thread> admitters;
	for (int t = 0; t < 8; ++t) {
		admitters.emplace_back([&] {
			while (!stop.load(std::memory_order_acquire)) {
				if (g.tryAdmit()) {
					admittedTotal.fetch_add(1, std::memory_order_relaxed);
					if (dropDone.load(std::memory_order_acquire)) {
						insideAfterDrop.fetch_add(1, std::memory_order_relaxed);
					}
					g.release();
				}
			}
		});
	}

	std::this_thread::sleep_for(std::chrono::milliseconds(20));
	g.beginDrop();
	g.waitForAdmitted();
	dropDone.store(true, std::memory_order_release);
	// Anything admitted from here on would be the poison this gate exists to prevent.
	std::this_thread::sleep_for(std::chrono::milliseconds(20));
	stop.store(true, std::memory_order_release);
	for (auto& t : admitters) t.join();

	EXPECT_GT(admittedTotal.load(), 0u);
	EXPECT_EQ(insideAfterDrop.load(), 0);
	EXPECT_EQ(g.admitted(), 0u);
}

TEST(ColumnFamilyAdmission, AllOrNothingUnwindsOnRefusal) {
	ColumnFamilyGate a(1, "a"), b(2, "b"), c(3, "c");
	b.beginDrop();
	{
		ColumnFamilyAdmission admission;
		EXPECT_TRUE(admission.admit(a));
		EXPECT_EQ(a.admitted(), 1u);
		EXPECT_FALSE(admission.admit(b));
		EXPECT_EQ(admission.refused(), &b);
		// the refusal released what was already admitted
		EXPECT_EQ(a.admitted(), 0u);
		EXPECT_EQ(admission.size(), 0u);
		EXPECT_EQ(c.admitted(), 0u);
	}
	EXPECT_EQ(a.admitted(), 0u);
}

TEST(ColumnFamilyAdmission, ScopeExitReleasesEverythingIncludingOverflow) {
	std::vector<std::unique_ptr<ColumnFamilyGate>> gates;
	for (uint32_t i = 1; i <= StagedColumnFamilies::kInline + 3; ++i) {
		gates.push_back(std::make_unique<ColumnFamilyGate>(i, "cf"));
	}
	{
		ColumnFamilyAdmission admission;
		admission.reserve(gates.size());
		for (auto& g : gates) {
			EXPECT_TRUE(admission.admit(*g));
		}
		EXPECT_EQ(admission.size(), gates.size());
		for (auto& g : gates) {
			EXPECT_EQ(g->admitted(), 1u);
		}
	}
	for (auto& g : gates) {
		EXPECT_EQ(g->admitted(), 0u);
	}
}

TEST(StagedColumnFamilies, DeduplicatesIgnoresNullAndClears) {
	StagedColumnFamilies staged;
	auto g1 = gate(1);
	auto g2 = gate(2);
	staged.note(nullptr); // the default family carries no gate
	staged.note(g1);
	staged.note(g1);
	staged.note(g2);
	staged.note(g1);
	EXPECT_EQ(staged.size(), 2u);
	EXPECT_FALSE(staged.overflowed());
	EXPECT_EQ(&staged.at(0), g1.get());
	EXPECT_EQ(&staged.at(1), g2.get());
	EXPECT_EQ(g1.use_count(), 2); // one pin per distinct family
	staged.clear();
	EXPECT_EQ(staged.size(), 0u);
	EXPECT_EQ(g1.use_count(), 1);
}

TEST(StagedColumnFamilies, OverflowFlagsWithoutGrowing) {
	StagedColumnFamilies staged;
	std::vector<std::shared_ptr<ColumnFamilyGate>> gates;
	for (uint32_t i = 1; i <= StagedColumnFamilies::kInline; ++i) {
		gates.push_back(gate(i));
		staged.note(gates.back());
	}
	EXPECT_EQ(staged.size(), StagedColumnFamilies::kInline);
	EXPECT_FALSE(staged.overflowed());
	auto extra = gate(100);
	staged.note(extra);
	EXPECT_TRUE(staged.overflowed());
	EXPECT_EQ(staged.size(), StagedColumnFamilies::kInline);
	EXPECT_EQ(extra.use_count(), 1); // the overflowing family is not pinned
	staged.clear();
	EXPECT_FALSE(staged.overflowed());
}

TEST(CollectColumnFamilyIds, DistinctIdsInFirstAppearanceOrder) {
	rocksdb::WriteBatch batch;
	ASSERT_TRUE(batch.Put("k0", "v").ok()); // default family (id 0)
	ASSERT_TRUE(batch.Delete("k1").ok());
	std::vector<uint32_t> ids;
	ASSERT_TRUE(rocksdb_js::collectColumnFamilyIds(batch, ids).ok());
	ASSERT_EQ(ids.size(), 1u);
	EXPECT_EQ(ids[0], 0u);
}

TEST(CollectColumnFamilyIds, EmptyBatchYieldsNothing) {
	rocksdb::WriteBatch batch;
	std::vector<uint32_t> ids;
	ASSERT_TRUE(rocksdb_js::collectColumnFamilyIds(batch, ids).ok());
	EXPECT_TRUE(ids.empty());
}

// A fresh gate for a recreated same-name family starts open: generations are
// distinct objects, so a dropped generation's closed gate cannot leak into the
// next one.
TEST(ColumnFamilyGate, RecreatedGenerationStartsOpen) {
	auto old = gate(5);
	old->beginDrop();
	old->markDropped();
	auto fresh = std::make_shared<ColumnFamilyGate>(9, old->name);
	EXPECT_FALSE(old->tryAdmit());
	EXPECT_TRUE(fresh->tryAdmit());
	fresh->release();
}
