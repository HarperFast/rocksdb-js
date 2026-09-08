#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>
#include "core/column_family_gate.h"

using rocksdb_js::ColumnFamilyAdmission;
using rocksdb_js::ColumnFamilyGate;
using rocksdb_js::StagedColumnFamilies;

namespace {

std::shared_ptr<ColumnFamilyGate> gate(uint32_t id) {
	return std::make_shared<ColumnFamilyGate>(id, "cf" + std::to_string(id));
}

void waitUntil(const std::atomic<bool>& flag) {
	while (!flag.load(std::memory_order_acquire)) {
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}
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
	EXPECT_FALSE(g.beginDrop());
	EXPECT_TRUE(g.isDropping());
	EXPECT_FALSE(g.tryAdmit());
	EXPECT_EQ(g.admitted(), 0u);
	EXPECT_FALSE(g.isDropped());
	g.markDropped();
	EXPECT_TRUE(g.isDropped());
	EXPECT_EQ(rocksdb_js::columnFamilyDroppedStatus(g).ToString(),
		"Column family dropped: column family \"cf1\" was dropped");
}

TEST(ColumnFamilyGate, DropWaitsForAdmittedCommitsThenProceeds) {
	ColumnFamilyGate g(1, "cf1");
	ASSERT_TRUE(g.tryAdmit());
	ASSERT_TRUE(g.tryAdmit());

	std::atomic<bool> dropBegan{false};
	std::atomic<bool> dropPassed{false};
	std::thread dropper([&] {
		g.beginDrop();
		dropBegan.store(true, std::memory_order_release);
		g.waitForAdmitted();
		dropPassed.store(true, std::memory_order_release);
	});
	waitUntil(dropBegan);

	std::this_thread::sleep_for(std::chrono::milliseconds(50));
	EXPECT_FALSE(dropPassed.load());
	if (g.tryAdmit()) {
		ADD_FAILURE() << "admitted after beginDrop";
		g.release();
	}

	g.release();
	std::this_thread::sleep_for(std::chrono::milliseconds(50));
	EXPECT_FALSE(dropPassed.load());
	g.release();
	dropper.join();
	EXPECT_TRUE(dropPassed.load());
}

TEST(ColumnFamilyGate, WaitReturnsImmediatelyWithNothingAdmitted) {
	ColumnFamilyGate g(1, "cf1");
	g.beginDrop();
	g.waitForAdmitted();
	EXPECT_TRUE(g.isDropping());
}

// Once the dropper is past its wait, no admitter may still be inside its
// admitted window and none may be admitted afterwards.
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
	staged.note(nullptr);
	staged.note(g1);
	staged.note(g1);
	staged.note(g2);
	staged.note(g1);
	EXPECT_EQ(staged.size(), 2u);
	EXPECT_EQ(&staged.at(0), g1.get());
	EXPECT_EQ(&staged.at(1), g2.get());
	EXPECT_EQ(g1.use_count(), 2);
	EXPECT_EQ(staged.firstDropping(), nullptr);
	g2->beginDrop();
	EXPECT_EQ(staged.firstDropping(), g2.get());
	staged.clear();
	EXPECT_EQ(staged.size(), 0u);
	EXPECT_EQ(g1.use_count(), 1);
}

TEST(StagedColumnFamilies, ForgetLastUndoesOnlyANewNote) {
	StagedColumnFamilies staged;
	auto g1 = gate(1);
	auto g2 = gate(2);
	EXPECT_TRUE(staged.note(g1));
	EXPECT_FALSE(staged.note(g1));
	EXPECT_TRUE(staged.note(g2));
	staged.forgetLast();
	EXPECT_EQ(staged.size(), 1u);
	EXPECT_EQ(&staged.at(0), g1.get());
	EXPECT_EQ(g2.use_count(), 1);
	// across the inline boundary the undo comes off the overflow vector first
	std::vector<std::shared_ptr<ColumnFamilyGate>> more;
	for (uint32_t i = 10; i < 10 + StagedColumnFamilies::kInline; ++i) {
		more.push_back(gate(i));
		staged.note(more.back());
	}
	EXPECT_EQ(staged.size(), StagedColumnFamilies::kInline + 1);
	staged.forgetLast();
	EXPECT_EQ(staged.size(), StagedColumnFamilies::kInline);
	EXPECT_EQ(more.back().use_count(), 1);
}

TEST(StagedColumnFamilies, OverflowKeepsEveryDistinctFamily) {
	StagedColumnFamilies staged;
	std::vector<std::shared_ptr<ColumnFamilyGate>> gates;
	for (uint32_t i = 1; i <= StagedColumnFamilies::kInline + 4; ++i) {
		gates.push_back(gate(i));
		staged.note(gates.back());
		staged.note(gates.back());
	}
	EXPECT_EQ(staged.size(), gates.size());
	for (size_t i = 0; i < gates.size(); ++i) {
		EXPECT_EQ(&staged.at(i), gates[i].get());
		EXPECT_EQ(gates[i].use_count(), 2);
	}
	gates.back()->beginDrop();
	EXPECT_EQ(staged.firstDropping(), gates.back().get());
	staged.clear();
	EXPECT_EQ(staged.size(), 0u);
	for (auto& g : gates) {
		EXPECT_EQ(g.use_count(), 1);
	}
}

// Generations are distinct objects: a dropped generation's closed gate cannot
// leak into a recreated same-name family.
TEST(ColumnFamilyGate, RecreatedGenerationStartsOpen) {
	auto old = gate(5);
	old->beginDrop();
	old->markDropped();
	auto fresh = std::make_shared<ColumnFamilyGate>(9, old->name);
	EXPECT_FALSE(old->tryAdmit());
	EXPECT_TRUE(fresh->tryAdmit());
	fresh->release();
}
