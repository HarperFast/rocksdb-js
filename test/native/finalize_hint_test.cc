#include <gtest/gtest.h>
#include <memory>
#include <thread>
#include "core/finalize_hint.h"

using rocksdb_js::SharedFinalizeHint;

namespace {

struct Hinted {
	std::shared_ptr<int> value = std::make_shared<int>(1);
	std::weak_ptr<int> watch = value;
	SharedFinalizeHint<int>* hint = new SharedFinalizeHint<int>(std::move(value));
};

} // namespace

TEST(SharedFinalizeHint, CreatedKeepsValueUntilFinalized) {
	Hinted h;
	EXPECT_EQ(*h.hint->get(), 1);
	h.hint->created();
	EXPECT_FALSE(h.watch.expired());
	SharedFinalizeHint<int>::finalize(h.hint);
	EXPECT_TRUE(h.watch.expired());
}

// Covers a finalizer never registered (the hint itself then leaks) and one that runs later.
TEST(SharedFinalizeHint, FailedReleasesValueBeforeAnyFinalizer) {
	Hinted h;
	h.hint->failed();
	EXPECT_TRUE(h.watch.expired());
	SharedFinalizeHint<int>::finalize(h.hint);
}

// node::Buffer::New runs the finalizer itself on some failures before the create returns.
TEST(SharedFinalizeHint, FailedAfterFinalizerAlreadyRan) {
	Hinted h;
	SharedFinalizeHint<int>::finalize(h.hint);
	EXPECT_FALSE(h.watch.expired());
	h.hint->failed();
	EXPECT_TRUE(h.watch.expired());
}

// A SharedArrayBuffer's finalizer can run on any thread.
TEST(SharedFinalizeHint, FinalizerOnAnotherThread) {
	for (int i = 0; i < 1000; i++) {
		Hinted h;
		std::thread finalizer([hint = h.hint]() { SharedFinalizeHint<int>::finalize(hint); });
		if (i % 2 == 0) {
			h.hint->created();
		} else {
			h.hint->failed();
		}
		finalizer.join();
		EXPECT_TRUE(h.watch.expired());
	}
}
