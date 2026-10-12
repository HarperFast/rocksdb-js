#include <gtest/gtest.h>
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <system_error>
#include <vector>
#include "database/backup_verify_tables.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/backup_engine.h"

using rocksdb_js::verifyBackupTables;

namespace {

struct BackupFixture {
	std::string root;
	std::string dbDir;
	std::string backupDir;

	BackupFixture(
		const char* name,
		int tableCount,
		rocksdb::CompressionType compression,
		bool shareTableFiles = true,
		bool shareFilesWithChecksum = true
	) {
		root = (std::filesystem::temp_directory_path() / name).string();
		std::filesystem::remove_all(root);
		std::filesystem::create_directories(root);
		dbDir = root + "/db";
		backupDir = root + "/backup";

		rocksdb::Options options;
		options.create_if_missing = true;
		options.disable_auto_compactions = true;
		options.compression = compression;
		std::unique_ptr<rocksdb::DB> db;
		rocksdb::Status s = rocksdb::DB::Open(options, dbDir, &db);
		EXPECT_TRUE(s.ok()) << s.ToString();
		if (!db) {
			return;
		}
		// Compressible values, so the table actually records a compressed block.
		for (int i = 0; i < tableCount; ++i) {
			for (int k = 0; k < 16; ++k) {
				db->Put(rocksdb::WriteOptions(), "key" + std::to_string(i) + "-" + std::to_string(k), std::string(512, 'a' + (k % 4)));
			}
			EXPECT_TRUE(db->Flush(rocksdb::FlushOptions()).ok());
		}

		rocksdb::BackupEngineOptions engineOptions(backupDir);
		engineOptions.share_table_files = shareTableFiles;
		engineOptions.share_files_with_checksum = shareFilesWithChecksum;
		rocksdb::BackupEngine* engine = nullptr;
		rocksdb::IOStatus io = rocksdb::BackupEngine::Open(engineOptions, rocksdb::Env::Default(), &engine);
		EXPECT_TRUE(io.ok()) << io.ToString();
		if (engine) {
			io = engine->CreateNewBackup(db.get());
			EXPECT_TRUE(io.ok()) << io.ToString();
			delete engine;
		}
	}

	~BackupFixture() {
		std::error_code ec;
		std::filesystem::remove_all(root, ec);
	}

	std::vector<std::string> tableFiles() const {
		std::vector<std::string> files;
		for (const auto& entry : std::filesystem::recursive_directory_iterator(backupDir)) {
			if (entry.is_regular_file() && entry.path().extension() == ".sst") {
				files.push_back(entry.path().string());
			}
		}
		std::sort(files.begin(), files.end());
		return files;
	}

	rocksdb::Status verify(
		rocksdb::Env* env = rocksdb::Env::Default(),
		bool (*isSupported)(rocksdb::CompressionType) = rocksdb_js::isCompressionSupported
	) const {
		rocksdb::BackupEngineReadOnly* engine = nullptr;
		rocksdb::IOStatus io = rocksdb::BackupEngineReadOnly::Open(rocksdb::BackupEngineOptions(backupDir), rocksdb::Env::Default(), &engine);
		if (!io.ok()) {
			return io;
		}
		rocksdb::Status s = verifyBackupTables(engine, 1, backupDir, env, isSupported);
		delete engine;
		return s;
	}
};

rocksdb::CompressionType firstSupportedCompression() {
	for (rocksdb::CompressionType type : rocksdb::GetSupportedCompressions()) {
		if (type != rocksdb::kNoCompression) {
			return type;
		}
	}
	return rocksdb::kNoCompression;
}

// The footer ends with a 4-byte little-endian format_version followed by the
// 8-byte table magic number.
void patchFormatVersion(const std::string& path, uint32_t version) {
	std::fstream file(path, std::ios::in | std::ios::out | std::ios::binary);
	ASSERT_TRUE(file.is_open()) << path;
	file.seekp(-12, std::ios::end);
	char bytes[4] = {
		static_cast<char>(version & 0xff),
		static_cast<char>((version >> 8) & 0xff),
		static_cast<char>((version >> 16) & 0xff),
		static_cast<char>((version >> 24) & 0xff),
	};
	file.write(bytes, sizeof(bytes));
	ASSERT_TRUE(file.good());
}

class TrackedRandomAccessFile : public rocksdb::FSRandomAccessFileOwnerWrapper {
public:
	TrackedRandomAccessFile(std::unique_ptr<rocksdb::FSRandomAccessFile>&& file, std::atomic<int>& open) :
		rocksdb::FSRandomAccessFileOwnerWrapper(std::move(file)),
		open_(open) {}

	~TrackedRandomAccessFile() override { --open_; }

private:
	std::atomic<int>& open_;
};

// Records how many random-access files (the handles SstFileReader opens) are
// alive at once.
class CountingFileSystem : public rocksdb::FileSystemWrapper {
public:
	explicit CountingFileSystem(const std::shared_ptr<rocksdb::FileSystem>& base) : rocksdb::FileSystemWrapper(base) {}

	const char* Name() const override { return "CountingFileSystem"; }

	rocksdb::IOStatus NewRandomAccessFile(
		const std::string& fname,
		const rocksdb::FileOptions& options,
		std::unique_ptr<rocksdb::FSRandomAccessFile>* result,
		rocksdb::IODebugContext* dbg
	) override {
		std::unique_ptr<rocksdb::FSRandomAccessFile> file;
		rocksdb::IOStatus s = target()->NewRandomAccessFile(fname, options, &file, dbg);
		if (s.ok()) {
			++opened;
			int now = ++open;
			int seen = peak.load();
			while (now > seen && !peak.compare_exchange_weak(seen, now)) {
			}
			result->reset(new TrackedRandomAccessFile(std::move(file), open));
		}
		return s;
	}

	std::atomic<int> open{ 0 };
	std::atomic<int> peak{ 0 };
	std::atomic<int> opened{ 0 };
};

} // namespace

TEST(BackupVerifyTables, HealthyBackupPasses) {
	BackupFixture fixture("rocksdb-js-verify-tables-healthy", 3, firstSupportedCompression());
	ASSERT_EQ(fixture.tableFiles().size(), 3u);
	rocksdb::Status s = fixture.verify();
	EXPECT_TRUE(s.ok()) << s.ToString();
}

TEST(BackupVerifyTables, HealthyBackupPassesForEverySharingLayout) {
	// shared_checksum/ (default), shared/, and private/<id>/ name tables differently
	// in the backup metadata.
	BackupFixture shared("rocksdb-js-verify-tables-shared", 2, firstSupportedCompression(), true, false);
	rocksdb::Status s = shared.verify();
	EXPECT_TRUE(s.ok()) << s.ToString();

	BackupFixture unshared("rocksdb-js-verify-tables-private", 2, firstSupportedCompression(), false);
	s = unshared.verify();
	EXPECT_TRUE(s.ok()) << s.ToString();
}

TEST(BackupVerifyTables, UnsupportedFormatVersionFailsAndNamesTheFile) {
	BackupFixture fixture("rocksdb-js-verify-tables-format", 3, firstSupportedCompression());
	std::vector<std::string> tables = fixture.tableFiles();
	ASSERT_EQ(tables.size(), 3u);
	patchFormatVersion(tables.back(), 99);

	rocksdb::BackupEngineReadOnly* engine = nullptr;
	ASSERT_TRUE(rocksdb::BackupEngineReadOnly::Open(rocksdb::BackupEngineOptions(fixture.backupDir), rocksdb::Env::Default(), &engine).ok());
	rocksdb::IOStatus sizeOnly = engine->VerifyBackup(1, false);
	delete engine;
	EXPECT_TRUE(sizeOnly.ok()) << "the existing size check is expected to miss this: " << sizeOnly.ToString();

	rocksdb::Status s = fixture.verify();
	EXPECT_TRUE(s.IsCorruption()) << s.ToString();
	EXPECT_NE(s.ToString().find(std::filesystem::path(tables.back()).filename().string()), std::string::npos) << s.ToString();
	EXPECT_NE(s.ToString().find("format_version"), std::string::npos) << s.ToString();
}

TEST(BackupVerifyTables, UnsupportedCompressionFailsAndNamesTheFile) {
	rocksdb::CompressionType compression = firstSupportedCompression();
	if (compression == rocksdb::kNoCompression) {
		GTEST_SKIP() << "this build links no compression library";
	}
	BackupFixture fixture("rocksdb-js-verify-tables-compression", 1, compression);
	std::vector<std::string> tables = fixture.tableFiles();
	ASSERT_EQ(tables.size(), 1u);

	static rocksdb::CompressionType missing;
	missing = compression;
	rocksdb::Status s = fixture.verify(rocksdb::Env::Default(), [](rocksdb::CompressionType type) {
		return type != missing;
	});
	EXPECT_TRUE(s.IsNotSupported()) << s.ToString();
	EXPECT_NE(s.ToString().find(std::filesystem::path(tables.front()).filename().string()), std::string::npos) << s.ToString();
}

TEST(BackupVerifyTables, OpensOneTableAtATime) {
	constexpr int kTables = 40;
	BackupFixture fixture("rocksdb-js-verify-tables-fds", kTables, firstSupportedCompression());
	std::vector<std::string> tables = fixture.tableFiles();
	ASSERT_EQ(tables.size(), static_cast<size_t>(kTables));

	auto fs = std::make_shared<CountingFileSystem>(rocksdb::FileSystem::Default());
	std::unique_ptr<rocksdb::Env> env = rocksdb::NewCompositeEnv(fs);
	rocksdb::Status s = fixture.verify(env.get());
	EXPECT_TRUE(s.ok()) << s.ToString();
	EXPECT_EQ(fs->opened.load(), kTables);
	EXPECT_EQ(fs->peak.load(), 1);
	EXPECT_EQ(fs->open.load(), 0);

	// A bad table among many is still caught, and the failure path closes it too.
	patchFormatVersion(tables[kTables / 2], 99);
	auto failingFs = std::make_shared<CountingFileSystem>(rocksdb::FileSystem::Default());
	std::unique_ptr<rocksdb::Env> failingEnv = rocksdb::NewCompositeEnv(failingFs);
	s = fixture.verify(failingEnv.get());
	EXPECT_TRUE(s.IsCorruption()) << s.ToString();
	EXPECT_NE(s.ToString().find(std::filesystem::path(tables[kTables / 2]).filename().string()), std::string::npos) << s.ToString();
	EXPECT_EQ(failingFs->peak.load(), 1);
	EXPECT_EQ(failingFs->open.load(), 0);
}
