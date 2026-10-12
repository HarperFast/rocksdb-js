#include "database/backup_verify_tables.h"
#include <memory>
#include "rocksdb/advanced_compression.h"
#include "rocksdb/metadata.h"
#include "rocksdb/options.h"
#include "rocksdb/sst_file_reader.h"
#include "rocksdb/table_properties.h"

namespace rocksdb_js {

namespace {

// Keeps the status code (an EMFILE or EACCES must not read as a corrupt
// backup) and prefixes the file the failure belongs to.
rocksdb::Status withFileName(const rocksdb::Status& s, const std::string& name) {
	std::string msg = s.getState() != nullptr ? std::string(s.getState()) : s.ToString();
	return rocksdb::Status(s.code(), s.subcode(), s.severity(), name + ": " + msg);
}

} // namespace

// A plain string check rather than std::filesystem::path, so `\` is a
// separator on every platform and no code-page conversion happens on Windows.
bool isConfinedBackupPath(const std::string& relative) {
	if (relative.empty() || relative[0] == '/' || relative[0] == '\\' || relative.find(':') != std::string::npos) {
		return false;
	}
	size_t start = 0;
	while (start <= relative.size()) {
		size_t end = relative.find_first_of("/\\", start);
		if (end == std::string::npos) {
			end = relative.size();
		}
		if (relative.compare(start, end - start, "..") == 0) {
			return false;
		}
		start = end + 1;
	}
	return true;
}

rocksdb::Status verifyBackupTables(
	rocksdb::BackupEngineReadOnly* engine,
	rocksdb::BackupID backupId,
	const std::string& backupDir,
	rocksdb::Env* env,
	bool (*isSupported)(rocksdb::CompressionType)
) {
	rocksdb::BackupInfo info;
	rocksdb::Status s = engine->GetBackupInfo(backupId, &info, /*include_file_details=*/true);
	if (!s.ok()) {
		return s;
	}

	rocksdb::Options options;
	options.env = env;
	const std::shared_ptr<rocksdb::CompressionManager>& builtin = rocksdb::GetBuiltinV2CompressionManager();

	for (const rocksdb::BackupFileInfo& file : info.file_details) {
		if (file.file_type != rocksdb::kTableFile) {
			continue;
		}
		const std::string& name = file.relative_filename;
		if (!isConfinedBackupPath(name)) {
			return rocksdb::Status::Corruption(name, "backup metadata names a table outside the backup directory");
		}

		rocksdb::SstFileReader reader(options);
		s = reader.Open(backupDir + "/" + name);
		if (!s.ok()) {
			return withFileName(s, name);
		}

		std::shared_ptr<const rocksdb::TableProperties> props = reader.GetTableProperties();
		if (!props) {
			return rocksdb::Status::Corruption(name, "table properties are unreadable");
		}
		auto types = tableCompressionTypes(props->compression_name);
		if (!types) {
			return rocksdb::Status::NotSupported(name, "unrecognized table compression \"" + props->compression_name + "\"");
		}
		for (rocksdb::CompressionType type : *types) {
			if (!isSupported(type)) {
				return rocksdb::Status::NotSupported(
					name,
					"table uses compression " + builtin->CompressionTypeToString(type) + ", which this build does not support"
				);
			}
		}
	}
	return rocksdb::Status::OK();
}

} // namespace rocksdb_js
