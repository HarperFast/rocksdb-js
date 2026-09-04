#include "core/destroy_layout.h"
#include <algorithm>
#include <utility>

namespace rocksdb_js {

static bool sameDbPaths(
	const std::vector<rocksdb::DbPath>& retained,
	const std::vector<rocksdb::DbPath>& candidate
) {
	return retained.size() == candidate.size() &&
		std::equal(retained.begin(), retained.end(), candidate.begin(),
			[](const rocksdb::DbPath& a, const rocksdb::DbPath& b) {
				return a.path == b.path;
			});
}

bool extendsDbPaths(
	const std::vector<rocksdb::DbPath>& retained,
	const std::vector<rocksdb::DbPath>& candidate
) {
	return candidate.size() >= retained.size() &&
		std::equal(retained.begin(), retained.end(), candidate.begin(),
			[](const rocksdb::DbPath& a, const rocksdb::DbPath& b) {
				return a.path == b.path;
			});
}

bool updateRetainedDestroyLayout(
	DBFileLayout& retained,
	DBFileLayout observed,
	bool writableOpen
) {
	bool pathsAccepted = true;
	if (writableOpen) {
		if (extendsDbPaths(retained.dbPaths, observed.dbPaths)) {
			retained.dbPaths = std::move(observed.dbPaths);
		} else {
			pathsAccepted = false;
		}
	} else if (!sameDbPaths(retained.dbPaths, observed.dbPaths)) {
		pathsAccepted = false;
	}
	retained.blobDirs = std::move(observed.blobDirs);
	return pathsAccepted;
}

}
