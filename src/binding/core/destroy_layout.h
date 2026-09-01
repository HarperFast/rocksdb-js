#ifndef __CORE_DESTROY_LAYOUT_H__
#define __CORE_DESTROY_LAYOUT_H__

#include <string>
#include <unordered_map>
#include <vector>
#include "rocksdb/options.h"

namespace rocksdb_js {

struct DBFileLayout {
	std::vector<rocksdb::DbPath> dbPaths;
	std::unordered_map<std::string, std::string> blobDirs;
};

bool extendsDbPaths(
	const std::vector<rocksdb::DbPath>& retained,
	const std::vector<rocksdb::DbPath>& candidate
);

/**
 * Records one successful open without letting a reader expand destroy targets.
 * Returns false when the candidate path list was refused.
 */
bool updateRetainedDestroyLayout(
	DBFileLayout& retained,
	DBFileLayout observed,
	bool writableOpen
);

}

#endif
