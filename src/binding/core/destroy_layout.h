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

/** Applies the retained-layout authority rule from AGENTS invariant 17. */
bool updateRetainedDestroyLayout(
	DBFileLayout& retained,
	DBFileLayout observed,
	bool writableOpen
);

}

#endif
