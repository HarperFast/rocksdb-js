#ifndef __CORE_CLOSABLE_H__
#define __CORE_CLOSABLE_H__

namespace rocksdb_js {

struct Closable {
	virtual ~Closable() = default;
	virtual void close() = 0;
};

} // namespace rocksdb_js

#endif
