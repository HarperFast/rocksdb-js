// Stub bodies for the slice of EventEmitter that low-level files
// (transaction_log_file.cpp) transitively reference via emitGlobalEvent.
// The native GoogleTest binary doesn't link the Node-API runtime, so the
// real event_emitter.cpp — which calls napi_* — can't be included. These
// stubs let recovery code emit warnings unchanged; the emission is a
// no-op in test builds since there are no JS listeners to dispatch to.

#include "napi/event_emitter.h"

namespace rocksdb_js {

bool EventEmitter::notify(const std::string& /*key*/, ListenerData* data) {
	delete data;
	return false;
}

void EventEmitter::releaseAll() {}

} // namespace rocksdb_js
