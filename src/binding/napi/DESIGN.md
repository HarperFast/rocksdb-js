# N-API error builders

`createRocksDBError` and `createJSError` (`helpers.cpp`) write their `error` out-param on every path. A failed N-API call inside either leaves a JS exception pending, or gets one synthesized; the builder takes that exception back as the value, so callers can `napi_throw` or reject with it without an initialisation check. `napi_get_and_clear_last_exception` does not fail on a live env: it returns `napi_ok`, with `undefined` when nothing is pending.

Regression: the async case in `test/error-object-failure.test.ts` (rejection through `backups.list`) fails on the pre-fix builder. The sync `createJSError` case passes there too: the synthesized exception is already pending, so the caller's `napi_throw` is a no-op. It checks the new path's result, not the regression.

# C++ exceptions in async work

N-API and libuv call the binding through C, so a C++ exception that leaves an async entry point, an execute callback or a complete callback terminates the process. `napi/async.h` contains them at those three boundaries:

- `guardAsyncEntry<Entry>` wraps each async entry in its property table and throws a JS error instead; every JS caller runs the entry inside a `new Promise` executor, so the throw rejects that promise. Unwinding is safe only because each wrapped entry owns what it took until the handoff: `PendingAsyncState` (or `PendingTransactionCommitState`) for the state, `ScopedAsyncWorkRegistration` for an admission taken before the allocation, and the in-flight claims' RAII guards. A new async entry needs the same before it is wrapped.
- `runAsyncExecute` wraps only the work of an execute callback, so the steps after it (releasing pins and in-flight claims, then `signalExecuteCompleted()`) always run. A failure becomes a message-free `Status`, because building a message can throw again.
- `runAsyncComplete` wraps only the settling part of a complete callback and rejects when it throws; releasing the state stays outside it. `callResolve`/`callReject` delete the unused callback only after calling theirs, so a failure before the call leaves the reject available.

Containment is best effort: building the JS error can fail too, and then the promise stays pending. Not covered: the commit lanes and the legacy commit execute/complete, the backup stream's threadsafe-function callbacks, and synchronous methods.

Regression: `test/async-exception.test.ts` injects `std::bad_alloc` at each stage through `forceAsyncExceptionForTesting`; on the pre-fix binding the first injected allocation aborts the process.
