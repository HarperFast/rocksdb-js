# N-API error builders

`createRocksDBError` and `createJSError` (`helpers.cpp`) write their `error` out-param on every path. A failed N-API call inside either leaves a JS exception pending, or gets one synthesized; the builder takes that exception back as the value, so callers can `napi_throw` or reject with it without an initialisation check.

Regression: the async case in `test/error-object-failure.test.ts` (rejection through `backups.list`) fails on the pre-fix builder. The sync `createJSError` case passes there too: the synthesized exception is already pending, so the caller's `napi_throw` is a no-op. It checks the new path's result, not the regression.
