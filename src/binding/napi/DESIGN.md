# N-API error builders

`createRocksDBError` and `createJSError` (`helpers.cpp`) write their `error` out-param on every path. A failed N-API call inside either leaves a JS exception pending, or gets one synthesized; the builder takes that exception back as the value, so callers can `napi_throw` or reject with it without an initialisation check. Enforced by `test/error-object-failure.test.ts`, which patches `Object.create` in a child process and covers one async and one sync site.
