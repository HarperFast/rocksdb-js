#include <napi-macros.h>
#include "rocksdb.h"
#include <stdio.h>

NAPI_INIT() {
	printf("Hello, world!\n");
}
