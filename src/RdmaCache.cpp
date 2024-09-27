#include "RdmaCache.h"

Cache::Cache(const CacheConfig &cache_config) {
    size = cache_config.cacheSize;
    printf("-----alloc 2------\n");
    data = (uint64_t)hugePageAlloc(size * define::GB);
    printf("-----alloc 2 end------\n");
}

Cache::~Cache() { hugePageFree((void *)data, size * define::GB); }