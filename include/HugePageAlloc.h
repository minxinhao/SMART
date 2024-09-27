#ifndef __HUGEPAGEALLOC_H__
#define __HUGEPAGEALLOC_H__


#include "Debug.h"

#include <cstdint>

#include <sys/mman.h>
#include <memory.h>


char *getIP();

// 替换为malloc
inline void *hugePageAlloc(size_t size) {

    // void *res = mmap(NULL, size, PROT_READ | PROT_WRITE,
    //                  MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
    void *res = malloc(size);
    if (res == MAP_FAILED) {
        Debug::notifyError("%s mmap failed!\n", getIP());
    }

    return res;
}

inline void hugePageFree(void *addr, size_t size) {
    free(addr);
    // int res = munmap(addr, size);
    // if (res == -1) {
    //     Debug::notifyError("%s munmap failed! %d\n", getIP(), errno);
    // }
    return;
}

#endif /* __HUGEPAGEALLOC_H__ */
