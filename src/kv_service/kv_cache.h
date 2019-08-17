//
// Created by parallels on 8/17/19.
//

#ifndef PROJECT_KV_CACHE_H
#define PROJECT_KV_CACHE_H

class KVCache {
    char * buffer = nullptr;

    KVCache() {

    }

    ~KVCache() {
        delete buffer;
    }
};
#endif //PROJECT_KV_CACHE_H
