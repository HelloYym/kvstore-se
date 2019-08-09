//
// Created by parallels on 8/8/19.
//

#ifndef PROJECT_KV_HASHLOG_LF_H
#define PROJECT_KV_HASHLOG_LF_H
#include <vector>
#include <iostream>
#include <algorithm>
#include <mutex>

#include "kv_hash.h"
#include "params.h"

using namespace std;

class HashLogLF {

private:
    KVHash ** kvHash = nullptr;
    int client_ref;
    int hash_finsh;
    std::mutex mutex_[HASH_NUM];
    std::mutex mutex1;

    HashLogLF(): client_ref(0), hash_finsh(0) {
        init();
    }

    ~HashLogLF() {
        if (kvHash != nullptr) {
            for (int i = 0; i < HASH_NUM; i++) {
                delete kvHash[i];
            }
            delete[] kvHash;
        }
    }

public:

    void init() {
        this->kvHash = (KVHash **) malloc(HASH_NUM * sizeof(HashLogLF *));
        for (int i = 0; i < HASH_NUM; i++) {
            kvHash[i] = new KVHash(HASH_CAPACITY);
        }
    }


    void client_on() {
        std::lock_guard<std::mutex> lock(mutex1);
        if (kvHash == nullptr) {
            init();
        }
        client_ref += 1;
        hash_finsh += 1;
    }

    bool hash_has_finish_1() {
        std::lock_guard<std::mutex> lock(mutex1);
        hash_finsh--;
    }
    bool hash_has_finish() {
        std::lock_guard<std::mutex> lock(mutex1);
        return hash_finsh == 0;
    }

    void close() {
        std::lock_guard<std::mutex> lock(mutex1);
        if (--client_ref == 0) {
            for (int i = 0; i < HASH_NUM; i++) {
                delete kvHash[i];
            }
            delete [] kvHash;
            kvHash = nullptr;
        }
    }

    void put(u_int64_t &bigEndkey, uint32_t compress_id_pos) {
        auto slot = bigEndkey & (HASH_NUM - 1);
        std::lock_guard<std::mutex> lock(mutex_[slot]);
        kvHash[slot]->put(bigEndkey, compress_id_pos);
    }

    int find(u_int64_t &bigEndkey) {
        auto slot = bigEndkey & (HASH_NUM - 1);
        return kvHash[slot]->getOrDefault(bigEndkey, -1);
    }

    static HashLogLF & getInstance() {
        static HashLogLF hashLog;
        return hashLog;
    }

};
#endif //PROJECT_KV_HASHLOG_LF_H
