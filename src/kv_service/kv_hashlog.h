//
// Created by parallels on 8/8/19.
//

#ifndef PROJECT_KV_HASHLOG_H
#define PROJECT_KV_HASHLOG_H
#include <vector>
#include <iostream>
#include <algorithm>
#include <mutex>

#include "kv_hash.h"
#include "params.h"

using namespace std;

class HashLog {

private:
    KVHash ** kvHash = nullptr;
    int client_ref;
    int hash_finsh;
    std::mutex mutex_[HASH_NUM];
    std::mutex mutex1;

    HashLog(): client_ref(0), hash_finsh(0) {
        init();
    }

    ~HashLog() {
        if (kvHash != nullptr) {
            for (int i = 0; i < HASH_NUM; i++) {
                delete kvHash[i];
            }
            delete[] kvHash;
        }
    }

public:

    void init() {
        this->kvHash = (KVHash **) malloc(HASH_NUM * sizeof(HashLog *));
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

    void hash_has_finish_1() {
        std::lock_guard<std::mutex> lock(mutex1);
        hash_finsh--;
        if (hash_finsh == 0) {
            for (int i = 0; i < HASH_NUM; i++) {
                printf("HASH : %d, %d\n", i, kvHash[i]->size());
            }
        }
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

    bool find(u_int64_t &bigEndkey, u_int32_t &val) {
        auto slot = bigEndkey & (HASH_NUM - 1);
        return kvHash[slot]->get(bigEndkey, val);
    }

    static HashLog & getInstance() {
        static HashLog hashLog;
        return hashLog;
    }

};
#endif //PROJECT_KV_HASHLOG_H
