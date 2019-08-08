//
// Created by parallels on 8/7/19.
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
    KVHash * kvHash = nullptr;
//    u_int32_t nums;
    int thread = THREAD_NUM;
    std::mutex mutex_;
    HashLog() {
        this->kvHash = new KVHash(HASH_CAPACITY);
    }

    ~HashLog() {
        delete kvHash;
    }

public:

//    int size() {
//        return nums;
//    }

    void reset() {
//        std::lock_guard<std::mutex> lock(mutex_);
//        if (--thread == 0) {
            delete kvHash;
            kvHash = nullptr;
//            nums = 0;
//            thread = THREAD_NUM;
//        }

    }

    void put(u_int64_t &bigEndkey, int id) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (kvHash == nullptr) {
            kvHash = new KVHash(HASH_CAPACITY);
        }
        kvHash->put(bigEndkey, id);
    }

    int find(u_int64_t &bigEndkey) {
        if (kvHash == nullptr) {
            kvHash = new KVHash(HASH_CAPACITY);
        }
        return kvHash->getOrDefault(bigEndkey, -1);
    }

    static HashLog & getInstance() {
        static HashLog hashLog;
        return hashLog;
    }

};

#endif //PROJECT_KV_HASHLOG_H
