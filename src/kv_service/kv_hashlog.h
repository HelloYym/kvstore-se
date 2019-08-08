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
    int client_ref;
    std::mutex mutex_;

    HashLog(): client_ref(0) {
        this->kvHash = new KVHash(HASH_CAPACITY);
    }

    ~HashLog() {
        delete kvHash;
    }

public:

    void client_on(){
       std::lock_guard<std::mutex> lock(mutex_);
       client_ref += 1;
    }

    void close() {
       std::lock_guard<std::mutex> lock(mutex_);
       if (--client_ref == 0) {
            delete kvHash;
            kvHash = nullptr;
       }
    }

    void put(u_int64_t &bigEndkey, uint32_t compress_id_pos) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (kvHash == nullptr) {
            kvHash = new KVHash(HASH_CAPACITY);
        }
        kvHash->put(bigEndkey, compress_id_pos);
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
