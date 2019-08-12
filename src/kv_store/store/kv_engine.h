#ifndef KV_ENGINE_H
#define KV_ENGINE_H

#include <memory>
#include <malloc.h>
#include <iostream>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/time.h>
#include <unistd.h>
#include <string>
#include <mutex>
#include <atomic>

#include "kv_file.h"
#include "kv_log.h"
#include "kv_string.h"
#include "params.h"

using namespace std;


class KVEngine {
public:
    void close() {
//        printf("Close KVEngine!\n");
        delete kvFile;
        kvFile = nullptr;
        delete kvLog;
        kvLog = nullptr;
    }

    bool init(const char * dir, int id) {
        printf("Engine init %s, %d\n", dir, id);
        string path = dir;
        std::ostringstream ss;
        ss << path << "/value-" << id;
        string filePath = ss.str();
        kvFile = new KVFile(path, id, false, VALUE_LOG_SIZE, KEY_LOG_SIZE);
        kvLog = new KVLog(kvFile->getValueFd(), kvFile->getKeyBuffer());
        return true;
    }

    void putKV(char * key, char * val) {
        kvLog->putValueKey(val, key);
    }


    // TODO: 多线程同时读一个文件 readbuf冲突
    void getV(char * val, int offset) {
        kvLog->preadValue((size_t)offset, val);
    }

    void getVZeroCopy(int sfd, int offset) {
        kvLog->preadValueZeroCopy(sfd, (off_t) offset);
    }

    void getVBatch(char * val, int offset) {
        kvLog->preadValueBatch((size_t)offset, val);
    }

    void getVBatchZeroCopy(int sfd, int offset) {
        kvLog->preadValueBatchZeroCopy(sfd, (off_t) offset);
    }

    void resetKeyPosition() {
        kvLog->resetKeyPosition();
    }

    char * getK() {
        return kvLog->getKey();
    }

    void recoverKeyPosition(int sum) {
        kvLog->recover((size_t)sum);
    }


private:
    //读缓存
//    std::unique_ptr<char> readBuffer =
//            unique_ptr<char>(static_cast<char *> (memalign((size_t) getpagesize(), VALUE_SIZE * READ_CACHE_SIZE)));
    KVFile * kvFile = nullptr;
    KVLog * kvLog = nullptr;
};
#endif
