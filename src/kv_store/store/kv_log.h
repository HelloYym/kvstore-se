#ifndef KV_LOG_H
#define KV_LOG_H

#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sstream>
#include <iostream>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <mutex>
#include "params.h"
#include <limits.h>
#include "kv_string.h"

class KVLog {
private:
    //对应ValueFile
    int fd;
    off_t filePosition;

    //对应KeyFile
    u_int64_t *keyBuffer;
    size_t keyBufferPosition;

public:
    KVLog(const int &fd, u_int64_t *keyBuffer) :
            fd(fd), filePosition(0),
            keyBuffer(keyBuffer), keyBufferPosition(0) {
    }

    ~KVLog() = default;

    off_t size() {
        return filePosition;
    }

    inline void putValueKey(const char *value, const char *key) {
        //写入key
        *(keyBuffer + keyBufferPosition) = *((u_int64_t *) key);
        keyBufferPosition++;

        //写入写缓存块
        pwrite(this->fd, value, VALUE_SIZE, filePosition);
        filePosition += VALUE_SIZE;
    }

    inline void preadValue(size_t pos, char *value) {
        pread(this->fd, value, VALUE_SIZE, (pos * VALUE_SIZE));
    }

    //再次open时恢复写的位置
    void recover(size_t sum) {
        this->keyBufferPosition = sum;
        this->filePosition = sum * VALUE_SIZE;
    }

    char * getKey() {
        char * key = (char *) (keyBuffer + keyBufferPosition);
        keyBufferPosition += KEY_NUM_TCP;
        return key;
    }

    inline void resetKeyPosition() {
        keyBufferPosition = 0;
    }
};


#endif //KV_LOG_H
