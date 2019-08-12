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
#include<sys/types.h>
#include<sys/socket.h>
#include<netinet/in.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <sys/sendfile.h>
class KVLog {
private:
    //对应ValueFile
    int fd;
    size_t filePosition;

    //对应KeyFile
    uint64_t *keyBuffer;
    size_t keyBufferPosition;

public:
    KVLog(const int &fd, uint64_t *keyBuffer) :
            fd(fd), filePosition(0),
            keyBuffer(keyBuffer), keyBufferPosition(0) {
    }

    ~KVLog() = default;

    size_t size() {
        return filePosition;
    }

    inline void putValueKey(const char *value, const char *key) {
        //写入key
        *(keyBuffer + keyBufferPosition) = *((uint64_t *) key);
        keyBufferPosition++;

        pwrite(this->fd, value, VALUE_SIZE, filePosition);
        filePosition += VALUE_SIZE;
    }

    inline void preadValue(size_t pos, char *value) {
        pread(this->fd, value, VALUE_SIZE, (pos * VALUE_SIZE));
    }

    inline void preadValueZeroCopy(int sfd, off_t offset) {
        off_t pos = offset * VALUE_SIZE;
        sendfile(sfd, fd, &pos, VALUE_SIZE);
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
