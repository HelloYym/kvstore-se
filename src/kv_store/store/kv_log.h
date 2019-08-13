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
    u_int64_t *keyBuffer;
    size_t keyBufferPosition;
    char *cacheBuffer;
    size_t cacheBufferPosition;

    //当前读缓存块是从第几个开始
    long readCacheStartNo;


    // 10M buf
    char *value_buf_head = nullptr;
    uint32_t value_buf_head_start_index;
    char *value_buf_tail = nullptr;
    uint32_t value_buf_tail_start_index;

public:
    KVLog(const int &fd, char *cacheBuffer, u_int64_t *keyBuffer) :
            fd(fd), filePosition(0),
            keyBuffer(keyBuffer), keyBufferPosition(0),
            cacheBuffer(cacheBuffer), cacheBufferPosition(0),
            readCacheStartNo(LONG_MIN) {
    }

    ~KVLog() {
        if (value_buf_head != nullptr) {
            delete[] value_buf_head;
            delete[] value_buf_tail;
        }
    }


    inline void putValueKey(const char *value, const char *key) {
        //写入key
        *(keyBuffer + keyBufferPosition) = *((u_int64_t *) key);
        keyBufferPosition++;
        //写入写缓存块
        memcpy(cacheBuffer + (cacheBufferPosition * VALUE_SIZE), value, VALUE_SIZE);
        cacheBufferPosition++;
        //value缓存达到缓存块大小就刷盘
        if (cacheBufferPosition == PAGE_PER_BLOCK) {
            pwrite(this->fd, cacheBuffer, BLOCK_SIZE, filePosition);
            filePosition += BLOCK_SIZE;
            cacheBufferPosition = 0;
        }
    }

    inline void preadValue(size_t pos, char *value, char *buffer) {
        int indexInReadBuffer = readValue(pos, buffer);
        memcpy(value, buffer + indexInReadBuffer * VALUE_SIZE, VALUE_SIZE);
    }

    //返回值是要读的值是读缓存块的第几个
    inline int readValue(size_t index, char *value) {
        //如果要读的value在mmap中
        if (this->filePosition <= index * VALUE_SIZE) {
            auto pos = index % PAGE_PER_BLOCK;
            memcpy(value, cacheBuffer + (pos * VALUE_SIZE), VALUE_SIZE);
            readCacheStartNo = LONG_MIN;
            return 0;
        }
            //这里就是读valueLog
        else {
            //如果当前要读的命中读缓存块
            size_t now = index / READ_CACHE_SIZE;
            if (now == readCacheStartNo / READ_CACHE_SIZE) {
                return index % READ_CACHE_SIZE;
            }
                //如果没命中
            else {
                readCacheStartNo = now * READ_CACHE_SIZE;
                size_t cap = VALUE_SIZE * READ_CACHE_SIZE;
                if (keyBufferPosition - readCacheStartNo < READ_CACHE_SIZE) {
                    cap = (keyBufferPosition - readCacheStartNo) * VALUE_SIZE;
                }
                pread(this->fd, value, cap, (readCacheStartNo * VALUE_SIZE));
                return index % READ_CACHE_SIZE;
            }
        }
    }

    // TODO: 第三阶段随机读  因为dio要用对齐的地址才可以读的
    inline void preadValueRandom(uint32_t index, char *value) {

        if (cacheBufferPosition > 0) {
            printf("write mmap to file\n");
            pwrite(this->fd, cacheBuffer, cacheBufferPosition * VALUE_SIZE, filePosition);
            filePosition += BLOCK_SIZE;
            cacheBufferPosition = 0;
        }
        
        if (value_buf_head == nullptr) {
            printf("create 2 buffer\n");
            value_buf_head = static_cast<char *> (memalign((size_t) getpagesize(), READ_CACHE_SIZE * VALUE_SIZE));
            value_buf_head_start_index = UINT32_MAX;
            value_buf_tail = static_cast<char *> (memalign((size_t) getpagesize(), READ_CACHE_SIZE * VALUE_SIZE));
            value_buf_tail_start_index = UINT32_MAX;
        }

        uint32_t current_buf_no = index / READ_CACHE_SIZE;

        if (current_buf_no != value_buf_head_start_index / READ_CACHE_SIZE &&
            current_buf_no != value_buf_tail_start_index / READ_CACHE_SIZE) {

            if (current_buf_no > value_buf_tail_start_index / READ_CACHE_SIZE) {

                printf("warning!!! value read back: %d %d %d\n", current_buf_no, value_buf_tail_start_index / READ_CACHE_SIZE,
                       value_buf_head_start_index / READ_CACHE_SIZE);

                pread(this->fd, value, VALUE_SIZE, (index * VALUE_SIZE));
                return;
            }

            // 交换两个buf
            char *tmp_value_buf = value_buf_tail;
            value_buf_tail = value_buf_head;
            value_buf_head = tmp_value_buf;

            // buf编号直接推进
            value_buf_tail_start_index = value_buf_head_start_index;
            value_buf_head_start_index = current_buf_no * READ_CACHE_SIZE;

            pread(this->fd, value_buf_head, VALUE_SIZE * READ_CACHE_SIZE, (value_buf_head_start_index * VALUE_SIZE));

        }

        char *current_value_buf;
        if (current_buf_no == value_buf_head_start_index / READ_CACHE_SIZE)
            current_value_buf = value_buf_head;
        else if (current_buf_no == value_buf_tail_start_index / READ_CACHE_SIZE)
            current_value_buf = value_buf_tail;
        else {
            current_value_buf = nullptr;
            printf("error!!! value buf no error\n");
        }

        uint32_t value_buf_offset = index % READ_CACHE_SIZE;
        memcpy(value, current_value_buf + value_buf_offset * VALUE_SIZE, VALUE_SIZE);
    }

    //再次open时恢复写的位置
    void recover(size_t sum) {
        this->keyBufferPosition = sum;
        auto cacheSize = sum % PAGE_PER_BLOCK;
        if (cacheSize == 0) {
            this->filePosition = sum * VALUE_SIZE;
        } else {
            this->filePosition = (sum - cacheSize) * VALUE_SIZE;
            this->cacheBufferPosition = cacheSize;
        }
    }

    char *getKey() {
        char *key = (char *) (keyBuffer + keyBufferPosition);
        keyBufferPosition += KEY_NUM_TCP;
        return key;
    }

    inline void resetKeyPosition() {
        keyBufferPosition = 0;
    }
};


#endif //KV_LOG_H
