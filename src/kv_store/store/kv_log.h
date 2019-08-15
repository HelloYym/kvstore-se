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
#include <malloc.h>
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

    // 顺序读 buffer
    char *value_buf = nullptr;
    uint32_t value_buf_start_index;

    // 随机读 buffer
    char *value_buf_head = nullptr;
    uint32_t value_buf_head_start_index;
    char *value_buf_tail = nullptr;
    uint32_t value_buf_tail_start_index;

    // 预读 buffer
    char *value_buf_pre_read = nullptr;
    uint32_t value_buf_pre_read_start_index;
    uint32_t value_buf_pre_read_current_index;

public:
    KVLog(const int &fd, char *cacheBuffer, u_int64_t *keyBuffer) :
            fd(fd), filePosition(0),
            keyBuffer(keyBuffer), keyBufferPosition(0),
            cacheBuffer(cacheBuffer), cacheBufferPosition(0) {
    }

    ~KVLog() {
        if (value_buf_head != nullptr)
            delete[] value_buf_head;

        if (value_buf_tail != nullptr)
            delete[] value_buf_tail;

        if (value_buf != nullptr)
            delete[] value_buf;

        if (value_buf_pre_read != nullptr)
            delete[] value_buf_pre_read;
    }


    void putValueKey(const char *value, const char *key) {
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

    void pre_read_value() {
        if (value_buf_pre_read == nullptr || value_buf_pre_read_start_index == UINT32_MAX)
            return;

        if (value_buf_pre_read_current_index - value_buf_pre_read_start_index == READ_CACHE_SIZE)
            return;

        uint32_t value_buf_offset = value_buf_pre_read_current_index % READ_CACHE_SIZE;

        pread(this->fd, value_buf_pre_read + value_buf_offset * VALUE_SIZE, VALUE_SIZE * PRE_READ_NUM,
              (value_buf_pre_read_current_index * VALUE_SIZE));

        value_buf_pre_read_current_index += PRE_READ_NUM;
    }

    void preadValue(uint32_t index, char *value) {

        //如果要读的value在mmap中
        if (this->filePosition <= index * VALUE_SIZE) {
            auto pos = index % PAGE_PER_BLOCK;
            memcpy(value, cacheBuffer + (pos * VALUE_SIZE), VALUE_SIZE);
            return;
        }

        if (value_buf == nullptr) {
            value_buf = static_cast<char *> (memalign((size_t) getpagesize(), VALUE_SIZE * READ_CACHE_SIZE));
            value_buf_start_index = UINT32_MAX;

            value_buf_pre_read = static_cast<char *> (memalign((size_t) getpagesize(), VALUE_SIZE * READ_CACHE_SIZE));
            value_buf_pre_read_start_index = UINT32_MAX;
            value_buf_pre_read_current_index = UINT32_MAX;
        }

        uint32_t current_buf_no = index / READ_CACHE_SIZE;

        if (current_buf_no != value_buf_start_index / READ_CACHE_SIZE) {
            value_buf_start_index = current_buf_no * READ_CACHE_SIZE;

            // 预读 buffer 已经满了
            if (value_buf_pre_read_start_index == value_buf_start_index &&
                value_buf_pre_read_current_index - value_buf_pre_read_start_index == READ_CACHE_SIZE) {

                // 交换预读的buffer
                char *tmp_value_buf = value_buf;
                value_buf = value_buf_pre_read;
                value_buf_pre_read = tmp_value_buf;
            } else {
                pread(this->fd, value_buf, VALUE_SIZE * READ_CACHE_SIZE, (value_buf_start_index * VALUE_SIZE));
            }

            value_buf_pre_read_start_index = value_buf_start_index + READ_CACHE_SIZE;
            value_buf_pre_read_current_index = value_buf_pre_read_start_index;
        }

        uint32_t value_buf_offset = index % READ_CACHE_SIZE;
        memcpy(value, value_buf + value_buf_offset * VALUE_SIZE, VALUE_SIZE);

    }


    // TODO: 第三阶段随机读  因为dio要用对齐的地址才可以读的
    void preadValueRandom(uint32_t index, char *value) {

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

            value_buf_pre_read_start_index = UINT32_MAX;
            value_buf_pre_read_current_index = UINT32_MAX;
        }

        uint32_t current_buf_no = index / READ_CACHE_SIZE;

        if (current_buf_no != value_buf_head_start_index / READ_CACHE_SIZE &&
            current_buf_no != value_buf_tail_start_index / READ_CACHE_SIZE) {

            if (current_buf_no > value_buf_tail_start_index / READ_CACHE_SIZE) {
                if (value_buf_tail_start_index == UINT32_MAX) {
                    value_buf_tail_start_index = current_buf_no * READ_CACHE_SIZE;
                    pread(this->fd, value_buf_tail, VALUE_SIZE * READ_CACHE_SIZE,
                          (value_buf_tail_start_index * VALUE_SIZE));
                } else {

                    printf("warning!!! value read back: %d %d %d\n", current_buf_no,
                           value_buf_tail_start_index / READ_CACHE_SIZE,
                           value_buf_head_start_index / READ_CACHE_SIZE);

                    pread(this->fd, value, VALUE_SIZE, (index * VALUE_SIZE));
                    return;
                }
            } else {
                // 交换两个buf
                char *tmp_value_buf = value_buf_tail;
                value_buf_tail = value_buf_head;
                value_buf_head = tmp_value_buf;

                // buf编号直接推进
                value_buf_tail_start_index = value_buf_head_start_index;
                value_buf_head_start_index = current_buf_no * READ_CACHE_SIZE;


                // 预读 buffer 已经满了
                if (value_buf_pre_read_start_index == value_buf_head_start_index &&
                    value_buf_pre_read_current_index - value_buf_pre_read_start_index == READ_CACHE_SIZE) {

                    // 交换预读的buffer
                    char *tmp_value_buf = value_buf_head;
                    value_buf_head = value_buf_pre_read;
                    value_buf_pre_read = tmp_value_buf;
                } else {
                    pread(this->fd, value_buf_head, VALUE_SIZE * READ_CACHE_SIZE, (value_buf_head_start_index * VALUE_SIZE));
                }

                if (value_buf_start_index < READ_CACHE_SIZE) {
                    value_buf_pre_read_start_index = UINT32_MAX;
                    value_buf_pre_read_current_index = UINT32_MAX;
                } else {
                    value_buf_pre_read_start_index = value_buf_start_index - READ_CACHE_SIZE;
                    value_buf_pre_read_current_index = value_buf_pre_read_start_index;
                }
            }

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
