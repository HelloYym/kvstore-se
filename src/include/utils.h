#ifndef __HUAWEI_UTILS_H__
#define __HUAWEI_UTILS_H__
//////////////////////////////////////////////////////////////////////////////////////////////////
#include "easylogging++.h"

#define KV_LOG(level) LOG(level) << "[" << __FUNCTION__ << ":" << __LINE__ << "] "

const int KV_OP_PUT_KV  = 1;
const int KV_OP_GET_V   = 2;
const int KV_OP_RESET_K = 3;
const int KV_OP_GET_K   = 4;
const int KV_OP_RECOVER = 5;
const int KV_OP_CLEAR   = 6;

const int NUM_PER_SLOT = 4 * 1024 * 1024;

const int KEY_SIZE = 8;
const size_t VALUE_SIZE = 4096;
const size_t VALUE_LOG_SIZE = NUM_PER_SLOT * VALUE_SIZE;
const size_t KEY_LOG_SIZE = NUM_PER_SLOT * KEY_SIZE;

//读缓存大小（多少块4KB）
const int READ_CACHE_SIZE = 4096 * 4;
//写缓存大小（多少块4KB）
const int PAGE_PER_BLOCK = 256;
//写缓存大小
const size_t BLOCK_SIZE = PAGE_PER_BLOCK * VALUE_SIZE;

//sortlog的容量
const int SORT_LOG_SIZE = NUM_PER_SLOT;

//hash的容量
const int HASH_CAPACITY = 2 * NUM_PER_SLOT;

#pragma pack(push)
#pragma pack(1)
const int PACKET_HEADER_SIZE = sizeof(int32_t) * 4;
struct Packet {
    uint32_t len = 0;
    uint32_t crc = 0;
    uint32_t sn  = 0;
    uint32_t type= 0;
    char buf[0];

    uint32_t Sum() {
        uint32_t cnt = 0;
        for (int i = 0; i < len; i ++) {
            cnt += (uint8_t)buf[i];
        }
        return cnt + len + sn + type;
    }
};
#pragma pack(pop)

//////////////////////////////////////////////////////////////////////////////////////////////////
#endif
