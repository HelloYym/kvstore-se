#ifndef __HUAWEI_UTILS_H__
#define __HUAWEI_UTILS_H__
//////////////////////////////////////////////////////////////////////////////////////////////////
#include "easylogging++.h"
#include "params.h"
#include <cassert>

#define KV_LOG(level) LOG(level) << "[" << __FUNCTION__ << ":" << __LINE__ << "] "

const uint32_t KV_OP_PUT_KV  = 1;
const uint32_t KV_OP_GET_V   = 2;
const uint32_t KV_OP_RESET_K = 3;
const uint32_t KV_OP_GET_K   = 4;
const uint32_t KV_OP_RECOVER = 5;

#define KV_OP_SUCCESS 'S'
#define KV_OP_FAILED 'F'


#pragma pack(push)
#pragma pack(4)

struct Packet {
    uint32_t len = 0;
    uint32_t type= 0;
    char buf[0];
};
#pragma pack(pop)

//////////////////////////////////////////////////////////////////////////////////////////////////
#endif
