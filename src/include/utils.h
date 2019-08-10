#ifndef __HUAWEI_UTILS_H__
#define __HUAWEI_UTILS_H__
//////////////////////////////////////////////////////////////////////////////////////////////////
#include "easylogging++.h"
#include "params.h"
#include <cassert>

#define KV_LOG(level) LOG(level) << "[" << __FUNCTION__ << ":" << __LINE__ << "] "

const uint8_t KV_OP_PUT_KV  = 1;
const uint8_t KV_OP_GET_V   = 2;
const uint8_t KV_OP_RESET_K = 3;
const uint8_t KV_OP_GET_K   = 4;
const uint8_t KV_OP_RECOVER = 5;

#define KV_OP_SUCCESS 'S'
#define KV_OP_FAILED 'F'


#pragma pack(push)
#pragma pack(1)

struct Packet {
    u_int32_t len = 0;
    uint8_t type= 0;
    char buf[0];
};
#pragma pack(pop)

//////////////////////////////////////////////////////////////////////////////////////////////////
#endif
