#include "rpc_process.h"

#include <unistd.h>
#include <thread>
#include "utils.h"

bool RpcProcess::Insert(int& threadId, char * buf, int len, DoneCbFunc cb) {
    if (buf == nullptr || len < PACKET_HEADER_SIZE) {
        KV_LOG(ERROR) << "insert to RpcProcess failed. size: " << len;
        return false;
    }
    auto & rpc = *(Packet *)buf;

    // 校验长度
    if (rpc.len + PACKET_HEADER_SIZE != len) {
        KV_LOG(ERROR) << "expect size: " << rpc.len << " buf size: " << len - PACKET_HEADER_SIZE;
        return false;
    }

    // 校验rpc
    uint32_t sum = rpc.Sum();
    if (sum != rpc.crc) {
        KV_LOG(ERROR) << "crc error, expect: " << rpc.crc << " get: " << sum;
        return false;
    }

    // 校验通过
    switch(rpc.type) {
        case KV_OP_PUT_KV:
            processPutKV(threadId, buf, cb);
            break;

        case KV_OP_GET_V:
            processGetV(buf, cb);
            break;

        case KV_OP_RESET_K:
            processResetKeyPosition(threadId, buf, cb);
            break;

        case KV_OP_GET_K:
            processGetK(threadId, buf, cb);
            break;

        case KV_OP_RECOVER:
            processRecoverKeyPosition(threadId, buf, cb);
            break;

        case KV_OP_CLEAR:
            //TODO: clear local data
            break;

        default:
            LOG(ERROR) << "unknown rpc type: " << rpc.type;
            cb(nullptr, 0);
            break;
    }

    return true;

}

bool RpcProcess::Run(const char * dir, bool clear) {

    kv_engines.Init(dir);
    /*
     *     if (clear) {
     *         kv_engines.clear();
     *         kv_engines.Init(dir_);
     *     }
     *  */
} 

void RpcProcess::Stop() { 
    kv_engines.Close();
    run_ = false; 
    sleep(1);
}

void RpcProcess::processPutKV(int& threadId, char * buf, DoneCbFunc cb) {
    // buf解析为packet
    auto & req = *(Packet *) buf;

    // 检查长度
    if (req.len != KEY_SIZE + VALUE_SIZE) {
        KV_LOG(ERROR) << "kv size error: " << req.len;
        return;
    }

    // 从buf构造kvstring
    KVString key;
    KVString val;
    char * key_buf = new char [KEY_SIZE];
    char * val_buf = new char [VALUE_SIZE];
    memcpy(key_buf, req.buf, KEY_SIZE);
    memcpy(val_buf, req.buf + KEY_SIZE, VALUE_SIZE);
    key.Reset(key_buf, KEY_SIZE);
    val.Reset(val_buf, KEY_SIZE);

    // 调用kvengines添加kv
    auto offset = kv_engines.putKV(key, val, threadId);

    // 在ret_buf这个数组中构建packet
    int    ret_len = PACKET_HEADER_SIZE + sizeof(offset);
    char * ret_buf = new char [ret_len];
    auto & reply = *(Packet *)ret_buf;
    memcpy(reply.buf, (char *)&offset, sizeof(offset));
    reply.len   = sizeof(offset);
    reply.sn    = req.sn;
    reply.type  = req.type;
    reply.crc   = reply.Sum();
    // callback发送pos
    cb(ret_buf, ret_len);

    delete [] ret_buf;
}

void RpcProcess::processGetV(char * buf, DoneCbFunc cb) {
    auto & req = *(Packet *)buf;

    // 请求只有一个offset参数
    if (req.len < sizeof(int32_t) ) {
        KV_LOG(ERROR) << "value index size error: " << req.len;
        return;
    }

    uint32_t compress = *(uint32_t *)req.buf;
    int threadId = (compress & 0xF0000000) >> 28;
    int offset = (compress & 0x0FFFFFFF);

    KVString val;
    kv_engines.getV(val, offset, threadId);

    int val_size = val.Size();

    int    ret_len = PACKET_HEADER_SIZE + val_size;
    char * ret_buf = new char[ret_len];
    auto & reply = *(Packet *)ret_buf;

    memcpy(reply.buf, val.Buf(), val_size);

    reply.len   = val_size;
    reply.sn    = req.sn;
    reply.type  = req.type;
    reply.crc   = reply.Sum();

    cb(ret_buf, ret_len);

    delete [] ret_buf;
}

void RpcProcess::processResetKeyPosition(int& threadId, char * buf, DoneCbFunc cb) {
    auto & req = *(Packet *)buf;

    kv_engines.resetKeyPosition(threadId);

    int    ret_len = PACKET_HEADER_SIZE;
    char * ret_buf = new char[ret_len];
    auto & reply = *(Packet *)ret_buf;

    reply.len   = 0;
    reply.sn    = req.sn;
    reply.type  = req.type;
    reply.crc   = reply.Sum();

    cb(ret_buf, ret_len);

    delete [] ret_buf;
}

void RpcProcess::processGetK(int& threadId, char * buf, DoneCbFunc cb) {
    auto & req = *(Packet *)buf;

    KVString key;
    bool has_key = kv_engines.getK(key, threadId);

    int key_size = key.Size();

    int    ret_len = PACKET_HEADER_SIZE + key_size;
    char * ret_buf = new char[ret_len];
    auto & reply = *(Packet *)ret_buf;

    memcpy(reply.buf, key.Buf(), key_size);

    reply.len   = key_size;
    if (!has_key) reply.len = 0;
    reply.sn    = req.sn;
    reply.type  = req.type;
    reply.crc   = reply.Sum();

    cb(ret_buf, ret_len);

    delete [] ret_buf;
}

void RpcProcess::processRecoverKeyPosition(int& threadId, char * buf, DoneCbFunc cb) {
    auto & req = *(Packet *)buf;

    if (req.len < sizeof(int32_t) ) {
        KV_LOG(ERROR) << "recover key position sum error: " << req.len;
        return;
    }

    auto sum = *(uint32_t *)req.buf;

    kv_engines.recoverKeyPosition(sum, threadId);

    int    ret_len = PACKET_HEADER_SIZE;
    char * ret_buf = new char[ret_len];
    auto & reply = *(Packet *)ret_buf;

    reply.len   = 0;
    reply.sn    = req.sn;
    reply.type  = req.type;
    reply.crc   = reply.Sum();

    cb(ret_buf, ret_len);

    delete [] ret_buf;
}
