#ifndef __HUAWEI_RPC_SERVICE_H__
#define __HUAWEI_RPC_SERVICE_H__
/////////////////////////////////////////////////////////////////////////////////////////////
#include <mutex>
#include <list>
#include <memory>
#include <atomic>
#include <condition_variable>
#include <functional>

#include "kv_string.h"
#include "store/kv_engines.h"
#include "utils.h"
#include "params.h"

typedef std::function<void (const char *, int)> DoneCbFunc;

struct PacketInfo {
    char * buf      = nullptr;
    DoneCbFunc cb   = nullptr;

    PacketInfo(char * buf, DoneCbFunc cb)
    : buf(buf), cb(cb) {
    }
};


// thread_id 可以在这里面维护
// 每个rpcprocess对应一个kvengine
class RpcProcess : public std::enable_shared_from_this<RpcProcess> {
public:
    RpcProcess(): run_(false) {
    }

    ~RpcProcess() {
        Stop();
    }

    bool Insert(int& threadId, Packet * buf, int len, DoneCbFunc cb, char * send_buf);

    bool Run(const char * dir, bool clear);

    bool IsRun() {
        return run_;
    }

    void Stop(); 

    std::shared_ptr<RpcProcess> GetPtr() {
        return shared_from_this();
    }

protected:
    bool process();

    void processPutKV(int& threadId, Packet * buf, DoneCbFunc cb, char * send_buf);

    void processGetV(Packet * buf, DoneCbFunc cb, char * send_buf);

    void processResetKeyPosition(int& threadId, Packet * buf, DoneCbFunc cb, char * send_buf);

    void processGetK(int& threadId, Packet * buf, DoneCbFunc cb, char * send_buf);

    void processRecoverKeyPosition(int& threadId, Packet * buf, DoneCbFunc cb, char * send_buf);

    void Getfilepath(const char *path, const char *filename,  char *filepath);

    bool DeleteFile(const char* path);


protected:

    // 这个参数没有用
    std::atomic_bool    run_;

    KVEngines   kv_engines;

};
/////////////////////////////////////////////////////////////////////////////////////////////
#endif
