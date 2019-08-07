#ifndef __HUAWEI_RPC_SERVICE_H__
#define __HUAWEI_RPC_SERVICE_H__
/////////////////////////////////////////////////////////////////////////////////////////////
#include <mutex>
#include <list>
#include <memory>
#include <atomic>
#include <condition_variable>

#include "data_mgr.h"
#include "kv_string.h"
#include "store/kv_engines.h"

typedef std::function<void (char *, int)> DoneCbFunc;

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

    bool Insert(int threadId, char * buf, int len, DoneCbFunc cb);

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

    void processPutKV(int threadId, char * buf, DoneCbFunc cb);

    void processGetV(int threadId, char * buf, DoneCbFunc cb);

    void processResetKeyPosition(int threadId, char * buf, DoneCbFunc cb);

    void processGetK(int threadId, char * buf, DoneCbFunc cb);

    void processRecoverKeyPosition(int threadId, char * buf, DoneCbFunc cb);


protected:

    // 这个参数没有用
    std::atomic_bool    run_;

    KVEngines   kv_engines;

};
/////////////////////////////////////////////////////////////////////////////////////////////
#endif
