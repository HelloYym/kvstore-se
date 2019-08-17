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
#include<sys/types.h>
#include<sys/socket.h>
#include<netinet/in.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>

typedef std::function<void (const char *, int)> DoneCbFunc;

class RpcProcess : public std::enable_shared_from_this<RpcProcess> {
public:
    RpcProcess(){
    }

    ~RpcProcess() {
        Stop();
    }

    bool Insert(int sfd, Packet * buf, char * send_buf);

    bool Run(const char * dir, bool clear);

    void Stop(); 

    std::shared_ptr<RpcProcess> GetPtr() {
        return shared_from_this();
    }

protected:
    bool process();

    void processPutKV(int sfd, Packet * buf, char * send_buf);

    void processGetV(int sfd, Packet * buf, char * send_buf);

    void processGetVRandom(int sfd, Packet * buf, char * send_buf);

    void processGetVCheck(int sfd, Packet * buf, char * send_buf);

    void processResetKeyPosition(int sfd, Packet * buf, char * send_buf);

    void processGetK(int sfd, Packet * buf, char * send_buf);

    void processRecoverKeyPosition(int sfd, Packet * buf, char * send_buf);

    void Getfilepath(const char *path, const char *filename,  char *filepath);

    bool DeleteFile(const char* path);


protected:

    KVEngines   kv_engines;

};
/////////////////////////////////////////////////////////////////////////////////////////////
#endif
