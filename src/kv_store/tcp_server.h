#ifndef __HUAWEI_TCP_SERER__
#define __HUAWEI_TCP_SERER__
///////////////////////////////////////////////////////////////////////////////////////////////
#include <mutex>
#include <memory>
#include <vector>
#include "rpc_process.h"
#include "utils.h"

class TcpServer {
public:
    TcpServer();
    ~TcpServer();

    static int Run(const char * host, int port, int threadId, std::shared_ptr<RpcProcess> process);

    static void StopAll();
protected:

    static TcpServer & getInst();

    int start(const char * host, int port);

    void stopAll();

    static int recvPack(int fd, char * buf);

    static void processRecv(int sfd, int threadId, std::shared_ptr<RpcProcess> process);

protected:
    std::mutex mutex_;

    std::vector<int> fds_;
};
///////////////////////////////////////////////////////////////////////////////////////////////
#endif

