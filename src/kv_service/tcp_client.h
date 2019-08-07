#ifndef __HUAWEI_TCP_SERER__
#define __HUAWEI_TCP_SERER__
///////////////////////////////////////////////////////////////////////////////////////////////
#include <mutex>
#include <memory>
#include <vector>

class TcpClient {
public:
    TcpClient();
    ~TcpClient();

    void close();

    char *send(char * buf, int len);

    int connect(const char * url);

private:
    std::mutex mutex_;
    int fd;

};
///////////////////////////////////////////////////////////////////////////////////////////////
#endif

