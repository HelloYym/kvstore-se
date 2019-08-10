#include "tcp_server.h"
#include <unistd.h>
#include <thread>
#include <memory>
#include <chrono>
#include <condition_variable>

//#include "nanomsg/nn.h"
//#include "nanomsg/tcp.h"
//#include "nanomsg/reqrep.h"
#include<sys/types.h>
#include<sys/socket.h>
#include<netinet/in.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>

//#define NN_LOG(level, msg) KV_LOG(level) << msg << " failed. error: " << nn_strerror(nn_errno())


TcpServer::TcpServer() {
}

TcpServer::~TcpServer() {
    stopAll();
}

int TcpServer::Run(const char * host, int port, int threadId, std::shared_ptr<RpcProcess> rpc_process) {
    auto & inst = getInst();
    // 启动一个新端口
    int ssfd = inst.start(host, port);

    if (ssfd != -1) {
        // 启动一个线程监听新端口
        std::thread recv(&TcpServer::processRecv, ssfd, threadId, rpc_process);
        recv.detach();
    }

    return ssfd;
}


void TcpServer::StopAll() {
    printf("STOP ALL...\n");
    getInst().stopAll();
    //    sleep(1);
}

TcpServer & TcpServer::getInst() {
    static TcpServer server;
    return server;
}

int TcpServer::start(const char * host, int port) {
    // 每次调用start启动一个端口
    //    int fd = nn_socket(AF_SP, NN_REP);
    //    if (fd < 0) {
    //        NN_LOG(ERROR, "nn_socket");
    //        return -1;
    //    }
    //
    //    if (nn_bind(fd, url) < 0) {
    //        NN_LOG(ERROR, "nn_bind with fd: " << fd);
    //        nn_close(fd);
    //        return -1;
    //    }

    struct sockaddr_in servaddr;
    int ssfd;

    memset(&servaddr, 0, sizeof(servaddr));
    if( (ssfd = socket(AF_INET, SOCK_STREAM, 0)) == -1 ){
        printf("create ssfd error\n");
    }

    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = inet_addr(host);
    servaddr.sin_port = htons(port);

    if(bind(ssfd, (struct sockaddr*)&servaddr, sizeof(servaddr)) == -1){
        printf("bind socket error\n");
    }
    if(listen(ssfd, 10) == -1){
        printf("listen socket error\n");
    }

    return ssfd;
}


void TcpServer::stopAll() {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto & fd : fds_) {
        //        nn_close(fd);
    }
    fds_.clear();
}

void TcpServer::processRecv(int ssfd, int threadId, std::shared_ptr<RpcProcess> process) {
    if (ssfd == -1 || process == nullptr) {
        return ;
    }

    int sfd;
    while(1) {

        printf("======waiting for client's request======\n");

        //阻塞直到有客户端连接，不然多浪费CPU资源。
        if ((sfd = accept(ssfd, (struct sockaddr *) NULL,  NULL)) == -1) {
            printf("accept socket error\n");
            break;
        } else {
            printf("accept socket successful\n");
        }
        
        int on = 1;
        if (setsockopt(sfd, IPPROTO_TCP, TCP_NODELAY, (void *)&on, sizeof(on)) == 0)
        {
//            printf("TCP_NODELAY\n");
        }

        // 接收缓冲区
        int nRecvBuf=32*1024;//设置为32K
        setsockopt(sfd,SOL_SOCKET,SO_RCVBUF,(const char*)&nRecvBuf,sizeof(int));
        //发送缓冲区
        int nSendBuf=0;//设置为32K
        setsockopt(sfd,SOL_SOCKET,SO_SNDBUF,(const char*)&nSendBuf,sizeof(int));

        // bool bSet = true;
        // setsockopt(sfd,SOL_SOCKET,SO_KEEPALIVE,(void*)&bSet,sizeof(bSet));

        // int keepIdle = 1000;
        // int keepInterval = 5;
        // int keepCount = 3;
        //
        // setsockopt(sfd, SOL_TCP, TCP_KEEPIDLE, (void *)&keepIdle, sizeof(keepIdle));
        // setsockopt(sfd, SOL_TCP,TCP_KEEPINTVL, (void *)&keepInterval, sizeof(keepInterval));
        // setsockopt(sfd,SOL_TCP, TCP_KEEPCNT, (void *)&keepCount, sizeof(keepCount));
        //

        std::function<void (const char *, int)> cb =
            [&] (const char * buf, int len) {
                send(sfd, buf, len, 0);
            };

        char * recv_buf = new char[MAX_PACKET_SIZE];
        char * send_buf = new char[MAX_PACKET_SIZE];

        while (1) {
            int rc = recvPack(sfd, recv_buf);
            //        printf("FD : %d, RECV: %d, %d\n",sfd, ((Packet *) recv_buf)->type, ((Packet *) recv_buf)->len);
            if (rc < 0) {
                break;
            }

            process->Insert(threadId, (Packet *) recv_buf, rc, cb, send_buf);
        }

    }

    printf("thread close\n");
}

int TcpServer::recvPack(int fd, char * buf) {
    auto bytes = recv(fd, buf, MAX_PACKET_SIZE, 0);
    if (bytes <= 0) {
        return -1;
    }
    while (bytes < sizeof(int)) {
        auto b = recv(fd, buf + bytes, MAX_PACKET_SIZE, 0);
        if (b <= 0) {
            return -1;
        }
        bytes += b;
    }
    int total = *(int *) buf;
    while (total != bytes) {
        auto b = recv(fd, buf + bytes, MAX_PACKET_SIZE, 0);
        if (b <= 0) {
            return -1;
        }
        bytes += b;
    }
    return total;
}
