#include "tcp_server.h"
#include <unistd.h>
#include <thread>
#include <memory>
#include <chrono>
#include <condition_variable>

#include "nanomsg/nn.h"
#include "nanomsg/reqrep.h"

#define NN_LOG(level, msg) KV_LOG(level) << msg << " failed. error: " << nn_strerror(nn_errno())


TcpServer::TcpServer() {
}

TcpServer::~TcpServer() {
    stopAll();
}

int TcpServer::Run(const char * url, int threadId, std::shared_ptr<RpcProcess> rpc_process) {
    auto & inst = getInst();
    // 启动一个新端口
    int fd = inst.start(url);

    if (fd != -1) {
        // 启动一个线程监听新端口
        std::thread recv(&TcpServer::processRecv, fd, threadId, rpc_process);
        recv.detach();
    }

    return fd;
}

void TcpServer::Stop(int fd) {
    getInst().stop(fd);
}

void TcpServer::StopAll() {
    getInst().stopAll();
    sleep(1);
}

TcpServer & TcpServer::getInst() {
    static TcpServer server;
    return server;
}

int TcpServer::start(const char * url) {
    // 每次调用start启动一个端口
    int fd = nn_socket(AF_SP, NN_REP);
    if (fd < 0) {
        NN_LOG(ERROR, "nn_socket");
        return -1;
    }

    if (nn_bind(fd, url) < 0) {
        NN_LOG(ERROR, "nn_bind with fd: " << fd);
        nn_close(fd);
        return -1;
    }

    mutex_.lock();
    fds_.emplace_back(fd);
    mutex_.unlock();

    KV_LOG(INFO) << "bind on " << url << " success with fd: " << fd;

    return fd;
}

void TcpServer::stop(int fd) {
    if (fd < 0)  {
        KV_LOG(ERROR) << "error with fd: " << fd;
        return;
    }

    mutex_.lock();
    auto it = std::find(fds_.begin(), fds_.end(), fd);
    if (it != fds_.end()) {
        mutex_.unlock();
        KV_LOG(ERROR) << "stop with fd: " << fd;
        return ;
    }
    fds_.erase(it);
    mutex_.unlock();

    nn_close(fd);
    KV_LOG(INFO) << "stop: " << fd;
}

void TcpServer::stopAll() {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto & fd : fds_) {
        fds_.clear();
    }
    fds_.clear();
}

void TcpServer::processRecv(int fd, int threadId, std::shared_ptr<RpcProcess> process) {
    if (fd == -1 || process == nullptr) {
        return ;
    }
    std::function<void (const char *, int)> cb =
        [&] (const char * buf, int len) {
            nn_send(fd, buf, len, NN_DONTWAIT);
        };

    char * recv_buf;

    while(1) {
        int rc = nn_recv(fd, &recv_buf, NN_MSG, 0);

        if (rc < 0) {
            NN_LOG(ERROR, "nn_recv with fd: " << fd);
            break;
        }

        process->Insert(threadId, (Packet *) recv_buf, rc, cb);
        nn_freemsg(recv_buf);
    }
}

