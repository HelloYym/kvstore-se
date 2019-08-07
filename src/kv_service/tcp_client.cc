#include "tcp_client.h"
#include <unistd.h>
#include <thread>
#include <memory>

#include "nanomsg/nn.h"
#include "nanomsg/reqrep.h"
#include "utils.h"

#define NN_LOG(level, msg) KV_LOG(level) << msg << " failed. error: " << nn_strerror(nn_errno())


TcpClient::TcpClient() {
}

TcpClient::~TcpClient() {
    close();
}

char * TcpClient::send(char * buf, int len) {
    int rc = nn_send(fd, buf, len, 0);
    if (rc < 0) {
        NN_LOG(ERROR, "nn_send") << "ret: " << rc;
        return nullptr;
    }

    char *msg;
    rc = nn_recv(fd, &msg, NN_MSG, 0);
    if (rc < 0) {
        NN_LOG(ERROR, "nn_recv with NN_MSG") << "ret: " << rc;
        return nullptr;
    }

    char * ret = nullptr;
    do {
        if (rc < PACKET_HEADER_SIZE) {
            KV_LOG(ERROR) << "recv msg size is less than packet header: " << rc;
            break;
        }

        auto & packet = *(Packet *) msg;
        if (rc < PACKET_HEADER_SIZE + packet.len) {
            KV_LOG(ERROR) << "recv msg size is less than expect: " << rc << "-" << packet.len;
            break;
        }

        int sum = packet.Sum();
        if (packet.crc != sum) {
            KV_LOG(ERROR) << "recv msg crc error, expect: " << packet.crc << " get: " << sum;
            break;
        }

        ret = new char[rc];
        memcpy(ret, msg, rc);
    } while(0);

    nn_freemsg(msg);
    return ret;
}


int TcpClient::connect(const char * url) {
    fd = nn_socket(AF_SP, NN_REQ);
    if (fd < 0) {
        NN_LOG(ERROR, "nn_socket");
        return -1;
    }

    if (nn_connect(fd, url) < 0) {
        NN_LOG(ERROR, "nn_connect");
        nn_close(fd);
        return -1;
    }

    KV_LOG(INFO) << "connect to store node success. fd: " << fd;

    return fd;
}

void TcpClient::close() {
    nn_close(fd);
    KV_LOG(INFO) << "stop: " << fd;
}

