#ifndef PROJECT_KV_CLIENT_H
#define PROJECT_KV_CLIENT_H

#include <memory>
#include <malloc.h>
#include <iostream>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <sys/socket.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/time.h>
#include <unistd.h>
#include <string>
#include <mutex>
#include <atomic>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>

#include "kv_hashlog.h"
#include "kv_string.h"
#include "params.h"
#include "utils.h"

using namespace std::chrono;

class KVClient {
public:

    void clientClose() {
        printf("Client close start %d\n", id);
        close(fd);
        delete [] sendBuf;
        delete [] recvBuf;
        delete [] seqReadBuf;
        HashLog::getInstance().close();
        printf("Client close over %d\n", id);
    }

    bool clientInit(const char *host, int id) {

        printf("Client init start %s, %d\n", host, id);

        this->id = (uint32_t)id;
        nums = 0;
        recoverFlag = false;
        isInStep2 = false;
        isInStep3 = false;

        sendBuf = new char[MAX_PACKET_SIZE];
        recvBuf = new char[MAX_PACKET_SIZE];
        setTimes = 0;
        getTimes = 0;

        //顺序读批量读相关变量
//        seqReadBuf = static_cast<char *>(memalign((size_t) getpagesize(), SEQREAD_CACHE_SIZE));
        seqReadBuf = new char[SEQREAD_CACHE_SIZE];
        seqReadBufStartIndex = INT32_MIN;

        // connect to storage
        auto port = 9500;
        if ((fd = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
            printf("socket error\n");
            exit(-1);
        }
        server_addr.sin_family = AF_INET;
        server_addr.sin_port = htons(port);
        server_addr.sin_addr.s_addr = inet_addr(host + 6);
        bzero(&(server_addr.sin_zero), sizeof(server_addr.sin_zero));

        while (connect(fd, (struct sockaddr *) &server_addr, sizeof(struct sockaddr_in)) == -1) {
            sleep(1);
        }

        int on = 1;
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (void *) &on, sizeof(on));
        // 接收缓冲区
        int nRecvBuf = 16 * 1024 * 1024;//设置为16M
        setsockopt(fd, SOL_SOCKET, SO_RCVBUF, (const char *) &nRecvBuf, sizeof(int));
        //发送缓冲区
        int nSendBuf = 1 * 1024 * 1024;//设置为1M
        setsockopt(fd, SOL_SOCKET, SO_SNDBUF, (const char *) &nSendBuf, sizeof(int));

        KV_LOG(INFO) << "connect to store node success. fd: " << fd;

        HashLog::getInstance().client_on();

        this->start = now();

        printf("Client init over %s, %d\n", host, id);
        return true;
    }

    void recoverIndex() {
        printf("start recover index: %d.\n", id);
        reset();
        while (getKey(nums)) {
        }
        printf("recover threadId %d. time spent is %lims\n", id, (now() - start).count());
        printf("======key num: %d\n", nums);
        recover(nums);
        HashLog::getInstance().wait_finish();
    }

    int set(KVString &key, KVString &val) {
        if (!recoverFlag) {
            recoverFlag = true;
            if (!HashLog::getInstance().isInStep1) {
                isInStep2 = true;
                isInStep3 = false;
                printf("================== step 2 ==================\n");
            }
            recoverIndex();
        }

        //print
        if (setTimes == 0) {
            this->start = now();
        }

        if (setTimes % 1000000 == 0 || setTimes < 10) {
            printf("ID : %d,  Set : %lu\n", id, *((uint64_t *) key.Buf()));
            printf("write %d. time spent is %lims\n", setTimes, (now() - start).count());
        }
        setTimes++;

        sendKV(key, val);
        HashLog::getInstance().put(*((uint64_t *) key.Buf()), (id << 28) + nums, id);
        nums++;
        return 1;
    }

    int get(KVString &key, KVString &val) {
        if (!recoverFlag) {
            recoverFlag = true;
            if (!HashLog::getInstance().isInStep1) {
                isInStep2 = false;
                isInStep3 = true;
                printf("================== step 3 ==================\n");
            }
            recoverIndex();
        }
        uint32_t pos;
        while (!HashLog::getInstance().find(*((uint64_t *) key.Buf()), pos, id)) {
            sleep(5);
            printf("wait to find pos from hash\n");
        }

        //print
        if (getTimes == 0) {
            this->start = now();
        }
        if (getTimes % 1000000 == 0 || getTimes < 10) {
            printf("ID : %d,  Get : %lu,  threadId : %d, pos : %d\n",
                   id, *((uint64_t *) key.Buf()), pos >> 28, pos & 0x0FFFFFFF);
            printf("read %d. time spent is %lims\n", getTimes, (now() - start).count());
        }
        getTimes++;

        //ToDo 暂时只做顺序读阶段的批量
        if (isInStep2) {
            getValueBatch(pos, val);
        } else {
            return getValue(pos, val);
        }
    }

private:

    uint32_t id;
    bool recoverFlag;
    bool isInStep2;//判断是否在顺序写+顺序读阶段
    bool isInStep3;//判断是否在随机读阶段

    char *sendBuf;
    char *recvBuf;
    int fd;
    struct sockaddr_in server_addr;
    uint32_t nums;

    //顺序读批量读相关变量
    char * seqReadBuf;
    int seqReadBufStartIndex;

    milliseconds now() {
        return duration_cast<milliseconds>(system_clock::now().time_since_epoch());
    }
    milliseconds start;
    int setTimes, getTimes;

    void sendKV(KVString &key, KVString &val) {
        auto &send_pkt = *(Packet *) sendBuf;
        send_pkt.len = PACKET_HEADER_SIZE + sizeof(uint32_t)+ KEY_SIZE + VALUE_SIZE;
        send_pkt.type = KV_OP_PUT_KV;
        memcpy(send_pkt.buf, (char *) &id, sizeof(uint32_t));
        memcpy(send_pkt.buf + sizeof(uint32_t), key.Buf(), KEY_SIZE);
        memcpy(send_pkt.buf + KEY_SIZE + sizeof(uint32_t), val.Buf(), VALUE_SIZE);
        sendPack(fd, sendBuf);
        recv_bytes(fd, recvBuf, 1);
    }

    bool getKey(uint32_t &sum) {
        auto &send_pkt = *(Packet *) sendBuf;
        send_pkt.len = PACKET_HEADER_SIZE + sizeof(uint32_t);
        send_pkt.type = KV_OP_GET_K;
        memcpy(send_pkt.buf, (char *) &id, sizeof(uint32_t));
        sendPack(fd, sendBuf);

        recv_bytes(fd, recvBuf, KEY_SIZE * KEY_NUM_TCP);

        for (int i = 0; i < KEY_NUM_TCP; ++i) {
            uint64_t k = *((uint64_t *) (recvBuf + 8 * i));

            if (k != 0 || (k == 0 && *(recvBuf + i * 8) != 0)) {
                HashLog::getInstance().put(k, (id << 28) + sum, id);
                sum++;
            } else
                return false;

        }

        return true;
    }

    int getValue(uint32_t pos, KVString &val) {
        auto &send_pkt = *(Packet *) sendBuf;
        send_pkt.len = sizeof(uint32_t) + PACKET_HEADER_SIZE;
        send_pkt.type = KV_OP_GET_V;
        memcpy(send_pkt.buf, (char *) &pos, sizeof(uint32_t));
        sendPack(fd, sendBuf);

        char *v = new char[VALUE_SIZE];
        recv_bytes(fd, v, VALUE_SIZE);
        val.Reset(v, VALUE_SIZE);

//        recv_bytes(fd, recvBuf, VALUE_SIZE);
//        val.Reset(recvBuf, VALUE_SIZE);
        return 1;
    }

    int getValueBatch(uint32_t pos, KVString &val) {
        uint32_t index = pos & 0x0FFFFFFF;

        uint32_t currentBufNo = index / (uint32_t)SEQREAD_CACHE_NUM;

        //如果缓存块还没读进来，就发情求批量读
        if (currentBufNo != seqReadBufStartIndex / (uint32_t)SEQREAD_CACHE_NUM) {
            seqReadBufStartIndex = currentBufNo * (uint32_t)SEQREAD_CACHE_NUM;

            auto &send_pkt = *(Packet *) sendBuf;
            send_pkt.len = sizeof(uint32_t) + PACKET_HEADER_SIZE;
            send_pkt.type = KV_OP_GETBATCH_V;
            memcpy(send_pkt.buf, (char *) &pos, sizeof(uint32_t));
            sendPack(fd, sendBuf);
            recv_bytes(fd, seqReadBuf, SEQREAD_CACHE_SIZE);
        }

        uint32_t value_buf_offset = index % (uint32_t)SEQREAD_CACHE_NUM;
        char *v = new char[VALUE_SIZE];
        memcpy(v, seqReadBuf + value_buf_offset * VALUE_SIZE, VALUE_SIZE);
        val.Reset(v, VALUE_SIZE);
//        val.Reset(seqReadBuf + value_buf_offset * VALUE_SIZE, VALUE_SIZE);
    }

    void reset() {
        auto &send_pkt = *(Packet *) sendBuf;
        send_pkt.len = PACKET_HEADER_SIZE + sizeof(uint32_t);
        send_pkt.type = KV_OP_RESET_K;
        memcpy(send_pkt.buf, (char *) &id, sizeof(uint32_t));
        sendPack(fd, sendBuf);
        recv_bytes(fd, recvBuf, 1);
    }

    void recover(int sum) {
        auto &send_pkt = *(Packet *) sendBuf;
        send_pkt.len = PACKET_HEADER_SIZE + 2 * sizeof(uint32_t);
        send_pkt.type = KV_OP_RECOVER;
        memcpy(send_pkt.buf, (char *) &id, sizeof(uint32_t));
        memcpy(send_pkt.buf + sizeof(uint32_t), (char *) &sum, sizeof(uint32_t));
        sendPack(fd, sendBuf);
        recv_bytes(fd, recvBuf, 1);

    }

    void sendPack(int fd, char *buf) {
        auto &send_pkt = *(Packet *) buf;
        if (send(fd, buf, send_pkt.len, 0) == -1) {
            printf("send error\n");
        }
    }

    int recvPack(int fd, char *buf) {
        auto bytes = recv(fd, buf, MAX_PACKET_SIZE, 0);
        if (bytes <= 0) {
            return bytes;
        }
        while (bytes < sizeof(int)) {
            auto b = recv(fd, buf + bytes, MAX_PACKET_SIZE, 0);
            if (b <= 0) {
                return b;
            }
            bytes += b;
        }
        int total = *(int *) buf;
        while (total != bytes) {
            auto b = recv(fd, buf + bytes, MAX_PACKET_SIZE, 0);
            if (b <= 0) {
                return b;
            }
            bytes += b;
        }

        return total;
    }

    int recv_bytes(int fd, char *buf, int total) {
        int bytes = 0;

        while (total != bytes) {
            auto b = recv(fd, buf + bytes, MAX_PACKET_SIZE, 0);
            if (b <= 0) {
                return -1;
            }
            bytes += b;
        }

        return total;
    }

};

#endif //PROJECT_KV_CLIENT_H
