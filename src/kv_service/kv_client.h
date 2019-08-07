#ifndef PROJECT_KV_CLIENT_H
#define PROJECT_KV_CLIENT_H

#include <memory>
#include <malloc.h>
#include <iostream>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/time.h>
#include <unistd.h>
#include <string>
#include <mutex>
#include <atomic>

#include "kv_hashlog.h"
#include "kv_string.h"
#include "tcp_client.h"

using namespace std;


class KVClient {
public:
    void close() {
    }

    bool init(const char * host, int id) {
        printf("Client init %s, %d\n", host, id);
        tcpClient = new TcpClient();
        tcpClient->connect(host);
        this->id = id;
        return true;
    }

    void recoverIndex() {
        int sum = 0;
        reset();
        while (getKey()) {
            sum ++;
        }
        recover(sum);
    }

    int set(KVString &key, KVString & val) {
        sendKV(key, val);
        HashLog::getInstance().put(*((u_int64_t *) key.Buf()), id);
        return 1;
    }

    int get(KVString &key, KVString & val) {
        auto pos = HashLog::getInstance().find(*((u_int64_t *) key.Buf()));
        return getValue(pos, val);
    }

private:
    int id;

    const int sendLen = PACKET_HEADER_SIZE + KEY_SIZE + VALUE_SIZE;

    char * sendBuf = new char[sendLen];

    TcpClient *tcpClient = nullptr;

    int sendKV(KVString & key, KVString & val) {
        auto send_len = KEY_SIZE + VALUE_SIZE;
        auto & send_pkt = *(Packet *) sendBuf;
        memcpy(send_pkt.buf, key.Buf(), KEY_SIZE);
        memcpy(send_pkt.buf + KEY_SIZE, val.Buf(), VALUE_SIZE);
        send_pkt.len    = send_len;
        send_pkt.sn     = 0;
        send_pkt.type   = KV_OP_PUT_KV;
        send_pkt.crc    = send_pkt.Sum();

        char * ret_buf = tcpClient->send(sendBuf, send_len + PACKET_HEADER_SIZE);

        if (ret_buf == nullptr) {
            KV_LOG(ERROR) << "set return null";
            return -1;
        }

        auto & recv_pkt = *(Packet *)ret_buf;
        auto ret = *(int *)recv_pkt.buf;
        delete [] ret_buf;
        return ret;
    }

    int getKey() {
        auto & send_pkt = *(Packet *) sendBuf;

        send_pkt.len    = 0;
        send_pkt.sn     = 0;
        send_pkt.type   = KV_OP_GET_K;
        send_pkt.crc = send_pkt.Sum();

        char * ret_buf = tcpClient->send(sendBuf, PACKET_HEADER_SIZE);

        if (ret_buf == nullptr) {
            KV_LOG(ERROR) << "Get return null";
            return -1;
        }


        auto & recv_pkt = *(Packet *)ret_buf;

        if (recv_pkt.len == 0) {
            return 0;
        } else {
            HashLog::getInstance().put(*((u_int64_t *) recv_pkt.buf), id);
            return 1;
        }
    }


    int getValue(uint32_t pos, KVString &val) {
        auto send_len = sizeof(uint32_t);
        auto & send_pkt = *(Packet *) sendBuf;
        memcpy(send_pkt.buf, (char *)&pos, send_len);

        send_pkt.len    = send_len;
        send_pkt.sn     = 0;
        send_pkt.type   = KV_OP_GET_V;
        send_pkt.crc = send_pkt.Sum();

        char * ret_buf = tcpClient->send(sendBuf, send_len + PACKET_HEADER_SIZE);

        if (ret_buf == nullptr) {
            KV_LOG(ERROR) << "Get return null";
            return -1;
        }

        auto & recv_pkt = *(Packet *)ret_buf;

        char * v = new char [VALUE_SIZE];
        memcpy(v, recv_pkt.buf, VALUE_SIZE);
        delete [] ret_buf;

        val.Reset(v, VALUE_SIZE);

        return 1;
    }

    int reset() {
        auto & send_pkt = *(Packet *) sendBuf;

        send_pkt.len    = 0;
        send_pkt.sn     = 0;
        send_pkt.type   = KV_OP_RESET_K;
        send_pkt.crc = send_pkt.Sum();

        char * ret_buf = tcpClient->send(sendBuf,  PACKET_HEADER_SIZE);

        if (ret_buf == nullptr) {
            KV_LOG(ERROR) << "Get return null";
            return -1;
        }

        return 1;
    }

    int recover(int sum) {
        auto send_len = sizeof(uint32_t);

        auto & send_pkt = *(Packet *) sendBuf;
        memcpy(send_pkt.buf, (char *)&sum, send_len);

        send_pkt.len    = sizeof(u_int32_t);
        send_pkt.sn     = 0;
        send_pkt.type   = KV_OP_RECOVER;
        send_pkt.crc = send_pkt.Sum();

        char * ret_buf = tcpClient->send(sendBuf, send_len + PACKET_HEADER_SIZE);

        if (ret_buf == nullptr) {
            KV_LOG(ERROR) << "Get return null";
            return -1;
        }

        return 1;
    }

};
#endif //PROJECT_KV_CLIENT_H
