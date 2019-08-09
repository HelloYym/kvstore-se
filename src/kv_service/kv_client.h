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

#include "kv_hashlog_lf.h"
#include "kv_string.h"
#include "params.h"
#include "utils.h"
#include "nanomsg/nn.h"
#include "nanomsg/tcp.h"
#include "nanomsg/reqrep.h"


#define NN_LOG(level, msg) KV_LOG(level) << msg << " failed. error: " << nn_strerror(nn_errno())
using namespace std::chrono;

class KVClient {
    public:

        void clientClose() {
            printf("Client close start %d\n", id);
            close(fd);
            nums = 0;
            recoverFlag = false;
            HashLogLF::getInstance().close();
            printf("Client close over %d\n", id);
        }

        bool clientInit(const char * host, int id) {

            printf("Client init start %s, %d\n", host, id);

            this->id = id;

            // connect to storage
            auto port = 9500 + id;
            if ((fd = socket(AF_INET,SOCK_STREAM, 0)) == -1) {
                printf("socket error\n");
                exit(-1);
            }


            server_addr.sin_family = AF_INET;
            server_addr.sin_port = htons(port);
            server_addr.sin_addr.s_addr = inet_addr(host);
            bzero(&(server_addr.sin_zero),sizeof(server_addr.sin_zero));

            if (connect(fd, (struct sockaddr *)&server_addr,sizeof(struct sockaddr_in)) == -1){
                printf("connect error\n");
                exit(-1);
            }

            KV_LOG(INFO) << "connect to store node success. fd: " << fd;

            HashLogLF::getInstance().client_on();
            this->start = now();

            printf("Client init over %s, %d\n", host, id);
            return true;
        }

        void recoverIndex() {
            printf("start recover index: %d.\n", id);
            reset();
            while (getKey(nums)) {
                nums ++;
            }

//            nums = getAllKeys();

            printf("recover threadId %d. time spent is %lims\n", id, (now() - start).count());
            printf("======key num: %d\n", nums);
            recover(nums);
        }

        int set(KVString &key, KVString & val) {
            if (!recoverFlag) {
                recoverFlag = true;
                recoverIndex();
            }
            if (setTimes == 0) {
                this->start = now();
            }

            if (setTimes % 500000 == 0) {
                printf("ID : %d,  Set : %ld\n", id, *((u_int64_t *) key.Buf()));
                printf("write %d. time spent is %lims\n", setTimes, (now() - start).count());
            }
            setTimes ++;

            sendKV(key, val);
            HashLogLF::getInstance().put(*((u_int64_t *) key.Buf()), (id << 28) + nums);
            nums++;
            return 1;
        }

        int get(KVString &key, KVString & val) {
            if (!recoverFlag) {
                recoverFlag = true;
                recoverIndex();
            }
            auto pos = HashLogLF::getInstance().find(*((u_int64_t *) key.Buf()));

            if (getTimes == 0) {
                this->start = now();
            }
            if (getTimes % 500000 == 0) {
                printf("ID : %d,  Get : %ld,  threadId : %d, pos : %d\n",
                       id, *((u_int64_t *) key.Buf()), (uint32_t)pos>>28, pos & 0x0FFFFFFF);
                printf("read %d. time spent is %lims\n", getTimes, (now() - start).count());
            }
            getTimes ++;

            return getValue(pos, val);
        }

    private:

        int id;

        bool recoverFlag = false;

        char * sendBuf = new char[MAX_PACKET_SIZE];
        char * recvBuf = new char[MAX_PACKET_SIZE];

        int fd;

        struct sockaddr_in server_addr;


        uint32_t nums = 0;

        milliseconds now() {
            return duration_cast<milliseconds>(system_clock::now().time_since_epoch());
        }
        milliseconds start;
        int setTimes = 0, getTimes = 0;


        void sendKV(KVString & key, KVString & val) {
            auto send_len = KEY_SIZE + VALUE_SIZE + PACKET_HEADER_SIZE;
            auto & send_pkt = *(Packet *) sendBuf;
            send_pkt.len = send_len;
            memcpy(send_pkt.buf, key.Buf(), KEY_SIZE);
            memcpy(send_pkt.buf + KEY_SIZE, val.Buf(), VALUE_SIZE);
            send_pkt.type = KV_OP_PUT_KV;

            if(send(fd, sendBuf, send_len, 0) == -1){
                printf("send error\n");
            }
            recvPack(fd, recvBuf);
        }

        int getKey(uint32_t& sum) {
            auto & send_pkt = *(Packet *) sendBuf;
            send_pkt.type   = KV_OP_GET_K;
            nn_send(fd, sendBuf, PACKET_HEADER_SIZE, 0);

            recvPack(fd, recvBuf);

            char * ret_buf;
            int rc = nn_recv(fd, &ret_buf, NN_MSG, 0);
            if (rc == 0) {
                nn_freemsg(ret_buf);
                return 0;
            } else {
                HashLogLF::getInstance().put(*((u_int64_t *) ret_buf), (id << 28) + sum);
                nn_freemsg(ret_buf);
                return 1;
            }
        }

        int getAllKeys() {
            auto & send_pkt = *(Packet *) sendBuf;
            send_pkt.type   = KV_OP_GET_K;
            nn_send(fd, sendBuf, PACKET_HEADER_SIZE, 0);

            u_int64_t * ret_buf;
            int rc = nn_recv(fd, &ret_buf, NN_MSG, 0);

            if (rc == 0) {
                nn_freemsg(ret_buf);
                return 0;
            } else {
                int count = 0;
                u_int64_t k = *(ret_buf + count);
                while (k || (k == 0 && *(ret_buf + count + 1) != 0)) {
                    HashLogLF::getInstance().put(k, (id << 28) + count);
                    k = *(ret_buf + ++count);
                }
                nn_freemsg(ret_buf);
                return count;
            }
        }

        int getValue(uint32_t pos, KVString &val) {
            auto send_len = sizeof(uint32_t);
            auto & send_pkt = *(Packet *) sendBuf;
            memcpy(send_pkt.buf, (char *)&pos, send_len);
            send_pkt.type   = KV_OP_GET_V;

            int rc = nn_send(fd, sendBuf, send_len + PACKET_HEADER_SIZE, 0);

            char * ret_buf;
            nn_recv(fd, &ret_buf, NN_MSG, 0);


            // TODO: 拷贝了好多次
            char * v = new char [VALUE_SIZE];
            memcpy(v, ret_buf, VALUE_SIZE);
            nn_freemsg(ret_buf);

            val.Reset(v, VALUE_SIZE);

            return 1;
        }

        void reset() {
            auto & send_pkt = *(Packet *) sendBuf;
            send_pkt.type   = KV_OP_RESET_K;

            int rc = nn_send(fd, sendBuf, PACKET_HEADER_SIZE, 0);

            char * ret_buf;
            rc = nn_recv(fd, &ret_buf, NN_MSG, 0);
            nn_freemsg(ret_buf);
        }

        void recover(int sum) {
            auto send_len = sizeof(uint32_t);

            auto & send_pkt = *(Packet *) sendBuf;
            memcpy(send_pkt.buf, (char *)&sum, send_len);

            send_pkt.type   = KV_OP_RECOVER;

            int rc = nn_send(fd, sendBuf, send_len + PACKET_HEADER_SIZE, 0);

            char * ret_buf;
            rc = nn_recv(fd, &ret_buf, NN_MSG, 0);
            nn_freemsg(ret_buf);
        }

};
#endif //PROJECT_KV_CLIENT_H
