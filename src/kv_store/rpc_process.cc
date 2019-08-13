#include "rpc_process.h"

#include <unistd.h>
#include <thread>
#include "utils.h"

#include <dirent.h>
bool RpcProcess::Insert(int sfd, Packet * recv_buf, char * send_buf) {
    switch(recv_buf->type) {
        case KV_OP_PUT_KV:
            processPutKV(sfd, recv_buf, send_buf);
            break;

        case KV_OP_GET_V:
            processGetV(sfd, recv_buf, send_buf);
            break;

        case KV_OP_RESET_K:
            processResetKeyPosition(sfd, recv_buf, send_buf);
            break;

        case KV_OP_GET_K:
            processGetK(sfd, recv_buf, send_buf);
            break;

        case KV_OP_RECOVER:
            processRecoverKeyPosition(sfd, recv_buf, send_buf);
            break;
        case KV_OP_GETBATCH_V:
            processGetBatchV(sfd, recv_buf, send_buf);
            break;
        default:
            LOG(ERROR) << "unknown rpc type: " << recv_buf->type;
            break;
    }
    return true;
}

void RpcProcess::processPutKV(int sfd, Packet * buf, char * send_buf) {
    int threadId = *((uint32_t *)buf->buf);
    kv_engines.putKV(buf->buf + sizeof(uint32_t), buf->buf + sizeof(uint32_t) + KEY_SIZE, threadId);
    send(sfd, "1", 1, 0);
}

void RpcProcess::processGetV(int sfd, Packet * buf, char * send_buf) {
    uint32_t compress = *(uint32_t *)buf->buf;
    int threadId = compress >> 28;
    int offset = compress & 0x0FFFFFFF;
    
    //方式1：pread出来
    kv_engines.getV(send_buf, offset, threadId);
    send(sfd, send_buf, VALUE_SIZE, 0);

    //方式2：sendfile零拷贝
//    kv_engines.getVZeroCopy(sfd, offset, threadId);
}

void RpcProcess::processGetBatchV(int sfd, Packet * buf, char * send_buf) {
    uint32_t compress = *(uint32_t *)buf->buf;
    int threadId = compress >> 28;
    int offset = compress & 0x0FFFFFFF;
    offset = offset / (uint32_t)SEQREAD_CACHE_NUM * (uint32_t)SEQREAD_CACHE_NUM;

    //方式1：pread出来
    kv_engines.getVBatch(send_buf, offset, threadId);
    send(sfd, send_buf, SEQREAD_CACHE_SIZE, 0);
    
    //方式2：sendfile零拷贝
//    kv_engines.getVBatchZeroCopy(sfd, offset, threadId);
}

void RpcProcess::processResetKeyPosition(int sfd, Packet * buf, char * send_buf) {
    int threadId = *((uint32_t *)buf->buf);
    kv_engines.resetKeyPosition(threadId);
    send(sfd, "1", 1, 0);
}

void RpcProcess::processGetK(int sfd, Packet * buf, char * send_buf) {
    int threadId = *((uint32_t *)buf->buf);
    auto & tmp = * (Packet *) send_buf;
    char * key_buf = kv_engines.getK(threadId);
    send(sfd, key_buf, KEY_NUM_TCP * KEY_SIZE, 0);
}

void RpcProcess::processRecoverKeyPosition(int sfd, Packet * buf, char * send_buf) {
    int threadId = *((uint32_t *)buf->buf);
    auto sum = *(uint32_t *)(buf->buf + sizeof(uint32_t));
    kv_engines.recoverKeyPosition(sum, threadId);
    send(sfd, "1", 1, 0);
}

void RpcProcess::Getfilepath(const char *path, const char *filename,  char *filepath)
{
    strcpy(filepath, path);
    if(filepath[strlen(path) - 1] != '/')
        strcat(filepath, "/");
    strcat(filepath, filename);
}

bool RpcProcess::DeleteFile(const char* path)
{
    DIR *dir;
    struct dirent *dirinfo;
    struct stat statbuf;
    char filepath[256] = {0};
    lstat(path, &statbuf);

    if (S_ISREG(statbuf.st_mode))//判断是否是常规文件
    {
        remove(path);
    }
    else if (S_ISDIR(statbuf.st_mode))//判断是否是目录
    {
        if ((dir = opendir(path)) == NULL)
            return 1;
        while ((dirinfo = readdir(dir)) != NULL)
        {
            Getfilepath(path, dirinfo->d_name, filepath);
            if (strcmp(dirinfo->d_name, ".") == 0 || strcmp(dirinfo->d_name, "..") == 0)//判断是否是特殊目录
                continue;
            DeleteFile(filepath);
            rmdir(filepath);
        }
        closedir(dir);
    }
    return 0;
}


bool RpcProcess::Run(const char * dir, bool clear) {
    if (clear) {
        DeleteFile(dir);
    }
    if (access(dir, 0) == -1) {
        mkdir(dir, 0777);
    }
    kv_engines.Init(dir);
}

void RpcProcess::Stop() {
    kv_engines.Close();
    sleep(1);
}