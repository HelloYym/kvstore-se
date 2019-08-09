#include "rpc_process.h"

#include <unistd.h>
#include <thread>
#include "utils.h"

#include <dirent.h>
bool RpcProcess::Insert(int& threadId, Packet * buf, int len, DoneCbFunc cb, char * send_buf) {
    // 校验通过
    switch(buf->type) {
        case KV_OP_PUT_KV:
            processPutKV(threadId, buf, cb, send_buf);
            break;

        case KV_OP_GET_V:
            processGetV(buf, cb, send_buf);
            break;

        case KV_OP_RESET_K:
            processResetKeyPosition(threadId, buf, cb, send_buf);
            break;

        case KV_OP_GET_K:
            processGetK(threadId, buf, cb, send_buf);
            break;

        case KV_OP_RECOVER:
            processRecoverKeyPosition(threadId, buf, cb, send_buf);
            break;

        default:
            LOG(ERROR) << "unknown rpc type: " << buf->type;
            cb(nullptr, 0);
            break;
    }

    return true;

}

bool RpcProcess::Run(const char * dir, bool clear) {
    if (clear) {
        DeleteFile(dir);
    }
    kv_engines.Init(dir);
}

void RpcProcess::Stop() {
    kv_engines.Close();
    run_ = false;
    sleep(1);
}

void RpcProcess::processPutKV(int& threadId, Packet * buf, DoneCbFunc cb, char * send_buf) {
    // 调用kvengines添加kv
    kv_engines.putKV(buf->buf, buf->buf + KEY_SIZE, threadId);
    auto & tmp = * (Packet *) send_buf;
    tmp.len = PACKET_HEADER_SIZE;
    tmp.type = KV_OP_SUCCESS;
    cb(send_buf, tmp.len);
}

void RpcProcess::processGetV(Packet * buf, DoneCbFunc cb, char * send_buf) {
    uint32_t compress = *(uint32_t *)buf->buf;
    int threadId = compress >> 28;
    int offset = compress & 0x0FFFFFFF;

    auto & tmp = * (Packet *) send_buf;

    kv_engines.getV(tmp.buf, offset, threadId);

    tmp.len = PACKET_HEADER_SIZE + VALUE_SIZE;
    tmp.type = KV_OP_SUCCESS;
    cb(send_buf, tmp.len);
}

void RpcProcess::processResetKeyPosition(int& threadId, Packet * buf, DoneCbFunc cb, char * send_buf) {

    kv_engines.resetKeyPosition(threadId);

    auto & tmp = * (Packet *) send_buf;
    tmp.len = PACKET_HEADER_SIZE;
    tmp.type = KV_OP_SUCCESS;
    cb(send_buf, tmp.len);
}

void RpcProcess::processGetK(int& threadId, Packet * buf, DoneCbFunc cb, char * send_buf) {

    auto & tmp = * (Packet *) send_buf;
    bool has_key = kv_engines.getK(tmp.buf, threadId);

    if (!has_key) {
        tmp.len = PACKET_HEADER_SIZE;
        tmp.type = KV_OP_FAILED;
        cb(send_buf, tmp.len);
    }
    else {
        tmp.len = PACKET_HEADER_SIZE + KEY_SIZE;
        tmp.type = KV_OP_SUCCESS;
        cb(send_buf, tmp.len);
    }
}


void RpcProcess::processRecoverKeyPosition(int& threadId, Packet * buf, DoneCbFunc cb, char * send_buf) {

    auto sum = *(uint32_t *)buf->buf;

    kv_engines.recoverKeyPosition(sum, threadId);

    auto & tmp = * (Packet *) send_buf;
    tmp.len = PACKET_HEADER_SIZE;
    tmp.type = KV_OP_SUCCESS;
    cb(send_buf, tmp.len);
}

void RpcProcess::Getfilepath(const char *path, const char *filename,  char *filepath)
{
    strcpy(filepath, path);
    if(filepath[strlen(path) - 1] != '/')
        strcat(filepath, "/");
    strcat(filepath, filename);
//    printf("path is = %s\n",filepath);
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
