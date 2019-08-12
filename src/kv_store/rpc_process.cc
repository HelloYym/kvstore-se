#include "rpc_process.h"

#include <unistd.h>
#include <thread>
#include "utils.h"

#include <dirent.h>
bool RpcProcess::Insert(Packet * buf, int len, DoneCbFunc cb, char * send_buf) {
    // 校验通过
    switch(buf->type) {
        case KV_OP_PUT_KV:
            processPutKV(buf, cb, send_buf);
            break;

        case KV_OP_GET_V:
            processGetV(buf, cb, send_buf);
            break;

        case KV_OP_RESET_K:
            processResetKeyPosition(buf, cb, send_buf);
            break;

        case KV_OP_GET_K:
            processGetK(buf, cb, send_buf);
            break;

        case KV_OP_RECOVER:
            processRecoverKeyPosition(buf, cb, send_buf);
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
    if (access(dir, 0) == -1) {
        mkdir(dir, 0777);
    }
    kv_engines.Init(dir);
}

void RpcProcess::Stop() {
    kv_engines.Close();
    run_ = false;
    sleep(1);
}

void RpcProcess::processPutKV(Packet * buf, DoneCbFunc cb, char * send_buf) {
    int threadId = *((uint32_t *)buf->buf);
    // 调用kvengines添加kv
    kv_engines.putKV(buf->buf + sizeof(uint32_t), buf->buf + sizeof(uint32_t) + KEY_SIZE, threadId);

    cb("1", 1);
}

void RpcProcess::processGetV(Packet * buf, DoneCbFunc cb, char * send_buf) {
    uint32_t compress = *(uint32_t *)buf->buf;
    int threadId = compress >> 28;
    int offset = compress & 0x0FFFFFFF;
    kv_engines.getV(send_buf, offset, threadId);
    cb(send_buf, VALUE_SIZE);
}

void RpcProcess::processResetKeyPosition(Packet * buf, DoneCbFunc cb, char * send_buf) {
    int threadId = *((uint32_t *)buf->buf);
    kv_engines.resetKeyPosition(threadId);
    cb("1", 1);
}

void RpcProcess::processGetK(Packet * buf, DoneCbFunc cb, char * send_buf) {
    int threadId = *((uint32_t *)buf->buf);
    auto & tmp = * (Packet *) send_buf;
    char * key_buf = kv_engines.getK(threadId);
    cb(key_buf, KEY_NUM_TCP * KEY_SIZE);
}


void RpcProcess::processRecoverKeyPosition(Packet * buf, DoneCbFunc cb, char * send_buf) {
    int threadId = *((uint32_t *)buf->buf);

    auto sum = *(uint32_t *)(buf->buf + sizeof(uint32_t));

    kv_engines.recoverKeyPosition(sum, threadId);

    cb("1", 1);
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
