#include "rpc_process.h"

#include <unistd.h>
#include <thread>
#include "utils.h"

#include <dirent.h>
bool RpcProcess::Insert(int& threadId, char * buf, int len, DoneCbFunc cb) {
    if (buf == nullptr || len < PACKET_HEADER_SIZE) {
        KV_LOG(ERROR) << "insert to RpcProcess failed. size: " << len;
        return false;
    }
    auto & rpc = *(Packet *)buf;

    // 校验通过
    switch(rpc.type) {
        case KV_OP_PUT_KV:
            processPutKV(threadId, buf, cb);
            break;

        case KV_OP_GET_V:
            processGetV(buf, cb);
            break;

        case KV_OP_RESET_K:
            processResetKeyPosition(threadId, buf, cb);
            break;

        case KV_OP_GET_K:
            processGetK(threadId, buf, cb);
            break;

        case KV_OP_RECOVER:
            processRecoverKeyPosition(threadId, buf, cb);
            break;

        case KV_OP_CLEAR:
            //TODO: clear local data
            break;

        default:
            LOG(ERROR) << "unknown rpc type: " << rpc.type;
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

void RpcProcess::processPutKV(int& threadId, char * buf, DoneCbFunc cb) {
    // buf解析为packet
    // TODO: 不传入req
    auto & req = *(Packet *) buf;

    // 从buf构造kvstring
    KVString key;
    KVString val;

    // TODO: 不重新构造
    // key.Reset(req.buf, KEY_SIZE);
    // val.Reset(req.buf + KEY_SIZE, VALUE_SIZE);
    char * key_buf = new char [KEY_SIZE];
    char * val_buf = new char [VALUE_SIZE];
    memcpy(key_buf, req.buf, KEY_SIZE);
    memcpy(val_buf, req.buf + KEY_SIZE, VALUE_SIZE);
    key.Reset(key_buf, KEY_SIZE);
    val.Reset(val_buf, VALUE_SIZE);

    // 调用kvengines添加kv
    auto offset = kv_engines.putKV(key, val, threadId);

    cb(KV_OP_SUCCESS, 0);
}

void RpcProcess::processGetV(char * buf, DoneCbFunc cb) {
    auto & req = *(Packet *)buf;

    uint32_t compress = *(uint32_t *)req.buf;
    int threadId = compress >> 28;
    int offset = compress & 0x0FFFFFFF;

    KVString val;
    kv_engines.getV(val, offset, threadId);

    cb(val.Buf(), val.Size());
}

void RpcProcess::processResetKeyPosition(int& threadId, char * buf, DoneCbFunc cb) {
    auto & req = *(Packet *)buf;

    kv_engines.resetKeyPosition(threadId);

    cb(KV_OP_SUCCESS, 0);
}

void RpcProcess::processGetK(int& threadId, char * buf, DoneCbFunc cb) {

    KVString key;
    bool has_key = kv_engines.getK(key, threadId);

    if (!has_key) {
        cb(KV_OP_FAILED, 0);
    }
    else {
        cb(key.Buf(), KEY_SIZE);
    }
}

void RpcProcess::processRecoverKeyPosition(int& threadId, char * buf, DoneCbFunc cb) {
    auto & req = *(Packet *)buf;

    auto sum = *(uint32_t *)req.buf;

    kv_engines.recoverKeyPosition(sum, threadId);

    cb(KV_OP_SUCCESS, 0);
}

void RpcProcess::Getfilepath(const char *path, const char *filename,  char *filepath)
{
    strcpy(filepath, path);
    if(filepath[strlen(path) - 1] != '/')
        strcat(filepath, "/");
    strcat(filepath, filename);
    printf("path is = %s\n",filepath);
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
