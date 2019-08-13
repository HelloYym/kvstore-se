#ifndef KV_ENGINES_H
#define KV_ENGINES_H

#include "kv_string.h"
#include "kv_engine.h"

class KVEngines{
public:
    KVEngines();
    //调用它进行初始化，初始化一个实例
    bool Init(const char * dir);

    void Close();

    //根据线程id 存储key和value
    void putKV(char * key, char * val, int threadId);

    //根据线程id 和 offset 获取 value
    void getV(char * val, int offset, int threadId);

    void getVRandom(char * val, int offset, int threadId);

    //下面三个函数是重启恢复位置。
    // 先调用第一个函数，再循环调用第二个直到false，最后调用最后一个函数。
    void resetKeyPosition(int threadId);

    //根据线程id 和 offset 获取 key
    //返回key是否读到末尾，false为末尾
    char * getK(int threadId);

    void recoverKeyPosition(int sum, int threadId);


private:
    KVEngine* engine;
};

#endif

