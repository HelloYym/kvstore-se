#ifndef KV_ENGINES_H
#define KV_ENGINES_H

#include "kv_string.h"
#include "kv_engine.h"

class KVEngines{
public:

    bool Init(const char * dir) {
        this->engine = new KVEngine[THREAD_NUM];
        printf("THREAD NUM : %d\n", THREAD_NUM);
        for (int id = 0; id < THREAD_NUM ; ++id) {
            engine[id].init(dir, id);
        }
        return true;
    }
    
    void Close() {
        for (int id = 0; id < THREAD_NUM ; ++id) {
            engine[id].close();
        }
    }

    void putKV(char* key, char * val, int threadId) {
        engine[threadId].putKV(key, val);
    }

    void getV(char * val, int offset, int threadId) {
        engine[threadId].getV(val, offset);
    }

    void getVRandom(char * val, int offset, int threadId) {
        engine[threadId].getVRandom(val, offset);
    }

    void pre_read_value(int threadId) {
        engine[threadId].pre_read_value();
    }

    char * getK(int threadId) {
        return engine[threadId].getK();
    }

    void resetKeyPosition(int threadId) {
        return engine[threadId].resetKeyPosition();
    }

    void recoverKeyPosition(int sum, int threadId) {
        return engine[threadId].recoverKeyPosition(sum);
    }



private:
    KVEngine* engine;
};

#endif

