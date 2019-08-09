#include "kv_engines.h"

KVEngines::KVEngines() {
}

bool KVEngines::Init(const char * dir) {
    this->engine = new KVEngine[THREAD_NUM];
    printf("THREAD NUM : %d\n", THREAD_NUM);
    for (int id = 0; id < THREAD_NUM ; ++id) {
        engine[id].init(dir, id);
    }
    return true;
}

void KVEngines::Close() {
    for (int id = 0; id < THREAD_NUM ; ++id) {
        engine[id].close();
    }
}

void KVEngines::putKV(char* key, char * val, int threadId) {
    engine[threadId].putKV(key, val);
}

void KVEngines::getV(char * val, int offset, int threadId) {
    engine[threadId].getV(val, offset);
}

bool KVEngines::getK(char * key, int threadId) {
    return engine[threadId].getK(key);
}

void KVEngines::resetKeyPosition(int threadId) {
    return engine[threadId].resetKeyPosition();
}

void KVEngines::recoverKeyPosition(int sum, int threadId) {
    return engine[threadId].recoverKeyPosition(sum);
}

