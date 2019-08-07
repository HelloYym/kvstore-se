#include "kv_engines.h"

KVEngines::KVEngines() {
}

bool KVEngines::Init(const char * dir) {
    this->engine = new KVEngine[16];
    for (int id = 0; id < 16 ; ++id) {
        engine[id].init(dir, id);
    }
    return true;
}

void KVEngines::Close() {
    for (int id = 0; id < 16 ; ++id) {
        engine[id].close();
    }
}

int KVEngines::putKV(KVString & key, KVString & val, int threadId) {
    return engine[threadId].putKV(key, val);
}

void KVEngines::getV(KVString & val, int offset, int threadId) {
    engine[threadId].getV(val, offset);
}

bool KVEngines::getK(KVString & key, int offset, int threadId) {
    return engine[threadId].getK(key, offset);
}

void KVEngines::resetKeyPosition(int threadId) {
    return engine[threadId].resetKeyPosition();
}

void KVEngines::recoverKeyPosition(int sum, int threadId) {
    return engine[threadId].recoverKeyPosition(sum);
}

