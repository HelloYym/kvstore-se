#include "kv_service.h"
#include "utils.h"

bool KVService::Init(const char * host, int id) {
    kvClient.init(host, id);
    kvClient.recoverIndex();
    return true;
}

void KVService::Close() {
    kvClient.close();
}

int KVService::Set(KVString & key, KVString & val) {
    return kvClient.set(key, val);
}

int KVService::Get(KVString & key, KVString & val) {
    return kvClient.get(key, val);
}
