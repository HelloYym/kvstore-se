#include "kv_intf.h"
#include "kv_service.h"
#include "utils.h"
static std::shared_ptr<KVService> kStore(new KVService);

std::shared_ptr<KVIntf> GetKVIntf() {
    printf("GetKVIntf\n");
    return std::make_shared<KVService>();
}


