//
//  on 11/8/18.
//

#include "random_variants.h"
#include <algorithm>
#include "utils.h"

using namespace std;


bool RandomCache::lookup(const SimpleRequest &req) {
    return key_space.exist(req.id);
}

void RandomCache::admit(const SimpleRequest &req) {
    const uint64_t & size = req.size;
    // object feasible to store?
    if (size > _cacheSize) {
        LOG("L", _cacheSize, req.id, size);
        return;
    }
    // admit new object
    key_space.insert(req.id);
    object_size.insert({req.id, req.size});
    _currentSize += size;

    // check eviction needed
    while (_currentSize > _cacheSize) {
        evict();
    }
}

void RandomCache::evict() {
    auto key = key_space.pickRandom();
    key_space.erase(key);
    auto & size = object_size.find(key)->second;
    _currentSize -= size;
    object_size.erase(key);
}


