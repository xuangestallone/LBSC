//
//  on 11/8/18.
//

#include <assert.h>
#include "request.h"
#include "belady_size.h"

bool BeladySizeCache::lookup(const SimpleRequest& _req) {
    auto &req = dynamic_cast<const AnnotatedRequest &>(_req);
    auto if_hit = _size_map.find(req.id) != _size_map.end();
    auto iter = _cacheMap.find(req.id);
    if(if_hit){
        if(iter == _cacheMap.end()){
            cerr<<req.id<< "not in cache error"<<endl;
            exit(-1);
        }
        uint64_t cachedObj = iter->first;
        ValueMapIteratorType si = iter->second;
        _valueMap.erase(si);
        long double rank = static_cast<double>(req.next_seq*_req.size);
        iter->second = _valueMap.emplace(rank, cachedObj);
    }else{
        if(iter != _cacheMap.end()){
            cerr<<req.id<< " in cache error"<<endl;
            exit(-1);
        }
    }
    return if_hit;
}

void BeladySizeCache::admit(const SimpleRequest& _req) {
    auto &req = dynamic_cast<const AnnotatedRequest &>(_req);
    const uint64_t & size = req.size;

    if (size > _cacheSize) {
        LOG("L", _cacheSize, req.id, size);
        return;
    }

    auto & obj = req.id;
    _size_map[obj] = size;
    long double rank = static_cast<double>(req.next_seq*_req.size);
    _cacheMap[obj] = _valueMap.emplace(rank, obj);
    _currentSize += size;
    while (_currentSize > _cacheSize) {
        evict();
    }

}

void BeladySizeCache::evict() {
    if (_valueMap.size() > 0) {
        ValueMapIteratorType lit  = _valueMap.begin();
        if (lit == _valueMap.end()) {
            std::cerr << "underun: " << _currentSize << ' ' << _cacheSize << std::endl;
        }
        assert(lit != _valueMap.end()); 
        uint64_t toDelObj = lit->second;

        auto size = _size_map[toDelObj];
        _currentSize -= size;
        _cacheMap.erase(toDelObj);
        _size_map.erase(toDelObj);
        _valueMap.erase(lit);
    }
}