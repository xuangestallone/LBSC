//
//  on 11/8/18.
//

#include <assert.h>
#include "request.h"
#include "belady.h"

bool BeladyCache::lookup(const SimpleRequest& _req) {
    auto &req = dynamic_cast<const AnnotatedRequest &>(_req);

    _next_req_map.emplace(req.next_seq, req.id);
    auto if_hit = _size_map.find(req.id) != _size_map.end();

    _next_req_map.erase(_req.seq);

#ifdef EVICTION_LOGGING
    {
        if (if_hit) {
            auto it = last_req_timestamps.find(req.id);
            assert(it != last_req_timestamps.end());
            unsigned int hit_distance =
                    static_cast<double>(req.seq - it->second) / (_cacheSize * 1e6 / byte_million_req);
            hit_distance = min((unsigned int) 255, hit_distance);
            hit_distances.emplace_back(hit_distance);
            it->second = req.seq;
        }
        current_t = req.seq;
    }
#endif

    return if_hit;
}

void BeladyCache::admit(const SimpleRequest& _req) {
    auto &req = dynamic_cast<const AnnotatedRequest &>(_req);
    const uint64_t & size = req.size;

    // object feasible to store?
    if (size > _cacheSize) {
        LOG("L", _cacheSize, req.id, size);
        return;
    }

    // admit new object
    _size_map.insert({req.id, req.size});
    _currentSize += size;

#ifdef EVICTION_LOGGING
    last_req_timestamps.insert({req.id, req.seq});
#endif

    // check eviction needed
    while (_currentSize > _cacheSize) {
        evict();
    }
}

void BeladyCache::evict() {
    auto it = _next_req_map.begin();
    auto iit = _size_map.find(it->second);
    if (iit != _size_map.end()) {
#ifdef EVICTION_LOGGING
        {   //record eviction decision quality
            unsigned int decision_qulity =
                    static_cast<double>(it->first - current_t) / (_cacheSize * 1e6 / byte_million_req);
            decision_qulity = min((unsigned int) 255, decision_qulity);
            eviction_distances.emplace_back(decision_qulity);
            last_req_timestamps.erase(it->second);
        }
#endif
        _currentSize -= iit->second;
        _size_map.erase(iit);
    }
    _next_req_map.erase(it);
}