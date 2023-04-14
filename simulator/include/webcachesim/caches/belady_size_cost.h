#ifndef WEBCACHESIM_BELADYSIZECOST_H
#define WEBCACHESIM_BELADYSIZECOST_H

#include "cache.h"
#include <utils.h>
#include <unordered_map>
#include <map>

using namespace std;
using namespace webcachesim;

typedef std::multimap<long double, uint64_t, greater<long double >> ValueMapType;
typedef ValueMapType::iterator ValueMapIteratorType;
/*
key -> rank
*/
typedef std::unordered_map<uint64_t, ValueMapIteratorType> BeladyCacheMapType;
/*
  Belady: Optimal for unit size 
*/
class BeladySizeCostCache : public Cache {
protected:
    unordered_map<uint64_t, uint64_t> _size_map;
    ValueMapType _valueMap;
    // find objects via unordered_map
    BeladyCacheMapType _cacheMap;

public:

    bool lookup(const SimpleRequest &req) override;

    void admit(const SimpleRequest &req) override;

    void evict();

    bool has(const uint64_t &id) override { return _size_map.find(id) != _size_map.end(); }
    
};

static Factory<BeladySizeCostCache> factoryBeladySizeCost("BeladySizeCost");


#endif //WEBCACHESIM_BELADYSIZECOST_H
