//
//  on 11/8/18.
//

#ifndef WEBCACHESIM_BELADYSIZE_H
#define WEBCACHESIM_BELADYSIZE_H

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
class BeladySizeCache : public Cache {
protected:
    unordered_map<uint64_t, uint64_t> _size_map;
    ValueMapType _valueMap;
    // find objects via unordered_map
    BeladyCacheMapType _cacheMap;

public:

    //request���¼�´η��ʵ�index
    bool lookup(const SimpleRequest &req) override;

    void admit(const SimpleRequest &req) override;

    void evict();

    bool has(const uint64_t &id) override { return _size_map.find(id) != _size_map.end(); }
};

static Factory<BeladySizeCache> factoryBeladySize("BeladySize");


#endif //WEBCACHESIM_BELADYSIZE_H
