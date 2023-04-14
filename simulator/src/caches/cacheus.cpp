#include "cacheus.h"
#include <algorithm>
#include "utils.h"
#include <chrono>

using namespace chrono;
using namespace std;
using namespace cacheus;


bool CACHEUSCache::lookup(const SimpleRequest &req) {
    bool miss = false;
    time += 1;
    /**
    test
    */
    if(q_size+s_size > _cacheSize){ 
        cerr<<"cur size: " << q_size+s_size << endl;
        cerr<<"cache size: " << _cacheSize << endl;
        cerr<<"exceed cache size too much" <<endl;
        exit(-1);
    }
    Cacheus_Entry evicted;

    learning_rate->update(req.size);

    if(s.isin(req.id)){
        hitinS(req.id);
    }else if(q.isin(req.id)){
        hitinQ(req.id);
    }else{
        miss = true;
    }

    if(!miss){
        learning_rate->byte_hitrate+=req.size;
    }

    return !miss;
}

void CACHEUSCache::miss(const uint64_t& _id, const uint64_t& size){
    miss_num++;
    int freq = 1;
    if(s_size<s_limit && q_size==0){
        addToS(_id, freq, false, size);
    }else if((s_size+q_size)<_cacheSize && q_size<q_limit){
        addToQ(_id, freq, false, size);
    }else{
        addToQ(_id, freq, true, size);
        limitStack();
    }

    while(s_size +q_size > _cacheSize){
        evict();
    }
}

void CACHEUSCache::admit(const SimpleRequest &req){
    if(lru_hist.isin(req.id)){
        hitinLRUHist(req.id, req.size);
    }else if(lfu_hist.isin(req.id)){
        hitinLFUHist(req.id, req.size);
    }else{
        miss(req.id, req.size);
    }

}

void CACHEUSCache::evict(){
    evict_num++;
    Cacheus_Entry evicted;
    Cacheus_Entry entry_lru = q.getOldest();
    Cacheus_Entry entry_lfu = lfu.getMin();

    int policy = getChoice();

    if(entry_lru.id == entry_lfu.id){
        policy = -1;
        evicted = entry_lru;
    }else if(policy == 0){
        evicted = entry_lru;
        q.erase(evicted.id);
        q_size -= evicted.size;
    }else if(policy == 1){
        evicted = entry_lfu;
        if(s.isin(evicted.id)){
            s.erase(evicted.id);
            s_size-=evicted.size;
        }else if(q.isin(evicted.id)){
            q.erase(evicted.id);
            q_size -= evicted.size;
        }
    }

    if(evicted.is_demoted){
        dem_count-=1;
        evicted.is_demoted = false;
    }

    if(policy == -1){
        q.erase(evicted.id);
        q_size -= evicted.size;
    }

    lfu.erase(evicted.id);
    evicted.evicted_time = time;
    addToHistory(evicted, policy);

}
 
void CACHEUSCache::update_stat_periodic(){
    _currentSize = s_size + q_size;
    cerr << "cache size: " << _currentSize << "/" << _cacheSize <<endl;
}