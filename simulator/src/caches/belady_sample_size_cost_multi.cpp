#include "belady_sample_size_cost_multi.h"
#include "utils.h"

using namespace std;


bool BeladySampleSizeCostMultiCache::lookup(const SimpleRequest &_req) {
    auto & req = dynamic_cast<const AnnotatedCostRequest &>(_req);
    current_t = req.seq;
    /*
    key_map key->pos of vector
    */
    auto it = key_map.find(req.id);
    if (it != key_map.end()) {
        uint32_t &pos_idx = it->second;
        meta_holder[pos_idx].update(req.seq, req.next_seq);

        return true;
    }
    return false;
}

void BeladySampleSizeCostMultiCache::admit(const SimpleRequest &_req) {
    auto & req = static_cast<const AnnotatedCostRequest &>(_req);
    const uint64_t & size = req.size;
    // object feasible to store?
    if (size > _cacheSize) {
        LOG("L", _cacheSize, req.id, size);
        return;
    }

    if(is_bypss_cold_data && (req.next_seq[0] - current_t > memory_window)){
        return;
    }


    auto it = key_map.find(req.id);
    if (it == key_map.end()) {
        key_map.insert({req.id, (uint32_t) meta_holder.size()});
        meta_holder.emplace_back(req.id, req.size, req.seq, req.next_seq, req.cost);
        _currentSize += size;
    }else{
        cerr<<"admit error key:"<<req.id<<endl;
        exit(-1);
    }
    if(_currentSize > _cacheSize){
        int64_t cache_items = key_map.size();
        if(cache_items > max_cache_items){
            max_cache_items = cache_items;
        }
        if(cache_items < min_cache_items){
            min_cache_items = cache_items;
        }
    }

    while (_currentSize > _cacheSize) {
        evict();
    }
}


pair<uint64_t, uint32_t> BeladySampleSizeCostMultiCache::rank() {
    vector<pair<uint64_t, uint32_t >> beyond_boundary_key_pos;
    long double max_future_interval = 0.0;
    uint64_t max_key;
    uint32_t max_pos;

    uint n_sample = min(sample_rate, (uint32_t) meta_holder.size());

    for (uint32_t i = 0; i < n_sample; i++) {
        //true random sample
        uint32_t pos = (i + _distribution(_generator)) % meta_holder.size();
        auto &meta = meta_holder[pos];


        long double future_interval;
        if (meta._future_timestamps[0] - current_t <= threshold) {
            // future_interval = static_cast<double>((meta._future_timestamp - current_t)*meta._size) / meta._cost;
            long double tmpp = 0.0;
            for(int i=0;i<meta._future_timestamps.size();i++){
                if(meta._future_timestamps[i]>0 && (meta._future_timestamps[i] - current_t <= threshold)){
                    tmpp += 1/static_cast<double>(meta._future_timestamps[i]-current_t);
                }
            }
            tmpp = 1/ tmpp;
            future_interval = static_cast<long double>(meta._size * tmpp / meta._cost);
        } else {
            beyond_boundary_key_pos.emplace_back(pair(meta._key, pos));
            continue;
        }

        if (future_interval > max_future_interval) {
            max_future_interval = future_interval;
            max_key = meta._key;
            max_pos = pos;
        }
    }

    if (beyond_boundary_key_pos.empty()) {
        return {max_key, max_pos};
    } else {
        total_exceed_threshold++;
        exceed_threshold++;
        auto rand_id = _distribution(_generator) % beyond_boundary_key_pos.size();
        auto &item = beyond_boundary_key_pos[rand_id];
        return {item.first, item.second};
    }
}

void BeladySampleSizeCostMultiCache::evict() {
    auto epair = rank();
    uint64_t & key = epair.first;
    uint32_t & old_pos = epair.second;

    _currentSize -= meta_holder[old_pos]._size;

    double evict_rate_  = meta_holder[old_pos]._cost /(double)meta_holder[old_pos]._size;
    if(evict_max_size_cost_rate < 0 || evict_max_size_cost_rate < evict_rate_){
        evict_max_size_cost_rate = evict_rate_;
    }
    if(evict_min_size_cost_rate < 0 || evict_min_size_cost_rate > evict_rate_){
        evict_min_size_cost_rate = evict_rate_;
    }
    
    if(is_write_file){
        evict_size_cost_ratios.insert(evict_rate_);
        //fwrite_cost_size_ratio<<evict_rate_ <<endl;
    }

    if(total_evict_min_size_cost_rate < 0 || total_evict_min_size_cost_rate > evict_rate_){
        total_evict_min_size_cost_rate = evict_rate_;
    }
    if(total_evict_max_size_cost_rate < 0 || total_evict_max_size_cost_rate < evict_rate_){
        total_evict_max_size_cost_rate = evict_rate_;
    }
    
    uint32_t activate_tail_idx = meta_holder.size() - 1;
    if (old_pos !=  activate_tail_idx) {
        //move tail
        meta_holder[old_pos] = meta_holder[activate_tail_idx];
        key_map.find(meta_holder[activate_tail_idx]._key)->second = old_pos;
    }
    meta_holder.pop_back();

    key_map.erase(key);
}