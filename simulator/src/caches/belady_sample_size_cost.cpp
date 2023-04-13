#include "belady_sample_size_cost.h"
#include "utils.h"

using namespace std;


bool BeladySampleSizeCostCache::lookup(const SimpleRequest &_req) {
    auto & req = dynamic_cast<const AnnotatedRequest &>(_req);
    current_t = req.seq;
    auto it = key_map.find(req.id);
    if (it != key_map.end()) {
        uint32_t &pos_idx = it->second;
        meta_holder[pos_idx].update(req.seq, req.next_seq);
        return true;
    }
    return false;
}

void BeladySampleSizeCostCache::admit(const SimpleRequest &_req) {
    auto & req = static_cast<const AnnotatedRequest &>(_req);
    const uint64_t & size = req.size;
    // object feasible to store?
    if (size > _cacheSize) {
        LOG("L", _cacheSize, req.id, size);
        return;
    }

    if(is_bypss_cold_data && (req.next_seq - current_t > memory_window)){
        return;
    }


    auto it = key_map.find(req.id);
    if (it == key_map.end()) {
        key_map.insert({req.id, (uint32_t) meta_holder.size()});
        meta_holder.emplace_back(req.id, req.size, req.seq, req.next_seq, req.cost);
        _currentSize += size;
    }
    while (_currentSize > _cacheSize) {
        evict();
    }
}


pair<uint64_t, uint32_t> BeladySampleSizeCostCache::rank() {
    vector<pair<uint64_t, uint32_t >> beyond_boundary_key_pos;
    long double max_future_interval = 0.0;
    uint64_t max_key;
    uint32_t max_pos;

    uint n_sample = min(sample_rate, (uint32_t) meta_holder.size());

    for (uint32_t i = 0; i < n_sample; i++) {
        uint32_t pos = (i + _distribution(_generator)) % meta_holder.size();
        auto &meta = meta_holder[pos];

        long double future_interval;
        if (meta._future_timestamp - current_t <= threshold) {
            future_interval = static_cast<double>((meta._future_timestamp - current_t)*meta._size) / meta._cost;
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

    if(max_rank < max_future_interval){
        max_rank = max_future_interval;
    }
    if(total_max_rank < max_future_interval){
        total_max_rank = max_future_interval;
    }
    if(min_rank < 0 || min_rank > max_future_interval){
        min_rank = max_future_interval;
    }
    if(total_min_rank < 0 || total_min_rank > max_future_interval){
        total_min_rank = max_future_interval;
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

void BeladySampleSizeCostCache::evict() {
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
    }

    if(total_evict_min_size_cost_rate < 0 || total_evict_min_size_cost_rate > evict_rate_){
        total_evict_min_size_cost_rate = evict_rate_;
    }
    if(total_evict_max_size_cost_rate < 0 || total_evict_max_size_cost_rate < evict_rate_){
        total_evict_max_size_cost_rate = evict_rate_;
    }

    
    uint32_t activate_tail_idx = meta_holder.size() - 1;
    if (old_pos !=  activate_tail_idx) {
        meta_holder[old_pos] = meta_holder[activate_tail_idx];
        key_map.find(meta_holder[activate_tail_idx]._key)->second = old_pos;
    }
    meta_holder.pop_back();

    key_map.erase(key);
}