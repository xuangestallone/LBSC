#ifndef WEBCACHESIM_BELADY_SAMPLESIZECOSTMULTI_H
#define WEBCACHESIM_BELADY_SAMPLESIZECOSTMULTI_H

#include <cache.h>
#include <unordered_map>
#include <cmath>
#include <random>
#include "mongocxx/client.hpp"
#include "mongocxx/uri.hpp"
#include <bsoncxx/builder/basic/document.hpp>
#include "bsoncxx/json.hpp"
#include <unordered_set>
#include <fstream>
#include <set>

using namespace std;
using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::sub_array;
using namespace webcachesim;

class BeladySampleSizeCostMultiMeta {
public:
    uint64_t _key;
    uint64_t _size;
    uint64_t _past_timestamp;
    vector<int64_t> _future_timestamps; 
    uint64_t _cost;

    BeladySampleSizeCostMultiMeta(const uint64_t & key, const uint64_t & size, const uint64_t & past_timestamp,
                     const vector<int64_t> & future_timestamp, const uint64_t& cost) {
        _key = key;
        _size = size;
        _past_timestamp = past_timestamp;
        _future_timestamps.resize(future_timestamp.size());
        for(int i=0;i<future_timestamp.size();i++){
            _future_timestamps[i]=future_timestamp[i];
        }
        _cost = cost;
    }

    inline void update(const uint64_t &past_timestamp, const vector<int64_t> &future_timestamp) {
        _past_timestamp = past_timestamp;
        for(int i=0;i<future_timestamp.size();i++){
            _future_timestamps[i]=future_timestamp[i];
        }
    }
};


class BeladySampleSizeCostMultiCache : public Cache
{
public:
    unordered_map<uint64_t, uint32_t> key_map;
    vector<BeladySampleSizeCostMultiMeta> meta_holder;

    uint sample_rate = 32;
    uint64_t threshold = 1000000000;
    int64_t exceed_threshold = 0;
    int64_t total_exceed_threshold = 0;
    int64_t max_cache_items = -1;
    int64_t min_cache_items = 100000000;

    default_random_engine _generator = default_random_engine();
    uniform_int_distribution<std::size_t> _distribution = uniform_int_distribution<std::size_t>();
    uint64_t current_t;

    bool is_bypss_cold_data = false;
    bool is_write_file = false;
    uint32_t memory_window = 67108864;

    ofstream fwrite_cost_size_ratio;

    double evict_max_size_cost_rate = -1.0;
    double evict_min_size_cost_rate = -1.0;
    double total_evict_max_size_cost_rate = -1.0;
    double total_evict_min_size_cost_rate = -1.0;
    set<long double> evict_size_cost_ratios;

    void init_with_params(const map<string, string> &params) override {

        for (auto& it: params) {
            if (it.first == "sample_rate") {
                sample_rate = stoul(it.second);
                cerr<<"sample_rate:"<<sample_rate<<endl;
            } else if (it.first == "threshold") {
                threshold = stoull(it.second);
            } else if(it.first == "is_bypss_cold_data"){
                uint uint_is_bypss_cold_data = stoul(it.second);
                if(uint_is_bypss_cold_data != 0){
                    is_bypss_cold_data = true;
                }else{
                    is_bypss_cold_data = false;
                }
                cerr << "is_bypss_cold_data: " << is_bypss_cold_data <<endl;
            }
            else if(it.first == "is_write_file"){
                uint uint_is_write_file = stoul(it.second);
                if(uint_is_write_file != 0){
                    is_write_file = true;
                }else{
                    is_write_file = false;
                }
                cerr << "is_write_file: " << is_write_file <<endl;
            }
            else if (it.first == "n_extra_fields") {
                cerr<<"n_extra_fields:"<<it.second<<endl;
            } else {
                cerr << "unrecognized parameter: " << it.first << endl;
            }
        }
        if(is_write_file){
            fwrite_cost_size_ratio.open("BSCMulti_cost_size_"+to_string(_cacheSize)+"_"+to_string(sample_rate)+".txt");
        }
    }

    void update_stat(bsoncxx::builder::basic::document &doc) override {
        if(is_write_file){
            for(auto it= evict_size_cost_ratios.begin();it!=evict_size_cost_ratios.end();it++){
                fwrite_cost_size_ratio<<*it<<endl;
            }
            fwrite_cost_size_ratio.close();
        }

        doc.append(kvp("total_exceed_threshold", total_exceed_threshold));
        doc.append(kvp("total_evict_max_size_cost_rate", to_string(total_evict_max_size_cost_rate)));
        doc.append(kvp("total_evict_min_size_cost_rate", to_string(total_evict_min_size_cost_rate)));
    }

    void update_stat_periodic() override {
        cerr<< "evict_max_size_cost_rate: "<<evict_max_size_cost_rate<<endl
            << "evict_min_size_cost_rate: "<<evict_min_size_cost_rate<<endl; 
        cerr<<"exceed_threshold: "<<exceed_threshold<<endl;
        exceed_threshold=0;
        cerr<<"max_cache_items:"<<max_cache_items<<"  min_cache_items:"<<min_cache_items<<endl;
        max_cache_items=-1;
        min_cache_items=100000000;

        evict_max_size_cost_rate = -1.0;
        evict_min_size_cost_rate = -1.0;
    }

    bool lookup(const SimpleRequest &req) override;

    void admit(const SimpleRequest &req) override;

    void evict();
    pair<uint64_t, uint32_t> rank();
};

static Factory<BeladySampleSizeCostMultiCache> factoryBeladySampleSizeCostMulti("BeladySampleSizeCostMulti");

#endif //WEBCACHESIM_BELADY_SAMPLESIZECOSTMULTI_H
