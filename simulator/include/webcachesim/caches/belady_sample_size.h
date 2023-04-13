//
//  on 12/17/18.
//

#ifndef WEBCACHESIM_BELADY_SAMPLESIZE_H
#define WEBCACHESIM_BELADY_SAMPLESIZE_H

#include <cache.h>
#include <unordered_map>
#include <cmath>
#include <random>
#include "mongocxx/client.hpp"
#include "mongocxx/uri.hpp"
#include <bsoncxx/builder/basic/document.hpp>
#include "bsoncxx/json.hpp"
#include <unordered_set>

using namespace std;
using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::sub_array;
using namespace webcachesim;

class BeladySampleSizeMeta {
public:
    uint64_t _key;
    uint64_t _size;
    uint64_t _past_timestamp;
    uint64_t _future_timestamp; 

    BeladySampleSizeMeta(const uint64_t & key, const uint64_t & size, const uint64_t & past_timestamp,
                     const uint64_t & future_timestamp) {
        _key = key;
        _size = size;
        _past_timestamp = past_timestamp;
        _future_timestamp = future_timestamp;
    }

    inline void update(const uint64_t &past_timestamp, const uint64_t &future_timestamp) {
        _past_timestamp = past_timestamp;
        _future_timestamp = future_timestamp;
    }
};


class BeladySampleSizeCache : public Cache
{
public:
    unordered_map<uint64_t, uint32_t> key_map;
    vector<BeladySampleSizeMeta> meta_holder;
    uint sample_rate = 32;
    uint64_t threshold = 1000000000;
    int64_t exceed_threshold = 0;
    int64_t total_exceed_threshold = 0;

    default_random_engine _generator = default_random_engine();
    uniform_int_distribution<std::size_t> _distribution = uniform_int_distribution<std::size_t>();
    uint64_t current_t;

    bool memorize_sample = false;
    unordered_set<uint64_t> memorize_sample_keys;

    void init_with_params(const map<string, string> &params) override {
        for (auto& it: params) {
            if (it.first == "sample_rate") {
                sample_rate = stoul(it.second);
                cerr<<"sample_rate:"<<endl;
                
            } else if (it.first == "threshold") {
                threshold = stoull(it.second);
            } else if (it.first == "memorize_sample") {
                memorize_sample = static_cast<bool>(stoi(it.second));
            }else if (it.first == "n_extra_fields") {
                cerr<<"n_extra_fields:"<<it.second<<endl;
            } else {
                cerr << "unrecognized parameter: " << it.first << endl;
            }
        }
    }

    void update_stat(bsoncxx::builder::basic::document &doc) override {
        doc.append(kvp("total_exceed_threshold", total_exceed_threshold));
    }

    void update_stat_periodic() override {
        cerr<<"exceed_threshold: "<<exceed_threshold<<endl;
        exceed_threshold=0;
    }

    bool lookup(const SimpleRequest &req) override;

    void admit(const SimpleRequest &req) override;

    void evict();
    //sample, rank the 1st and return
    pair<uint64_t, uint32_t> rank();
};

static Factory<BeladySampleSizeCache> factoryBeladySampleSize("BeladySampleSize");

#endif //WEBCACHESIM_BELADY_SAMPLESIZE_H
