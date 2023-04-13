#ifndef WEBCACHESIM_SIMULATION_H
#define WEBCACHESIM_SIMULATION_H

#include <string>
#include <chrono>
#include <fstream>
#include <vector>
#include <bsoncxx/builder/basic/document.hpp>
#include <unordered_set>
#include "cache.h"
#include "bsoncxx/document/view.hpp"
#include "bloom_filter.h"
#include <set>


/*
 * single thread simulation. Returns results. 
 */
bsoncxx::builder::basic::document simulation(std::string trace_file, std::string cache_type,
                                             uint64_t cache_size, std::map<std::string, std::string> params);

std::vector<std::string> split(const std::string& str, const std::string& splitStr);

using namespace webcachesim;

class FrameWork {
public:
    bool uni_size = false;
    uint64_t segment_window = 1000000;
    //unit: second
    uint64_t real_time_segment_window = 600;
    uint n_extra_fields = 0;
    bool is_metadata_in_cache_size = false;
    unique_ptr<Cache> webcache = nullptr;
    std::ifstream infile;
    int64_t n_early_stop = -1;  //-1: no stop
    int64_t seq_start = 0;

    std::string _trace_file;
    std::string _cache_type;
    uint64_t _cache_size;

    int64_t _delta_ratio;
    int64_t _fixed_byte;
    int64_t _min_ratio;

    int _offline_num;
    int _cost_threshold;
    int _offline_threshold;
    const unordered_set<string> offline_algorithms = {"Belady", "BeladySample", "RelaxedBelady", "BinaryRelaxedBelady","BeladySampleSizeCostMulti",
                                                      "PercentRelaxedBelady","BeladySize","BeladySizeCost","BeladySampleSize", "BeladySampleSizeCost"};
    const unordered_set<string> cost_algorithms = {"Belady","BeladySize", "BeladySizeCost", "BeladySample",
                                                    "BeladySampleSize", "BeladySampleSizeCost"
                                                    "GDSF","GDWheel", "GDSFCost", "GDWheelCost","CACHEUS",
                                                    "LRU","ThLRU","LRUK","LFUDA","S4LRU","ThS4LRU",
                                                    "Hyperbolic","FIFO",
                                                    "LHD","AdaptSize","UCB",
                                                    "LeCaR","LRB","BeladySampleSizeCostMulti",
                                                    "CostSize","LBSC"
                                                    };
    bool is_offline;
    bool is_cost;
    bool is_statistical = false;
    double min_cost_size = -1.0;
    double max_cost_size = -1.0;
    ofstream write_cost_size_ratio;
    multiset<long double> size_cost_ratios;

    uint64_t min_size = 0;
    uint64_t max_size = 0;
    set<uint64_t> size_dist;
    ofstream write_size_dist;

    uint64_t min_re_dist = 0;
    uint64_t max_re_dist = 0;
    map<uint64_t, uint32_t> re_dist_ratios;
    ofstream write_re_dist;

    unordered_map<uint64_t, uint32_t> m_popular;
    ofstream write_popular;

    ofstream write_map_obj;
    unordered_map<uint64_t, uint32_t> m_map_obj;


    ofstream write_reuse_size_cost_used;
    unordered_map<uint64_t, uint64_t> key_last_size;
    unordered_map<uint64_t, uint64_t> key_last_saving;
    map<uint64_t, uint64_t> size_cost_distance;

    bool bloom_filter = false;
    AkamaiBloomFilter *filter;


    int64_t t, id, size, usize, next_seq;
    int64_t cost=0;
    int same_chunk;
    int64_t read_size = 0;
    int64_t segment_read_size = 0;
    int64_t byte_req = 0, byte_miss = 0, obj_req = 0, obj_miss = 0;
    int64_t rt_byte_req = 0, rt_byte_miss = 0, rt_obj_req = 0, rt_obj_miss = 0;
    std::vector<int64_t> seg_byte_req, seg_byte_miss, seg_object_req, seg_object_miss;
    std::vector<int64_t> seg_rss;
    std::vector<int64_t> seg_byte_in_cache;
    std::vector<int64_t> rt_seg_byte_req, rt_seg_byte_miss, rt_seg_object_req, rt_seg_object_miss;
    std::vector<int64_t> rt_seg_rss;
    uint64_t time_window_end;
    std::vector<uint16_t> extra_features;
    uint64_t seq = 0;
    std::chrono::system_clock::time_point t_now;
    int64_t interval_get_time=0;
    int64_t total_get_time=0;
    int64_t total_get_time_no_threshold=0;


    FrameWork(const std::string &trace_file, const std::string &cache_type, const uint64_t &cache_size,
              std::map<std::string, std::string> &params);

    bsoncxx::builder::basic::document simulate();

    bsoncxx::builder::basic::document simulation_results();

    void adjust_real_time_offset();

    void update_real_time_stats();

    void update_stats();
};



#endif //WEBCACHESIM_SIMULATION_H
