#ifndef WEBCACHESIM_LBSC_H
#define WEBCACHESIM_LBSC_H

#include "cache.h"
#include <unordered_map>
#include <unordered_set>
#include <set>
#include "sparsepp/sparsepp/spp.h"  
#include <vector>
#include <random>
#include <cmath>
#include <LightGBM/c_api.h>
#include <assert.h> 
#include <fstream>
#include <list>
#include "mongocxx/client.hpp"
#include "mongocxx/uri.hpp"
#include <bsoncxx/builder/basic/document.hpp>
#include "bsoncxx/json.hpp"
#include <chrono>
#include <map>
#include <Eigen/Dense>

#include <array>

#define MAX_RANK 4000000000
using namespace chrono;

using namespace webcachesim;
using spp::sparse_hash_map;
using namespace std;
using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::sub_array;

namespace lbsc{
    uint32_t current_seq = -1; 
    uint8_t max_n_past_timestamps = 32;
    uint8_t max_n_past_distances = 31;
    uint8_t base_edc_window = 10;
    const uint8_t n_edc_feature = 10; 
    vector<uint32_t> edc_windows;
    vector<double> hash_edc;
    uint32_t max_hash_edc_idx;
    uint32_t memory_window = 67108864; 
    uint32_t n_extra_fields = 0;
    uint32_t batch_size = 131072; 
    const uint max_n_extra_feature = 4; 
    uint32_t n_feature;

    bool is_cost_size_sample = false; 
    double cost_size_threshol = 0.5;

    bool is_dynamic_cost_size_threshold=false;

    bool is_use_kl=false;
    bool is_use_js=false;
    double kl_threshold=1.0;

    bool is_optimize_train = false;
    bool is_bypss_cold_data = false; 
    bool add_new_features = false; 
    bool is_use_decay = false;  

struct MetaExtra { 
    float _edc[10]; 
    vector<uint32_t> _past_distances; 
    list<uint64_t> _past_timestamps;
    uint8_t _past_distance_idx = 1; 

    MetaExtra(const uint32_t &distance, const uint64_t &timestamp) {
        _past_distances = vector<uint32_t>(1, distance);
        _past_timestamps.emplace_back(timestamp);
        for (uint8_t i = 0; i < n_edc_feature; ++i) { 
            uint32_t _distance_idx = min(uint32_t(distance / edc_windows[i]), max_hash_edc_idx);
            _edc[i] = hash_edc[_distance_idx] + 1;
        }
    }

    void update(const uint32_t &distance, const uint64_t &timestamp) {
        uint8_t distance_idx = _past_distance_idx % max_n_past_distances;
        _past_timestamps.emplace_back(timestamp);
        if(timestamp-*(_past_timestamps.begin())>memory_window){
            _past_timestamps.pop_front();
        }
        if (_past_distances.size() < max_n_past_distances){
            _past_distances.emplace_back(distance);
        }
        else{
            _past_distances[distance_idx] = distance;
        }
        assert(_past_distances.size() <= max_n_past_distances);
        _past_distance_idx = _past_distance_idx + (uint8_t) 1;
        if (_past_distance_idx >= max_n_past_distances * 2)
            _past_distance_idx -= max_n_past_distances;
        for (uint8_t i = 0; i < n_edc_feature; ++i) {
            uint32_t _distance_idx = min(uint32_t(distance / edc_windows[i]), max_hash_edc_idx);
            _edc[i] = _edc[i] * hash_edc[_distance_idx] + 1;
        }
    }
};   


class Meta {  
public:
    uint64_t _key; //8
    uint64_t _size; // 4
    uint32_t _cost;
    uint32_t _past_timestamp; // 4
    uint16_t _extra_features[max_n_extra_feature];
    MetaExtra *_extra = nullptr;
    uint32_t _freq;

    Meta(const uint64_t &key, const uint64_t &size, const uint64_t &cost, const uint64_t &past_timestamp,
            const vector<uint16_t> &extra_features) {
        _key = key;
        _size = size;//key size
        _cost = cost;
        _past_timestamp = past_timestamp;
        _freq = 1;
        for (int i = 0; i < n_extra_fields; ++i)
            _extra_features[i] = extra_features[i];
    }

    virtual ~Meta() = default;

    void free() {
        delete _extra;
    }

    void update(const uint32_t &past_timestamp, const uint32_t &freq) {
        //distance
        uint32_t _distance = past_timestamp - _past_timestamp;
        assert(_distance);
        if (!_extra) {
            _extra = new MetaExtra(_distance, past_timestamp);
        } else
            _extra->update(_distance, past_timestamp);
        _past_timestamp = past_timestamp;
        _freq = freq;
    }

    uint32_t metaSize(){
        if(_extra){
            return _extra->_past_timestamps.size();
        }else{
            return 0;
        }
        
    }

};

class TrainingData {
public:
    vector<float> labels;
    vector<int32_t> indptr;
    vector<int32_t> indices;
    vector<double> data;
    uint32_t outlier_num;

    TrainingData() {
        labels.reserve(batch_size);  
        indptr.reserve(batch_size + 1);
        indptr.emplace_back(0);
        indices.reserve(batch_size * n_feature);
        data.reserve(batch_size * n_feature);  
        outlier_num=0;
    }

    void emplace_back(shared_ptr<Meta>& meta, uint32_t &future_distance, const uint64_t &key) {
        int32_t counter = indptr.back();
        uint32_t this_past_distance = 0;
        int j = 0;
        uint8_t n_within = 0;
        if (meta->_extra) {
            if(is_optimize_train){
                if((future_distance<memory_window) && 
                (meta->_extra->_past_distances[(meta->_extra->_past_distance_idx - 1) % max_n_past_distances] > memory_window)){
                    outlier_num++;
                    return; 
                }
            }
            for (; j < meta->_extra->_past_distance_idx && j < max_n_past_distances; ++j) {
                uint8_t past_distance_idx = (meta->_extra->_past_distance_idx - 1 - j) % max_n_past_distances;
                const uint32_t &past_distance = meta->_extra->_past_distances[past_distance_idx];
                this_past_distance += past_distance;
                indices.emplace_back(j + 1);
                data.emplace_back(past_distance);
                if (this_past_distance < memory_window) {
                    ++n_within;
                }
            }
        }

        counter += j;

        indices.emplace_back(max_n_past_timestamps);
        data.push_back(meta->_size);
        ++counter;

        for (int k = 0; k < n_extra_fields; ++k) {
            indices.push_back(max_n_past_timestamps + k + 1);
            data.push_back(meta->_extra_features[k]); 
        }
        counter += n_extra_fields;

        indices.push_back(max_n_past_timestamps + n_extra_fields + 1);
        data.push_back(n_within);
        ++counter;

        if (meta->_extra) {
            for (int k = 0; k < n_edc_feature; ++k) {
                indices.push_back(max_n_past_timestamps + n_extra_fields + 2 + k);
                data.push_back(meta->_extra->_edc[k]);
            }
        } else {
            for (int k = 0; k < n_edc_feature; ++k) {
                indices.push_back(max_n_past_timestamps + n_extra_fields + 2 + k);
                data.push_back(hash_edc[max_hash_edc_idx]);
            }
        }
        counter += n_edc_feature;

        if(add_new_features){
            indices.push_back(max_n_past_distances + n_extra_fields + n_edc_feature + 2);
            data.push_back(static_cast<double>(meta->_cost)/static_cast<double>(meta->_size));
            ++counter;

            indices.push_back(max_n_past_distances + n_extra_fields + n_edc_feature + 3);
            data.push_back(static_cast<double>(meta->_freq));
            ++counter;
        }

        labels.push_back(log1p(future_distance));
        indptr.push_back(counter);
    }

    void clear() {
        labels.clear();
        indptr.resize(1);
        indices.clear();
        data.clear();
        outlier_num=0;
    }
};

typedef std::multimap<long double, pair<uint64_t, shared_ptr<Meta>>> ValueMapType;
typedef ValueMapType::iterator ValueMapIteratorType;
typedef std::unordered_map<uint64_t, ValueMapIteratorType> GdCacheMapType;//key -> ValueMapType iterator

typedef std::unordered_map<uint64_t, uint64_t> CacheStatsMapType;

typedef std::multimap<long double, uint64_t> ValueMapTypeTmp;
typedef ValueMapTypeTmp::iterator ValueMapIteratorTypeTmp;
typedef std::unordered_map<uint64_t, ValueMapIteratorTypeTmp> GdCacheMapTypeTmp;


struct KeyMapEntryT {
    unsigned int list_idx: 1;
    unsigned int list_pos: 31;
};

class LBSCCache : public Cache{
public:
    unordered_set<uint64_t> cold_data;

    vector<shared_ptr<Meta>> in_cache_metas; 
    vector<shared_ptr<Meta>> out_cache_metas;
    unordered_map<uint64_t, KeyMapEntryT> key_pos;

    vector<shared_ptr<Meta>> in_cache_metas_threshold_up;
    vector<shared_ptr<Meta>> in_cache_metas_threshold_low;
    unordered_map<uint64_t, KeyMapEntryT> key_pos_thresholds;


    ValueMapTypeTmp _gdsfInferenceValueMap;
    uint32_t _gdsfInferenceValueMapMaxSize = 0;
    GdCacheMapTypeTmp _gdsfInferenceCacheMap;
    uint32_t evict_num = 0;

    unordered_map<uint64_t, double> key_sets;

    ValueMapType _valueMap;
    GdCacheMapType _cacheMap;
    CacheStatsMapType _reqsMap;

    long double _currentL = 0;  
    unordered_map<uint64_t , uint64_t > _sizemap;
    double max_size_cost_rate = -1.0;
    double min_size_cost_rate = -1.0;


    double evict_max_size_cost_rate = -1.0;
    double evict_min_size_cost_rate = -1.0;

    double min_cost_size_ratio = -1.0;
    double max_cost_size_ratio = -1.0;

    double min_js = -1.0;
    double max_js = -1.0;


    int bypass_num = 0;
    int eviction_num = 0;

    uint32_t evict_max_last_distance = 0;
    uint32_t evict_min_last_distance = memory_window;

    uint32_t max_sample_rate=0;
    uint32_t min_sample_rate;

    TrainingData *training_data;

    double training_loss = 0;
    vector<double> training_losses;
    vector<uint32_t> training_data_size;
    vector<uint32_t> caches_data_size;
    vector<uint32_t> outlier_num_size;
    int32_t n_force_eviction = 0;

    bool have_trained;
    bool start_collect_train_data;

    double min_kl_threshold = -1.0;
    double max_kl_threshold = -1.0;
    uint32_t min_vec_kl_size = 0;
    uint32_t max_vec_kl_size = 0;

    uint64_t total_inference_time = 0;
    uint64_t total_inference_add_feature_time = 0;
    uint64_t total_training_time = 0;

    double training_time = 0;
    double inference_time = 0;
    double inferenct_sort_time = 0;
    uint64_t total_inference_sort_time = 0;
    double inference_add_feature_time = 0;

    uint sample_rate = 64;
    uint sample_rate_to_low = 12800;
    uint64_t sample_num = 0;
    system_clock::time_point interval_timeBegin = chrono::system_clock::now();;

    default_random_engine _generator = default_random_engine();
    uniform_int_distribution<std::size_t> _distribution = uniform_int_distribution<std::size_t>(); 

    BoosterHandle booster = nullptr;

    double calKL(int index);
    double calJS(int index);
    double calJS2(int index);
    vector<BoosterHandle> boosters;
    vector<pair<double,double>> zipfinPara;
    vector<vector<double>> klparas;


    uint32_t kl_sample_num = 2000;
    double currentA=0.0;
    double currentB=0.0;
    vector<double> currentPars;
    unordered_map<uint64_t, uint32_t> key_nums;

    unordered_map<string, string> training_params = {
            {"boosting",         "gbdt"},
            {"objective",        "regression"},
            {"num_iterations",   "32"},
            {"num_leaves",       "32"},
            {"num_threads",      "4"},
            {"feature_fraction", "0.8"},
            {"bagging_freq",     "5"},
            {"bagging_fraction", "0.8"},
            {"learning_rate",    "0.1"},
            {"verbosity",        "0"},
    };

    unordered_map<string, string> inference_params;

    int n_retrain;
    int n_inference;

    vector<int> segment_n_retrain;
    vector<int32_t> segment_n_in;
    vector<int> segment_n_out;


    bool need_train();
    bool need_train2();
    bool need_train3();
    bool lookup(const SimpleRequest &req) override;
    bool _lookup(const SimpleRequest &req);
    long double ageValue(const SimpleRequest& req);
    long double ageValueRank(const SimpleRequest& req);
    void hit(const SimpleRequest& req);

    void admit(const SimpleRequest &req) override;

    void evict(const SimpleRequest &req);

    void train();

    pair<uint64_t, KeyMapEntryT> rank();
    void sample_move_to_low();
    void move_to_low();

    void update_stat_periodic() override;


    bool start_sample=true;
    bool is_change_threshold=false;
    uint32_t tmp_evict_num=0;
    uint32_t evict_num_threshold=1000;
    uint32_t tmp_start_sample_num=0;
    uint32_t start_sample_threshold=1000;

    void init_with_params(const map<string, string> &params) override {
        for (auto &it: params) {
            if (it.first == "sample_rate") {
                sample_rate = stoul(it.second);
                cerr << "sample_rate: " << sample_rate <<endl;
            }
            else if (it.first == "sample_rate_to_low") {
                sample_rate_to_low = stoul(it.second);
                cerr << "sample_rate_to_low: " << sample_rate_to_low <<endl;
            }
            
            else if (it.first == "memory_window") {
                memory_window = stoull(it.second);
                cerr << "memory_window: " << memory_window << endl;
            } else if (it.first == "max_n_past_timestamps") {
                max_n_past_timestamps = (uint8_t) stoi(it.second);
                cerr << "max_n_past_timestamps: " << max_n_past_timestamps << endl;
            } else if (it.first == "batch_size") {
                batch_size = stoull(it.second);
                cerr << "batch_size: " << batch_size << endl;
            } else if (it.first == "n_extra_fields") {
                n_extra_fields = stoull(it.second);
                cerr << "n_extra_fields: " << n_extra_fields << endl;
            } else if (it.first == "num_iterations") {
                training_params["num_iterations"] = it.second;
                cerr << "num_iterations: " << training_params["num_iterations"] << endl;
            } else if (it.first == "learning_rate") {
                training_params["learning_rate"] = it.second;
                cerr << "learning_rate: " << training_params["learning_rate"] << endl;
            } else if (it.first == "num_threads") {
                training_params["num_threads"] = it.second;
                cerr << "num_threads: " << training_params["num_threads"] << endl;
            } else if (it.first == "num_leaves") {
                training_params["num_leaves"] = it.second;
                cerr << "num_leaves: " << training_params["num_leaves"] << endl;
            } else if(it.first == "add_new_features"){
                uint uint_add_new_features = stoul(it.second);
                if(uint_add_new_features!=0){
                    add_new_features = true;
                }else{
                    add_new_features = false;
                }
                cerr << "add_new_features: " << add_new_features <<endl;
            }

            
            else if(it.first == "is_cost_size_sample"){
                uint uint_is_cost_size_sample = stoul(it.second);
                if(uint_is_cost_size_sample!=0){
                    is_cost_size_sample = true;
                }else{
                    is_cost_size_sample = false;
                }
                cerr << "is_cost_size_sample: " << is_cost_size_sample <<endl;
            }

            else if(it.first == "is_dynamic_cost_size_threshold"){
                uint uint_is_dynamic_cost_size_threshold = stoul(it.second);
                if(uint_is_dynamic_cost_size_threshold!=0){
                    is_dynamic_cost_size_threshold = true;
                }else{
                    is_dynamic_cost_size_threshold = false;
                }
                cerr << "is_dynamic_cost_size_threshold: " << is_dynamic_cost_size_threshold <<endl;
            }
            
            else if(it.first == "is_optimize_train"){
                uint uint_is_optimize_train = stoul(it.second);
                if(uint_is_optimize_train!=0){
                    is_optimize_train = true;
                }else{
                    is_optimize_train = false;
                }
                cerr << "is_optimize_train: " << is_optimize_train <<endl;
            }
            else if(it.first == "is_bypss_cold_data"){
                uint uint_is_bypss_cold_data = stoul(it.second);
                if(uint_is_bypss_cold_data != 0){
                    is_bypss_cold_data = true;
                }else{
                    is_bypss_cold_data = false;
                }
                cerr << "is_bypss_cold_data: " << is_bypss_cold_data <<endl;
            }
            else if(it.first == "is_use_decay"){
                uint uint_is_use_decay = stoul(it.second);
                if(uint_is_use_decay != 0){
                    is_use_decay = true;
                }else{
                    is_use_decay = false;
                }
                cerr << "is_use_decay: " << is_use_decay <<endl;
            }
            
            else if(it.first == "evict_num_threshold"){
                evict_num_threshold = stod(it.second);
                cerr << "evict_num_threshold: "<<evict_num_threshold<<endl;
            }
            else if(it.first == "start_sample_threshold"){
                start_sample_threshold = stod(it.second);
                cerr << "start_sample_threshold: "<<start_sample_threshold<<endl;
            }

            else if(it.first == "cost_size_threshol"){
                cost_size_threshol = stod(it.second);
                cerr << "cost_size_threshol: "<<cost_size_threshol<<endl;
            }


            else if(it.first == "is_use_kl"){
                uint uint_is_use_kl = stoul(it.second);
                if(uint_is_use_kl != 0){
                    is_use_kl = true;
                }else{
                    is_use_kl = false;
                }
                cerr << "is_use_kl: " << is_use_kl <<endl;
            }
            else if(it.first == "kl_threshold"){
                kl_threshold = stod(it.second);
                cerr << "kl_threshold: "<<kl_threshold<<endl;
            }

            else if(it.first == "kl_sample_num"){
                kl_sample_num = stod(it.second);
                cerr << "kl_sample_num: "<<kl_sample_num<<endl;
            }

            else if(it.first == "is_use_js"){
                uint uint_is_use_js = stoul(it.second);
                if(uint_is_use_js != 0){
                    is_use_js = true;
                }else{
                    is_use_js = false;
                }
                cerr << "is_use_js: " << is_use_js <<endl;
            }

            else if (it.first == "n_edc_feature") {
                if (stoull(it.second) != n_edc_feature) {
                    cerr << "error: cannot change n_edc_feature because of const" << endl;
                    abort();
                }
            } else {
                cerr << "LRB unrecognized parameter: " << it.first << endl;
            }
        }

        currentPars.resize(kl_sample_num);

        min_sample_rate=sample_rate;
        n_retrain = 0;
        n_inference = 0;
        have_trained = false;
        start_collect_train_data = false;
        max_n_past_distances = max_n_past_timestamps - 1; 
        edc_windows = vector<uint32_t>(n_edc_feature);
        for (uint8_t i = 0; i < n_edc_feature; ++i) {
            edc_windows[i] = pow(2, base_edc_window + i);
        }
        set_hash_edc();

        //interval, distances, size, extra_features, n_past_intervals, edwt
        if(add_new_features){
            n_feature = max_n_past_timestamps + n_extra_fields + 2 + 2 + n_edc_feature;
        }else{
            n_feature = max_n_past_timestamps + n_extra_fields + 2 + n_edc_feature;
        }
        
        cout<<"n_feature:"<<n_feature<<endl;
        

        if (n_extra_fields) {
            if (n_extra_fields > max_n_extra_feature) {
                cerr << "error: only support <= " + to_string(max_n_extra_feature)
                        + " extra fields because of static allocation" << endl;
                abort();
            }
            string categorical_feature = to_string(max_n_past_timestamps + 1);
            for (uint i = 0; i < n_extra_fields - 1; ++i) {
                categorical_feature += "," + to_string(max_n_past_timestamps + 2 + i);
            }
            training_params["categorical_feature"] = categorical_feature;
        }
        inference_params = training_params;
        training_data = new TrainingData();
    
    }


    static void set_hash_edc() {
        max_hash_edc_idx = (uint64_t) (memory_window / pow(2, base_edc_window)) - 1; 
        hash_edc = vector<double>(max_hash_edc_idx + 1);
        for (int i = 0; i < hash_edc.size(); ++i)
            hash_edc[i] = pow(0.5, i); 
    }

    void update_stat(bsoncxx::v_noabi::builder::basic::document &doc) override {
        total_training_time /= 1000;
        total_inference_add_feature_time /= 1000000;
        total_inference_time /= 1000000;
        total_inference_sort_time /= 1000000;
        doc.append(kvp("total_training_time", to_string(total_training_time)));
        doc.append(kvp("total_inference_time", to_string(total_inference_time)));
        doc.append(kvp("total_add_feature_inference_time", to_string(total_inference_add_feature_time)));
        doc.append(kvp("total_inference_sort_time", to_string(total_inference_sort_time)));
        doc.append(kvp("n_force_eviction", n_force_eviction));
        doc.append(kvp("min_js", to_string(min_js)));
        doc.append(kvp("max_js", to_string(max_js)));

        int res;
        auto importances = vector<double>(n_feature, 0);

        if (booster) {
            res = LGBM_BoosterFeatureImportance(booster,
                                                0,
                                                1,
                                                importances.data());
            if (res == -1) {
                cerr << "error: get model importance fail" << endl;
                abort();
            }
        }

        doc.append(kvp("segment_n_in", [this](sub_array child) {
            for (const auto &element : segment_n_in)
                child.append(element);
        }));

        doc.append(kvp("segment_n_out", [this](sub_array child) {
            for (const auto &element : segment_n_out)
                child.append(element);
        }));

        doc.append(kvp("segment_n_retrain", [this](sub_array child) {
            for (const auto &element : segment_n_retrain)
                child.append(element);
        }));

        doc.append(kvp("model_importance", [importances](sub_array child) {
            for (const auto &element : importances) 
                child.append(element);
        }));
    }

};
static Factory<LBSCCache> factoryLBSC("LBSC");
}
#endif //WEBCACHESIM_LBSC_H