//
//  on 9/22/20.  
//

#ifndef NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_LEARNINGCACHINGPOLICY_H
#define NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_LEARNINGCACHINGPOLICY_H

#include <memory>
#include <cmath>
#include <list>
#include <forward_list>
#include <unordered_set>
#include <unordered_map>
#include <vector>

#include "SegmentKey.h"
#include "CachingPolicy.h"
#include "normal/connector/MiniCatalogue.h"
#include <LightGBM/c_api.h>
#include <chrono>
#include <atomic>

#define MAX_N_EXTRA_FEATURE 4
#define N_EDC_FEATURE 0

namespace normal::cache {

// This must be populated with SegmentKey->[Query #s Segment is used in] and
// QueryNumber->[Involved Segment Keys] prior to executing any queries
extern std::shared_ptr<connector::MiniCatalogue> beladyLearningMiniCatalogue;
extern std::unordered_map<std::shared_ptr<SegmentKey>, double, SegmentKeyPointerHash, SegmentKeyPointerPredicate>  keyProb_;
extern uint8_t max_n_past_timestamps;
extern uint8_t max_n_past_distances;
extern uint32_t memory_window; 
extern uint32_t n_extra_fields;
extern uint32_t batch_size; 
extern uint32_t n_feature;

struct MetaExtra {
    std::vector<uint32_t> _past_distances; 
    uint8_t _past_distance_idx = 1; 
    MetaExtra(const uint32_t &distance) { 
        _past_distances = std::vector<uint32_t>(1, distance);
    }
    void update(const uint32_t &distance) {
        uint8_t distance_idx = _past_distance_idx % max_n_past_distances; 
        if (_past_distances.size() < max_n_past_distances)
            _past_distances.emplace_back(distance);
        else
            _past_distances[distance_idx] = distance;
        assert(_past_distances.size() <= max_n_past_distances);
        _past_distance_idx = _past_distance_idx + (uint8_t) 1;
        if (_past_distance_idx >= max_n_past_distances * 2)
            _past_distance_idx -= max_n_past_distances;
    }
};  

class Meta {
public:
    std::shared_ptr<SegmentKey> _key;
    uint32_t _past_timestamp; 
    uint16_t _extra_features[MAX_N_EXTRA_FEATURE];
    MetaExtra *_extra = nullptr;

    Meta(const std::shared_ptr<SegmentKey> &key, const uint64_t &past_timestamp,
            const std::vector<uint16_t> &extra_features) {
        _key = key;
        _past_timestamp = past_timestamp;
        for (int i = 0; i < n_extra_fields; ++i) 
            _extra_features[i] = extra_features[i];
    }

    virtual ~Meta() = default;
    void free() {
        delete _extra;
    }
    void update(const uint32_t &past_timestamp) {
        //distance
        uint32_t _distance = past_timestamp - _past_timestamp;
        assert(_distance);
        if (!_extra) {
            _extra = new MetaExtra(_distance);
        } else
            _extra->update(_distance);
        _past_timestamp = past_timestamp;
    }
};

class TrainingData {
public:
    std::vector<float> labels;
    std::vector<int32_t> indptr;
    std::vector<int32_t> indices;
    std::vector<double> data;

    TrainingData() {
        labels.reserve(batch_size);  
        indptr.reserve(batch_size + 1);
        indptr.emplace_back(0);
        indices.reserve(batch_size * n_feature);
        data.reserve(batch_size * n_feature); 
    }
    void emplace_back(std::shared_ptr<Meta> meta, uint32_t &future_distance, const std::shared_ptr<SegmentKey> &key, bool labelLog) {
        int32_t counter = indptr.back();
        uint32_t this_past_distance = 0;
        int j = 0;
        uint8_t n_within = 0;
        if (meta->_extra) {
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
        auto preComputedSize = beladyLearningMiniCatalogue->getSegmentSize(key);
        data.push_back(preComputedSize);
        ++counter;
        for (int k = 0; k < n_extra_fields; ++k) {
            indices.push_back(max_n_past_timestamps + k + 1);
            data.push_back(meta->_extra_features[k]); 
        }
        counter += n_extra_fields;
        indices.push_back(max_n_past_timestamps + n_extra_fields + 1);
        data.push_back(n_within);
        ++counter;
        counter += N_EDC_FEATURE;

        if(labelLog){
            labels.push_back(log1p(future_distance));
        }else{
            labels.push_back(future_distance);
        }
        indptr.push_back(counter); 
    }

    void clear() {
        labels.clear();
        indptr.resize(1);
        indices.clear();
        data.clear();
    }
};


class LearningCachingPolicy: public CachingPolicy {


public:
  ///start  learning new
  TrainingData *training_data;
  BoosterHandle booster = nullptr;

  std::unordered_map<std::string, std::string> training_params = {
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
  std::unordered_map<std::string, std::string> inference_params;
  bool start_collect_train_data;
  bool startInference_;

  std::unordered_map<std::shared_ptr<SegmentKey>, std::shared_ptr<Meta>, SegmentKeyPointerHash, SegmentKeyPointerPredicate> out_cache; 
  std::unordered_map<std::shared_ptr<SegmentKey>, std::shared_ptr<Meta>, SegmentKeyPointerHash, SegmentKeyPointerPredicate> in_cache;

  void train();
  explicit LearningCachingPolicy(size_t maxSize, std::shared_ptr<normal::plan::operator_::mode::Mode> mode);
  static std::shared_ptr<LearningCachingPolicy> make(size_t maxSize, std::shared_ptr<normal::plan::operator_::mode::Mode> mode);

  void onLoad(const std::shared_ptr<SegmentKey> &key) override;
  void onRemove(const std::shared_ptr<SegmentKey> &key) override;
  std::optional<std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>>> onStore(const std::shared_ptr<SegmentKey> &key) override;
  std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> onToCache(std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> segmentKeys) override;
  std::shared_ptr<std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate>> getKeysetInCachePolicy() override;
  std::string showCurrentLayout() override;

  void generateCacheDecisions(int numQueries);

  void onWeight(const std::shared_ptr<std::unordered_map<std::shared_ptr<SegmentKey>, double>> &weightMap, long queryId);
  void updateWeight(long queryNum);
  void updateWeightAfterInference(long queryNum);

  CachingPolicyId id() override;
  void onNewQuery() override;
  void setWeightMethod(int wMethod);
  bool getStartInference();
  long getStartInferenceQueryNum(){
    return startInferenceQueryNum;
  }

  void setLabelLog(bool labelLog){
    labelLog_ = labelLog;
  }

  void setExtraFeature(bool extraFeature){
    extraFeature_ = extraFeature;
  }

  void setMultiModel(bool multiModel){
    multiModel_ = multiModel;
  }

private:

  long currentQueryId_;
  uint sample_rate = 64;
  long startInferenceQueryNum;
  std::unordered_map<int, std::shared_ptr<std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate>>> queryNumToKeysInCache_;
  std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate> keySet_;

  std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate> weightUpdatedKeys_;

  void erase(const std::shared_ptr<SegmentKey> &key);
  int weightMethod_;
  double delta_;
  bool labelLog_;
  bool extraFeature_;
  bool multiModel_;
};

}

#endif //NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_LearningCachingPolicy_H
