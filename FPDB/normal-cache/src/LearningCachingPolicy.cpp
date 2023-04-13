//
//  on 9/22/20.
//

#include <sstream>
#include <fmt/format.h>
#include <cstdlib>
#include <utility>
#include <chrono>
#include "normal/cache/LearningCachingPolicy.h"
#include "normal/connector/MiniCatalogue.h"
#include <iostream>

std::shared_ptr<normal::connector::MiniCatalogue> normal::cache::beladyLearningMiniCatalogue;
std::unordered_map<std::shared_ptr<normal::cache::SegmentKey>, double, normal::cache::SegmentKeyPointerHash, normal::cache::SegmentKeyPointerPredicate> normal::cache::keyProb_;
uint8_t normal::cache::max_n_past_timestamps;
uint8_t normal::cache::max_n_past_distances;
uint32_t normal::cache::memory_window;
uint32_t normal::cache::n_extra_fields;
uint32_t normal::cache::batch_size; 
uint32_t normal::cache::n_feature;

using namespace normal::cache;

bool lessKeyValueLearningBLD (const std::shared_ptr<SegmentKey> &key1, const std::shared_ptr<SegmentKey> &key2) {
  int currentQuery = beladyLearningMiniCatalogue->getCurrentQueryNum();
  int key1NextUse = beladyLearningMiniCatalogue->querySegmentNextUsedIn(key1, currentQuery);
  int key2NextUse = beladyLearningMiniCatalogue->querySegmentNextUsedIn(key2, currentQuery);

  if (key1NextUse == key2NextUse) {

    size_t key1Size = beladyLearningMiniCatalogue->getSegmentSize(key1);
    size_t key2Size = beladyLearningMiniCatalogue->getSegmentSize(key2);
    return key1Size > key2Size;
  } else if (key1NextUse == -1 || (key2NextUse != -1 && key2NextUse < key1NextUse)) {
    return true;
  } else if(key2NextUse == -1 || (key1NextUse != -1 && key1NextUse < key2NextUse)) {
    return false;
  }
  throw std::runtime_error("Error, lessKeyValue reached code that should not be reachable");
}

bool lessKeyValueLearningBLDSC (const std::shared_ptr<SegmentKey> &key1, const std::shared_ptr<SegmentKey> &key2) {
  int currentQuery = beladyLearningMiniCatalogue->getCurrentQueryNum();
  int key1NextUse = beladyLearningMiniCatalogue->querySegmentNextUsedIn(key1, currentQuery);
  int key2NextUse = beladyLearningMiniCatalogue->querySegmentNextUsedIn(key2, currentQuery);

  auto key1Meta = key1->getMetadata();
  auto key2Meta = key2->getMetadata();
  size_t key1Size = beladyLearningMiniCatalogue->getSegmentSize(key1);
  size_t key2Size = beladyLearningMiniCatalogue->getSegmentSize(key2);
  if(key1Meta!=NULL &&  key2Meta!=NULL){
    if(key1NextUse==-1 && key2NextUse==-1){
      return key1Size > key2Size;
    }else if(key1NextUse==-1 && key2NextUse != -1){
      return true;
    }else if(key1NextUse != -1 && key2NextUse == -1){
      return false;
    }else{
      if(key1Meta->valueValid() && key2Meta->valueValid()){
        double rate1 = key1Meta->value() / (double)(key1NextUse+1-currentQuery);
        double rate2 = key2Meta->value() / (double)(key2NextUse+1-currentQuery);
        return rate1<rate2;
      }else{
        if (key1NextUse == key2NextUse) {
          return key1Size > key2Size;
        } else if (key1NextUse == -1 || (key2NextUse != -1 && key2NextUse < key1NextUse)) {
          return true;
        } else if(key2NextUse == -1 || (key1NextUse != -1 && key1NextUse < key2NextUse)) {
          return false;
        }
      }
    }
  } else  {
    if (key1NextUse == key2NextUse) {
      return key1Size > key2Size;
    } else if (key1NextUse == -1 || (key2NextUse != -1 && key2NextUse < key1NextUse)) {
      return true;
    } else if(key2NextUse == -1 || (key1NextUse != -1 && key1NextUse < key2NextUse)) {
      return false;
    }
  } 
  throw std::runtime_error("Error, lessKeyValue reached code that should not be reachable");
}


bool lessKeyValueLearning (const std::shared_ptr<SegmentKey> &key1, const std::shared_ptr<SegmentKey> &key2) {
  auto key1Iter = keyProb_.find(key1);
  auto key2Iter = keyProb_.find(key2);
  if(key1Iter == keyProb_.end()){
    std::cout<<"key1 error"<<std::endl;
  }
  if(key2Iter == keyProb_.end()){
    std::cout<<"key2 error"<<std::endl;
  }
  double key1Dist = key1Iter->second;
  double key2Dist = key2Iter->second;

  auto key1Meta = key1->getMetadata();
  auto key2Meta = key2->getMetadata();
  if(key1Meta!=NULL &&  key2Meta!=NULL){
    if(key1Meta->valueValid() && key2Meta->valueValid()){
      double rate1 = key1Meta->value() / key1Dist;
      double rate2 = key2Meta->value() / key2Dist;
      return rate1<rate2;
    }else{
      return key1Dist > key2Dist;
    }
  } else  {
    return key1Dist > key2Dist;
  } 
  throw std::runtime_error("Error, lessKeyValue reached code that should not be reachable");
}


bool lessKeyValueLearningLog1P (const std::shared_ptr<SegmentKey> &key1, const std::shared_ptr<SegmentKey> &key2) {
  auto key1Iter = keyProb_.find(key1);
  auto key2Iter = keyProb_.find(key2);
  if(key1Iter == keyProb_.end()){
    std::cout<<"key1 error"<<std::endl;
  }
  if(key2Iter == keyProb_.end()){
    std::cout<<"key2 error"<<std::endl;
  }
  double key1Dist = key1Iter->second;
  double key2Dist = key2Iter->second;

  auto key1Meta = key1->getMetadata();
  auto key2Meta = key2->getMetadata();
  if(key1Meta!=NULL &&  key2Meta!=NULL){
    if(key1Meta->valueValid() && key2Meta->valueValid()){
      double rate1 = log1p(key1Meta->value()) - key1Dist;
      double rate2 = log1p(key2Meta->value()) - key2Dist;
      return rate1<rate2;
    }else{
      return key1Dist > key2Dist;
    }
  } else  {
    return key1Dist > key2Dist;
  } 
  throw std::runtime_error("Error, lessKeyValue reached code that should not be reachable");
}
 
LearningCachingPolicy::LearningCachingPolicy(size_t maxSize, std::shared_ptr<normal::plan::operator_::mode::Mode> mode) :
        CachingPolicy(maxSize, std::move(mode)), currentQueryId_(0),weightMethod_(1), delta_(0.5){
          startInference_ =false;
          start_collect_train_data=false;
        n_feature = max_n_past_timestamps + n_extra_fields + 2 + N_EDC_FEATURE;
        if (n_extra_fields) {
            if (n_extra_fields > MAX_N_EXTRA_FEATURE) {
                std::cout << "error: only support <= " + std::to_string(MAX_N_EXTRA_FEATURE)
                        + " extra fields because of static allocation" << std::endl;
                std::exit(-1);
            }
            std::string categorical_feature = std::to_string(max_n_past_timestamps + 1);
            for (uint i = 0; i < n_extra_fields - 1; ++i) {
                categorical_feature += "," + std::to_string(max_n_past_timestamps + 2 + i);
            }
            training_params["categorical_feature"] = categorical_feature;
        }
        inference_params = training_params; 
        training_data = new TrainingData();
} 

void LearningCachingPolicy::setWeightMethod(int wMethod){
  weightMethod_ = wMethod;
}

std::shared_ptr<LearningCachingPolicy> LearningCachingPolicy::make(size_t maxSize, std::shared_ptr<normal::plan::operator_::mode::Mode> mode) {
  return std::make_shared<LearningCachingPolicy>(maxSize, mode);
}

void LearningCachingPolicy::train(){
    if (booster) LGBM_BoosterFree(booster);
    // create training dataset
    DatasetHandle trainData;
    LGBM_DatasetCreateFromCSR(
            static_cast<void *>(training_data->indptr.data()),
            C_API_DTYPE_INT32,
            training_data->indices.data(),
            static_cast<void *>(training_data->data.data()),
            C_API_DTYPE_FLOAT64,
            training_data->indptr.size(),
            training_data->data.size(),
            n_feature,  //remove future t
            training_params,
            nullptr,
            &trainData);

    LGBM_DatasetSetField(trainData,
                         "label",
                         static_cast<void *>(training_data->labels.data()),
                         training_data->labels.size(),
                         C_API_DTYPE_FLOAT32);

    // init booster
    LGBM_BoosterCreate(trainData, training_params, &booster);
    // train
    for (int i = 0; i < stoi(training_params["num_iterations"]); i++) {
        int isFinished;
        LGBM_BoosterUpdateOneIter(booster, &isFinished);
        if (isFinished) {
            break;
        }
    }

    int64_t len;
    std::vector<double> result(training_data->indptr.size() - 1);
    LGBM_BoosterPredictForCSR(booster,
                              static_cast<void *>(training_data->indptr.data()),
                              C_API_DTYPE_INT32,
                              training_data->indices.data(),
                              static_cast<void *>(training_data->data.data()),
                              C_API_DTYPE_FLOAT64,
                              training_data->indptr.size(),
                              training_data->data.size(),
                              n_feature,  //remove future t
                              C_API_PREDICT_NORMAL,
                              0,
                              training_params,
                              &len,
                              result.data());


    double se = 0;
    for (int i = 0; i < result.size(); ++i) {
        auto diff = result[i] - training_data->labels[i];
        se += diff * diff;
    }
    //training_loss = training_loss * 0.99 + se / batch_size * 0.01;
    //training_losses.push_back(se/batch_size);
    std::cout<<"model training done.    training loss:"<<se/batch_size<<std::endl;

    LGBM_DatasetFree(trainData);
    startInference_=true;
}

void LearningCachingPolicy::onLoad(const std::shared_ptr<SegmentKey> &key) {
  auto keyEntry = keySet_.find(key);
  if (keyEntry == keySet_.end()) {
    keySet_.emplace(key);
  }

  int currentQueryNum = beladyLearningMiniCatalogue->getCurrentQueryNum();
  if(!startInference_){
    startInferenceQueryNum = currentQueryNum;
  }

  if(currentQueryNum > memory_window){
    if(!start_collect_train_data){
      std::cout<<"start to collect train data....."<<std::endl;
    }
    start_collect_train_data = true;
  }

  auto itkey = in_cache.find(key);
  if(itkey != in_cache.end()){
    //cahce hit
    auto meta = itkey->second;
    if(start_collect_train_data && !startInference_){
      uint32_t _distance = currentQueryNum + 1 - meta->_past_timestamp;
      if(multiModel_){ 
        auto ColumnName = meta->_key->getColumnName();
        if(beladyLearningMiniCatalogue->findTableOfColumn(ColumnName) == "lineorder"){
          training_data->emplace_back(meta, _distance, meta->_key, labelLog_);
          if (training_data->labels.size() >= batch_size ) { 
            train();
            training_data->clear();
          }
        }
      }else{
        training_data->emplace_back(meta, _distance, meta->_key, labelLog_);
        if (training_data->labels.size() >= batch_size ) { 
            train();
            training_data->clear();
        }
      }
    }
    meta->update(currentQueryNum);
    in_cache.erase(itkey);

    in_cache.insert({key, meta});  
  }
  
}

void LearningCachingPolicy::onRemove(const std::shared_ptr<SegmentKey> &key) {
  erase(key);
}

std::optional<std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>>>
LearningCachingPolicy::onStore(const std::shared_ptr<SegmentKey> &key) {
  auto startTime = std::chrono::steady_clock::now();

  int currentQueryNum = beladyLearningMiniCatalogue->getCurrentQueryNum();
  auto segmentSize = key->getMetadata()->size();
  auto preComputedSize = beladyLearningMiniCatalogue->getSegmentSize(key);
  // make sure segmentSize within 1% of our expected size, if not then something went wrong
  int absSizeDiff = abs((int) (segmentSize - preComputedSize));
  int onePercentSizeDiff = (int) ((float)segmentSize * 0.001);
  if (absSizeDiff > onePercentSizeDiff) {
    throw std::runtime_error("Error, segment has wrong size when compared to expected size");
  }
  if (maxSize_ < segmentSize) {
    return std::nullopt;
  }

  auto it = out_cache.find(key);
  if(it != out_cache.end()){
    auto _meta = it->second;
    if(start_collect_train_data && !startInference_){
      uint32_t _distance = currentQueryNum + 1 - _meta->_past_timestamp;
      if(multiModel_){ 
        auto ColumnName = _meta->_key->getColumnName();
        if(beladyLearningMiniCatalogue->findTableOfColumn(ColumnName) == "lineorder"){
          training_data->emplace_back(_meta, _distance, _meta->_key, labelLog_);
          if (training_data->labels.size() >= batch_size ) { 
            train();
            training_data->clear();
          }
        }
      }else{
        training_data->emplace_back(_meta, _distance, _meta->_key, labelLog_);
        if (training_data->labels.size() >= batch_size ) { 
            train();
            training_data->clear();
        }
      }
     }
    _meta->update(currentQueryNum);
    out_cache.erase(it);
    in_cache.insert({key, _meta});
  }else{
    std::vector<uint16_t> extra_features;
    if(extraFeature_){
      auto tmp_extra_features = beladyLearningMiniCatalogue->getSegmentSizegetSegmentTablePartitionColumnIndex(key);
      if(tmp_extra_features.size() != n_extra_fields){
        std::cout<<"error:  extra_features.size()="<<tmp_extra_features.size()<<std::endl;
        std::exit(-1);
      }
      extra_features.resize(n_extra_fields);
      for(int jj=0;jj<extra_features.size();jj++){
        extra_features[jj] = tmp_extra_features[jj];
      }

    }
    auto _meta = std::make_shared<Meta>(key, currentQueryNum, extra_features);
    in_cache.insert({key, _meta});
  }
  auto removableKeys = std::make_shared<std::vector<std::shared_ptr<SegmentKey>>>();

  auto queryKey = queryNumToKeysInCache_.find(currentQueryNum);
  if (queryKey == queryNumToKeysInCache_.end()) {
    throw std::runtime_error("Error, query " + std::to_string(currentQueryNum) + " not populated in queryNumToKeysInCache_ in LearningCachingPolicy.cpp");
  }
  auto keysThatShouldBeCached = queryKey->second;

  if (keysThatShouldBeCached->find(key) == keysThatShouldBeCached->end()) {
    in_cache.erase(key);
    return std::nullopt;
  }

  if (segmentSize < freeSize_) {
    //in_cache.insert(key);
    auto stopTime = std::chrono::steady_clock::now();
    onStoreTime += std::chrono::duration_cast<std::chrono::nanoseconds>(stopTime - startTime).count();
    freeSize_ -= segmentSize;
    return std::optional(removableKeys);
  }

  for (auto iterIncache = in_cache.begin(); iterIncache!=in_cache.end(); iterIncache++) {
    if (keysThatShouldBeCached->find(iterIncache->first) == keysThatShouldBeCached->end()) {
      removableKeys->emplace_back(iterIncache->first);
      //for test
      auto segmentSize_ = (iterIncache->first)->getMetadata()->size();
      if(segmentSize_==0){
        segmentSize_ = beladyLearningMiniCatalogue->getSegmentSize(iterIncache->first);
        (iterIncache->first)->getMetadata()->setSize(segmentSize_);
      }
      freeSize_ += segmentSize_;
      out_cache.insert({iterIncache->first, iterIncache->second});
    }
  }
  // remove these keys from the cache
  for (const auto& removableKey: *removableKeys) {
    in_cache.erase(removableKey);
  }

  //in_cache.insert(key);
  freeSize_ -= segmentSize;
  if (freeSize_ < 0) {
    throw std::runtime_error("Error, freeSize_ < 0, is: " + std::to_string(freeSize_)+"  segmentSize:"+std::to_string(segmentSize));
  }

  auto stopTime = std::chrono::steady_clock::now();
  onStoreTime += std::chrono::duration_cast<std::chrono::nanoseconds>(stopTime - startTime).count();

  return std::optional(removableKeys);
}


std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>>
LearningCachingPolicy::onToCache(std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> segmentKeys) {
  auto startTime = std::chrono::steady_clock::now();
  if (mode_->id() == normal::plan::operator_::mode::ModeId::PullupCaching) {
    return segmentKeys;
  }

  auto keysToCache = std::make_shared<std::vector<std::shared_ptr<SegmentKey>>>();
  int currentQueryNum = beladyLearningMiniCatalogue->getCurrentQueryNum();
  auto queryKey = queryNumToKeysInCache_.find(currentQueryNum);
  if (queryKey == queryNumToKeysInCache_.end()) {
    throw std::runtime_error("Error, " + std::to_string(currentQueryNum) + " not populated in queryNumToKeysInCache_ in LearningCachingPolicy.cpp");
  }

  auto keysThatShouldBeCached = queryKey->second;
  // decide whether to cache
  for (const auto& key: *segmentKeys) {
    // see if we decided to cache this segment for this query number
    if (keysThatShouldBeCached->find(key) != keysThatShouldBeCached->end()) {
      keysToCache->emplace_back(key);
    }
  }

  auto stopTime = std::chrono::steady_clock::now();
  onToCacheTime += std::chrono::duration_cast<std::chrono::nanoseconds>(stopTime - startTime).count();

  return keysToCache;
}

void LearningCachingPolicy::erase(const std::shared_ptr<SegmentKey> &key) {
  auto iter = in_cache.find(key);
  if(iter != in_cache.end()){
    in_cache.erase(iter);
  }
}

std::shared_ptr<std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate>>
LearningCachingPolicy::getKeysetInCachePolicy() {
  auto keysetInCachePolicy = std::make_shared<std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate>>();
  //keysetInCachePolicy->insert(in_cache.begin(), in_cache.end());
  for(auto iter=in_cache.begin(); iter!=in_cache.end(); iter++){
    keysetInCachePolicy->insert(iter->first);
  }
  return keysetInCachePolicy;
}

void LearningCachingPolicy::updateWeightAfterInference(long queryNum){
  auto startTime = std::chrono::steady_clock::now();
  std::cout<<"test queryNum:"<<queryNum<<std::endl;
  beladyLearningMiniCatalogue->setCurrentQueryNum(queryNum);

  auto keysInCurrentQuery = beladyLearningMiniCatalogue->getSegmentsInQuery(queryNum);
  auto allPotentialKeysToCache = std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate>();

  allPotentialKeysToCache.insert(keysInCurrentQuery->begin(), keysInCurrentQuery->end());
  for(auto iter = in_cache.begin(); iter!=in_cache.end(); iter++){
    allPotentialKeysToCache.insert(iter->first);
  }

  auto queryIter = queryNumToKeysInCache_.find(queryNum);
  assert(queryIter != queryNumToKeysInCache_.end());
  auto queryNumSeq = queryIter->second;
  allPotentialKeysToCache.insert(queryNumSeq->begin(), queryNumSeq->end());

  auto potentialKeysToCache = std::make_shared<std::vector<std::shared_ptr<SegmentKey>>>();
  potentialKeysToCache->insert(potentialKeysToCache->end(), allPotentialKeysToCache.begin(), allPotentialKeysToCache.end());  


  int potentialKeysToCacheIndex=0;
  const int sample_rate = potentialKeysToCache->size();
  int32_t indptr[sample_rate + 1];
  indptr[0] = 0;
  int32_t indices[sample_rate * n_feature];
  double data[sample_rate * n_feature];

  unsigned int idx_feature = 0;
  unsigned int idx_row = 0;

  for(auto iter = potentialKeysToCache->begin(); iter!=potentialKeysToCache->end(); iter++){
    potentialKeysToCacheIndex++;
    std::shared_ptr<SegmentKey> realKey;
    auto keyEntry = keySet_.find(*iter);
    if (keyEntry != keySet_.end()) {
      realKey = *keyEntry;
      
      if(realKey->getMetadata()!=NULL){
        if((*iter)->getMetadata() == NULL){
          (*iter)->setMetadata(std::make_shared<SegmentMetadata>(*realKey->getMetadata()));
        }
        (*iter)->getMetadata()->setValue(realKey->getMetadata()->value());
      }
    }
      std::vector<uint16_t> extra_features;
      if(extraFeature_){
        auto tmp_extra_features = beladyLearningMiniCatalogue->getSegmentSizegetSegmentTablePartitionColumnIndex(*iter);
        if(tmp_extra_features.size() != n_extra_fields){
          std::cout<<"error:  extra_features.size()="<<tmp_extra_features.size()<<std::endl;
          std::exit(-1);
        }
        extra_features.resize(n_extra_fields);
        for(int jj=0;jj<extra_features.size();jj++){
          extra_features[jj] = tmp_extra_features[jj];
        }
      }
      auto meta = std::make_shared<Meta>(*iter, queryNum, extra_features);

      auto metaIterInCache = in_cache.find(*iter);
      auto metaIterOutCache = out_cache.find(*iter);
      if(metaIterInCache != in_cache.end()){
        meta = metaIterInCache->second;
      }else if(metaIterOutCache != out_cache.end()){
        meta = metaIterOutCache->second;
      }

      indices[idx_feature] = 0;
      data[idx_feature++] = queryNum - meta->_past_timestamp;
      uint8_t j = 0;
      uint32_t this_past_distance = 0;
      uint8_t n_within = 0;
      if (meta->_extra) {
        for (j = 0; j < meta->_extra->_past_distance_idx && j < max_n_past_distances; ++j) {
          uint8_t past_distance_idx = (meta->_extra->_past_distance_idx - 1 - j) % max_n_past_distances;
          uint32_t &past_distance = meta->_extra->_past_distances[past_distance_idx];
          this_past_distance += past_distance;
          indices[idx_feature] = j + 1;
          data[idx_feature++] = past_distance;
          if (this_past_distance < memory_window) {
            ++n_within;
          }
        }
      }
      indices[idx_feature] = max_n_past_timestamps;
      //data[idx_feature++] = meta._size; 
      auto preComputedSize = beladyLearningMiniCatalogue->getSegmentSize(*iter);
      data[idx_feature++] = preComputedSize;

      for (uint k = 0; k < n_extra_fields; ++k) {
        indices[idx_feature] = max_n_past_timestamps + k + 1;
        data[idx_feature++] = meta->_extra_features[k];
      }

      indices[idx_feature] = max_n_past_timestamps + n_extra_fields + 1;
      data[idx_feature++] = n_within;

      indptr[++idx_row] = idx_feature;
  }

  double scores[sample_rate];
  int64_t len;

  LGBM_BoosterPredictForCSR(booster,
                              static_cast<void *>(indptr),
                              C_API_DTYPE_INT32,
                              indices,
                              static_cast<void *>(data),
                              C_API_DTYPE_FLOAT64,
                              idx_row + 1,
                              idx_feature,
                              n_feature,  //remove future t
                              C_API_PREDICT_NORMAL,
                              0,
                              inference_params,
                              &len,
                              scores);
  int scoreIndex = 0;
  for (auto iter = potentialKeysToCache->begin(); iter!=potentialKeysToCache->end(); iter++){
    //scores[i] = scores[i] + log1p(sizes[i]) - log1p(costs[i]);
    if(multiModel_){
      auto ColumnName = (*iter)->getColumnName();
      if(beladyLearningMiniCatalogue->findTableOfColumn(ColumnName) != "lineorder"){
         //int currentQuery = beladyLearningMiniCatalogue->getCurrentQueryNum();
        int keyNextUse = beladyLearningMiniCatalogue->querySegmentNextUsedIn((*iter), queryNum);
        if(keyNextUse == -1){
          keyNextUse = 1000000000000000;
        }
        if(labelLog_){
          scores[scoreIndex] = log1p(keyNextUse +1 -queryNum);
        }else{
          scores[scoreIndex] = keyNextUse +1 -queryNum;
        }
      }
    }

    auto keyProbIter = keyProb_.find(*iter);
    if(keyProbIter != keyProb_.end()){
      keyProbIter->second = scores[scoreIndex];
    }else{
      keyProb_.emplace(*iter,scores[scoreIndex]);
    }
    scoreIndex++;
  }

  // reset our keysInCache to be empty for this query and populate it
  auto keysInCache = std::make_shared<std::vector<std::shared_ptr<SegmentKey>>>();

  if(labelLog_){
    std::sort(potentialKeysToCache->begin(), potentialKeysToCache->end(), lessKeyValueLearningLog1P);
  }else{
    std::sort(potentialKeysToCache->begin(), potentialKeysToCache->end(), lessKeyValueLearning);
  }
  // Reverse this ordering as we want keys with the greatest value first
  std::reverse(potentialKeysToCache->begin(), potentialKeysToCache->end());

  size_t remainingCacheSize = maxSize_ * 0.99;
  for (const auto& segmentKey : *potentialKeysToCache) {
      size_t segmentSize = beladyLearningMiniCatalogue->getSegmentSize(segmentKey);
      //int keyNextUse = beladyLearningMiniCatalogue->querySegmentNextUsedIn(segmentKey, queryNum);
      if (segmentSize < remainingCacheSize) {
        keysInCache->emplace_back(segmentKey);
        remainingCacheSize -= segmentSize;
      }
  }
  auto keysToCacheSet = std::make_shared<std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate>>();
  keysToCacheSet->insert(keysInCache->begin(), keysInCache->end());
  queryIter->second=keysToCacheSet;

  auto stopTime = std::chrono::steady_clock::now();
  updateWeightTime += std::chrono::duration_cast<std::chrono::nanoseconds>(stopTime - startTime).count();

}

void LearningCachingPolicy::updateWeight(long queryNum){
  auto startTime = std::chrono::steady_clock::now();
  std::cout<<"test queryNum:"<<queryNum<<std::endl;
  beladyLearningMiniCatalogue->setCurrentQueryNum(queryNum);
  auto keysInCurrentQuery = beladyLearningMiniCatalogue->getSegmentsInQuery(queryNum);
  auto allPotentialKeysToCache = std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate>();
  for(auto iter = in_cache.begin(); iter!=in_cache.end(); iter++){ 
    allPotentialKeysToCache.insert(iter->first);
  }
  allPotentialKeysToCache.insert(keysInCurrentQuery->begin(), keysInCurrentQuery->end());
  //new add
  auto queryIter = queryNumToKeysInCache_.find(queryNum);
  assert(queryIter != queryNumToKeysInCache_.end());
  auto queryNumSeq = queryIter->second;
  allPotentialKeysToCache.insert(queryNumSeq->begin(), queryNumSeq->end());

  auto potentialKeysToCache = std::make_shared<std::vector<std::shared_ptr<SegmentKey>>>();
  potentialKeysToCache->insert(potentialKeysToCache->end(), allPotentialKeysToCache.begin(), allPotentialKeysToCache.end());

  for(auto iter = potentialKeysToCache->begin(); iter!=potentialKeysToCache->end(); iter++){
    std::shared_ptr<SegmentKey> realKey;
    auto keyEntry = keySet_.find(*iter);
    if (keyEntry != keySet_.end()) {
      realKey = *keyEntry;
      
      if(realKey->getMetadata()!=NULL){
        if((*iter)->getMetadata() == NULL){
          (*iter)->setMetadata(std::make_shared<SegmentMetadata>(*realKey->getMetadata()));
        }
        (*iter)->getMetadata()->setValue(realKey->getMetadata()->value());
      }
    }
  }

  auto keysInCache = std::make_shared<std::vector<std::shared_ptr<SegmentKey>>>();
  std::sort(potentialKeysToCache->begin(), potentialKeysToCache->end(), lessKeyValueLearningBLDSC);
  std::reverse(potentialKeysToCache->begin(), potentialKeysToCache->end());

  size_t remainingCacheSize = maxSize_ * 0.99;
  for (const auto& segmentKey : *potentialKeysToCache) {
      size_t segmentSize = beladyLearningMiniCatalogue->getSegmentSize(segmentKey);
      int keyNextUse = beladyLearningMiniCatalogue->querySegmentNextUsedIn(segmentKey, queryNum);
      if (segmentSize < remainingCacheSize && keyNextUse != -1) {
        keysInCache->emplace_back(segmentKey);
        remainingCacheSize -= segmentSize;
      }
  }

  auto keysToCacheSet = std::make_shared<std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate>>();
  keysToCacheSet->insert(keysInCache->begin(), keysInCache->end());
  queryIter->second=keysToCacheSet;
  
  auto stopTime = std::chrono::steady_clock::now();
  updateWeightTime += std::chrono::duration_cast<std::chrono::nanoseconds>(stopTime - startTime).count();
}

void LearningCachingPolicy::onWeight(const std::shared_ptr<std::unordered_map<std::shared_ptr<SegmentKey>, double>> &weightMap, long queryId) {
  auto startTime = std::chrono::steady_clock::now();
  if (queryId > currentQueryId_) {
    weightUpdatedKeys_.clear();
    currentQueryId_ = queryId;
  }

  for (auto const &weightEntry: *weightMap) {
    auto segmentKey = weightEntry.first;
    auto weight = weightEntry.second;
    if (weightUpdatedKeys_.find(segmentKey) == weightUpdatedKeys_.end()) {
      std::shared_ptr<SegmentKey> realKey;
      auto keyEntry = keySet_.find(segmentKey);
      if (keyEntry != keySet_.end()) {
        realKey = *keyEntry;
        if(weightMethod_==1){
          realKey->getMetadata()->addValue(weight);
        }else if(weightMethod_==2){
          realKey->getMetadata()->addValueDelta(weight, delta_);
        }else if(weightMethod_==3){
          realKey->getMetadata()->addAvgValue(weight);
        }else{
          throw std::runtime_error("weightMethod error");
        }
      } else {
        throw std::runtime_error("onWeight: Key should exist in keySet_");
      }

      weightUpdatedKeys_.emplace(realKey);
    }
  }

  auto stopTime = std::chrono::steady_clock::now();
  onWeightTime += std::chrono::duration_cast<std::chrono::nanoseconds>(stopTime - startTime).count();
}

void LearningCachingPolicy::generateCacheDecisions(int numQueries) {
  auto keysInCache = std::make_shared<std::vector<std::shared_ptr<SegmentKey>>>();
  for (int queryNum = 1; queryNum <= numQueries; ++queryNum) {
    beladyLearningMiniCatalogue->setCurrentQueryNum(queryNum);
    auto keysInCurrentQuery = beladyLearningMiniCatalogue->getSegmentsInQuery(queryNum);
    auto allPotentialKeysToCache = std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate>();
    allPotentialKeysToCache.insert(keysInCache->begin(), keysInCache->end());
    allPotentialKeysToCache.insert(keysInCurrentQuery->begin(), keysInCurrentQuery->end());
    auto potentialKeysToCache = std::make_shared<std::vector<std::shared_ptr<SegmentKey>>>();
    potentialKeysToCache->insert(potentialKeysToCache->end(), allPotentialKeysToCache.begin(), allPotentialKeysToCache.end());

    keysInCache = std::make_shared<std::vector<std::shared_ptr<SegmentKey>>>(); 
    std::sort(potentialKeysToCache->begin(), potentialKeysToCache->end(), lessKeyValueLearningBLD);
    std::reverse(potentialKeysToCache->begin(), potentialKeysToCache->end());

    size_t remainingCacheSize = maxSize_ * 0.99;
    for (const auto& segmentKey : *potentialKeysToCache) {
      size_t segmentSize = beladyLearningMiniCatalogue->getSegmentSize(segmentKey);
      int keyNextUse = beladyLearningMiniCatalogue->querySegmentNextUsedIn(segmentKey, queryNum);
      if (segmentSize < remainingCacheSize && keyNextUse != -1) {
        keysInCache->emplace_back(segmentKey);
        remainingCacheSize -= segmentSize;
      }
    }
    auto keysToCacheSet = std::make_shared<std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate>>();
    keysToCacheSet->insert(keysInCache->begin(), keysInCache->end());
    queryNumToKeysInCache_.emplace(queryNum, keysToCacheSet);
  }
}


std::string LearningCachingPolicy::showCurrentLayout() {
  std::stringstream ss;
  ss << "Total numbers: " << in_cache.size() << std::endl;
  for (auto const &segmentKey: in_cache) {
    int queryNum = beladyLearningMiniCatalogue->getCurrentQueryNum();
    int keyNextUse = beladyLearningMiniCatalogue->querySegmentNextUsedIn(segmentKey.first, queryNum);
    size_t keySize = beladyLearningMiniCatalogue->getSegmentSize(segmentKey.first);
    ss << fmt::format("Key: {};\tSize: {}\n Next Use: {}", (segmentKey.first)->toString(), keySize, keyNextUse) << std::endl;
  }
  ss << "Max size: " << maxSize_ << std::endl;
  ss << "Free size: " << freeSize_ << std::endl;
  return ss.str();
}

CachingPolicyId LearningCachingPolicy::id() {
  return LEARN;
}

void LearningCachingPolicy::onNewQuery() {

}


bool LearningCachingPolicy::getStartInference(){
  return startInference_;
}