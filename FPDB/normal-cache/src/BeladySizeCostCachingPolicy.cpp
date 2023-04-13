#include <sstream>
#include <fmt/format.h>
#include <cstdlib>
#include <utility>
#include <chrono>
#include "normal/cache/BeladySizeCostCachingPolicy.h"
#include "normal/connector/MiniCatalogue.h"
#include <iostream>

std::shared_ptr<normal::connector::MiniCatalogue> normal::cache::beladySCMiniCatalogue;
size_t normal::cache::bldscLessValue1Gloal;
size_t normal::cache::bldscLessValue2Gloal;
size_t normal::cache::bldscLessValue3Gloal;
size_t normal::cache::bldscLessValue4Gloal;

using namespace normal::cache;

bool lessKeyValueBLD (const std::shared_ptr<SegmentKey> &key1, const std::shared_ptr<SegmentKey> &key2) {
  // Used to ensure weak ordering, can't have a key be less than itself
  int currentQuery = beladySCMiniCatalogue->getCurrentQueryNum();
  // note that this is -1 if the key is never used again
  int key1NextUse = beladySCMiniCatalogue->querySegmentNextUsedIn(key1, currentQuery);
  int key2NextUse = beladySCMiniCatalogue->querySegmentNextUsedIn(key2, currentQuery);

  if (key1NextUse == key2NextUse) {
    size_t key1Size = beladySCMiniCatalogue->getSegmentSize(key1);
    size_t key2Size = beladySCMiniCatalogue->getSegmentSize(key2);
    return key1Size > key2Size;
  } else if (key1NextUse == -1 || (key2NextUse != -1 && key2NextUse < key1NextUse)) {
    return true;
  } else if(key2NextUse == -1 || (key1NextUse != -1 && key1NextUse < key2NextUse)) {
    return false;
  }
  throw std::runtime_error("Error, lessKeyValue reached code that should not be reachable");
}


bool lessKeyValueSC (const std::shared_ptr<SegmentKey> &key1, const std::shared_ptr<SegmentKey> &key2) {
  int currentQuery = beladySCMiniCatalogue->getCurrentQueryNum();
  int key1NextUse = beladySCMiniCatalogue->querySegmentNextUsedIn(key1, currentQuery);
  int key2NextUse = beladySCMiniCatalogue->querySegmentNextUsedIn(key2, currentQuery);
 auto key1Meta = key1->getMetadata();
 auto key2Meta = key2->getMetadata();
 size_t key1Size = beladySCMiniCatalogue->getSegmentSize(key1);
 size_t key2Size = beladySCMiniCatalogue->getSegmentSize(key2);
 if(key1Meta!=NULL &&  key2Meta!=NULL){
  if(key1NextUse==-1 && key2NextUse==-1){
    bldscLessValue4Gloal++;
    return key1Size > key2Size;
  }else if(key1NextUse==-1 && key2NextUse != -1){
    return true;
  }else if(key1NextUse != -1 && key2NextUse == -1){
    return false;
  }else{
    if(key1Meta->valueValid() && key2Meta->valueValid()){
      bldscLessValue3Gloal++;
      double rate1 = key1Meta->value() / (double)(key1NextUse+1-currentQuery);
      double rate2 = key2Meta->value() / (double)(key2NextUse+1-currentQuery);
      return rate1<rate2;
    }else{
      bldscLessValue2Gloal++;
      if (key1NextUse == key2NextUse) {
        //return key1Size < key2Size;
        return key1Size > key2Size;
      } else if (key1NextUse == -1 || (key2NextUse != -1 && key2NextUse < key1NextUse)) {
        return true;
      } else if(key2NextUse == -1 || (key1NextUse != -1 && key1NextUse < key2NextUse)) {
        return false;
      }
    }
  }
 }
 
 else{
  bldscLessValue1Gloal++;
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
 
BeladySizeCostCachingPolicy::BeladySizeCostCachingPolicy(size_t maxSize, std::shared_ptr<normal::plan::operator_::mode::Mode> mode) :
        CachingPolicy(maxSize, std::move(mode)), currentQueryId_(0),weightMethod_(1), delta_(0.5){} 

void BeladySizeCostCachingPolicy::setWeightMethod(int wMethod){
  weightMethod_ = wMethod;
}

std::shared_ptr<BeladySizeCostCachingPolicy> BeladySizeCostCachingPolicy::make(size_t maxSize, std::shared_ptr<normal::plan::operator_::mode::Mode> mode) {
  return std::make_shared<BeladySizeCostCachingPolicy>(maxSize, mode);
}

void BeladySizeCostCachingPolicy::onLoad(const std::shared_ptr<SegmentKey> &key) {
  // Nothing to do for Belady caching policy do nothing
  auto keyEntry = keySet_.find(key);
  if (keyEntry == keySet_.end()) {
    keySet_.emplace(key);
  }
}

void BeladySizeCostCachingPolicy::onRemove(const std::shared_ptr<SegmentKey> &key) {
  erase(key);
}

std::optional<std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>>>
BeladySizeCostCachingPolicy::onStore(const std::shared_ptr<SegmentKey> &key) {
  auto startTime = std::chrono::steady_clock::now();
  auto removableKeys = std::make_shared<std::vector<std::shared_ptr<SegmentKey>>>();

  auto segmentSize = key->getMetadata()->size();
  auto preComputedSize = beladySCMiniCatalogue->getSegmentSize(key);
  // make sure segmentSize within 1% of our expected size, if not then something went wrong
  int absSizeDiff = abs((int) (segmentSize - preComputedSize));
  int onePercentSizeDiff = (int) ((float)segmentSize * 0.001);
  if (absSizeDiff > onePercentSizeDiff) {
    throw std::runtime_error("Error, segment has wrong size when compared to expected size");
  }
  if (maxSize_ < segmentSize) {
    return std::nullopt;
  }

  int currentQueryNum = beladySCMiniCatalogue->getCurrentQueryNum();
  auto queryKey = queryNumToKeysInCache_.find(currentQueryNum);
  if (queryKey == queryNumToKeysInCache_.end()) {
    throw std::runtime_error("Error, query " + std::to_string(currentQueryNum) + " not populated in queryNumToKeysInCache_ in BeladySizeCostCachingPolicy.cpp");
  }
  auto keysThatShouldBeCached = queryKey->second;

  // Shouldn't cache segments that aren't supposed to be in the cache at the end of this query
  if (keysThatShouldBeCached->find(key) == keysThatShouldBeCached->end()) {
    return std::nullopt;
  }

  // if room for this key no need to evict anything
  if (segmentSize < freeSize_) {
    keysInCache_.insert(key);
    auto stopTime = std::chrono::steady_clock::now();
    onStoreTime += std::chrono::duration_cast<std::chrono::nanoseconds>(stopTime - startTime).count();
    freeSize_ -= segmentSize;
    return std::optional(removableKeys);
  }

  // remove all keys in the cache that shouldn't be present after this query
  // amortized operation as usually nothing done and we do earlier empty return
  for (const auto& potentialKeyToRemove: keysInCache_) {
    if (keysThatShouldBeCached->find(potentialKeyToRemove) == keysThatShouldBeCached->end()) {
      removableKeys->emplace_back(potentialKeyToRemove);
      freeSize_ += potentialKeyToRemove->getMetadata()->size();
    }
  }
  // remove these keys from the cache
  for (const auto& removableKey: *removableKeys) {
    keysInCache_.erase(removableKey);
  }

  // bring in
  keysInCache_.insert(key);
  freeSize_ -= segmentSize;

  if (freeSize_ < 0) {
    throw std::runtime_error("Error, freeSize_ < 0, is: " + std::to_string(freeSize_));
  }
  auto stopTime = std::chrono::steady_clock::now();
  onStoreTime += std::chrono::duration_cast<std::chrono::nanoseconds>(stopTime - startTime).count();

  return std::optional(removableKeys);
}

std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>>
BeladySizeCostCachingPolicy::onToCache(std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> segmentKeys) {
  auto startTime = std::chrono::steady_clock::now();
  if (mode_->id() == normal::plan::operator_::mode::ModeId::PullupCaching) {
    return segmentKeys;
  }

  auto keysToCache = std::make_shared<std::vector<std::shared_ptr<SegmentKey>>>();
  int currentQueryNum = beladySCMiniCatalogue->getCurrentQueryNum();
  auto queryKey = queryNumToKeysInCache_.find(currentQueryNum);
  if (queryKey == queryNumToKeysInCache_.end()) {
    throw std::runtime_error("Error, " + std::to_string(currentQueryNum) + " not populated in queryNumToKeysInCache_ in BeladySizeCostCachingPolicy.cpp");
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

void BeladySizeCostCachingPolicy::erase(const std::shared_ptr<SegmentKey> &key) { 
  keysInCache_.erase(std::find(keysInCache_.begin(), keysInCache_.end(), key));
}

std::shared_ptr<std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate>>
BeladySizeCostCachingPolicy::getKeysetInCachePolicy() {
  auto keysetInCachePolicy = std::make_shared<std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate>>();
  keysetInCachePolicy->insert(keysInCache_.begin(), keysInCache_.end());
  return keysetInCachePolicy;
}

std::string BeladySizeCostCachingPolicy::showCurrentLayout() {
  std::stringstream ss;
  ss << "Total numbers: " << keysInCache_.size() << std::endl;
  for (auto const &segmentKey: keysInCache_) {
    int queryNum = beladySCMiniCatalogue->getCurrentQueryNum();
    int keyNextUse = beladySCMiniCatalogue->querySegmentNextUsedIn(segmentKey, queryNum);
    size_t keySize = beladySCMiniCatalogue->getSegmentSize(segmentKey);
    ss << fmt::format("Key: {};\tSize: {}\n Next Use: {}", segmentKey->toString(), keySize, keyNextUse) << std::endl;
  }
  ss << "Max size: " << maxSize_ << std::endl;
  ss << "Free size: " << freeSize_ << std::endl;
  return ss.str();
}

void BeladySizeCostCachingPolicy::updateWeight(long queryNum){

  auto startTime = std::chrono::steady_clock::now();
  std::cout<<"test queryNum:"<<queryNum<<std::endl;
  beladySCMiniCatalogue->setCurrentQueryNum(queryNum);
  auto keysInCurrentQuery = beladySCMiniCatalogue->getSegmentsInQuery(queryNum);

  auto allPotentialKeysToCache = std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate>();
  allPotentialKeysToCache.insert(keysInCache_.begin(), keysInCache_.end()); 
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

  // reset our keysInCache to be empty for this query and populate it
  auto keysInCache = std::make_shared<std::vector<std::shared_ptr<SegmentKey>>>();
  std::sort(potentialKeysToCache->begin(), potentialKeysToCache->end(), lessKeyValueSC);
  // Reverse this ordering as we want keys with the greatest value first
  std::reverse(potentialKeysToCache->begin(), potentialKeysToCache->end());

  size_t remainingCacheSize = maxSize_ * 0.99;
  for (const auto& segmentKey : *potentialKeysToCache) {
      size_t segmentSize = beladySCMiniCatalogue->getSegmentSize(segmentKey);
      int keyNextUse = beladySCMiniCatalogue->querySegmentNextUsedIn(segmentKey, queryNum);
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

void BeladySizeCostCachingPolicy::onWeight(const std::shared_ptr<std::unordered_map<std::shared_ptr<SegmentKey>, double>> &weightMap, long queryId) {
  // if new query executes, clear temporary weight updated keys
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


void BeladySizeCostCachingPolicy::generateCacheDecisions(int numQueries) {

  auto keysInCache = std::make_shared<std::vector<std::shared_ptr<SegmentKey>>>();
  for (int queryNum = 1; queryNum <= numQueries; ++queryNum) {
    beladySCMiniCatalogue->setCurrentQueryNum(queryNum);

    // Create list of potential keys to have in our cache after this query from keys already in the cache
    // and keys that are about to be queried
    auto keysInCurrentQuery = beladySCMiniCatalogue->getSegmentsInQuery(queryNum);
    // add keys in Current Query that aren't already in the cache
    auto allPotentialKeysToCache = std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate>();
    allPotentialKeysToCache.insert(keysInCache->begin(), keysInCache->end());
    allPotentialKeysToCache.insert(keysInCurrentQuery->begin(), keysInCurrentQuery->end());

    auto potentialKeysToCache = std::make_shared<std::vector<std::shared_ptr<SegmentKey>>>();
    potentialKeysToCache->insert(potentialKeysToCache->end(), allPotentialKeysToCache.begin(), allPotentialKeysToCache.end());

    // reset our keysInCache to be empty for this query and populate it
    keysInCache = std::make_shared<std::vector<std::shared_ptr<SegmentKey>>>();
    std::sort(potentialKeysToCache->begin(), potentialKeysToCache->end(), lessKeyValueBLD);
    // Reverse this ordering as we want keys with the greatest value first
    std::reverse(potentialKeysToCache->begin(), potentialKeysToCache->end());


    // Multiply by 0.99 as pre computed segment sizes are sometimes slightly off (< 1%), so this buffer
    // ensures we never store more in our cache than the max cache size
    size_t remainingCacheSize = maxSize_ * 0.99;
    for (const auto& segmentKey : *potentialKeysToCache) {
      size_t segmentSize = beladySCMiniCatalogue->getSegmentSize(segmentKey);
      int keyNextUse = beladySCMiniCatalogue->querySegmentNextUsedIn(segmentKey, queryNum);
      // Decide to cache key if there is room and the key is used again ie: keyNextUse != -1
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


CachingPolicyId BeladySizeCostCachingPolicy::id() {
  return BELADYSC;
}

void BeladySizeCostCachingPolicy::onNewQuery() {

}
