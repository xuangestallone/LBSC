//
//  on 9/22/20.
//

#include <sstream>
#include <fmt/format.h>
#include <cstdlib>
#include <utility>
#include <chrono>
#include "normal/cache/BeladyCachingPolicy.h"
#include "normal/connector/MiniCatalogue.h"
#include <iostream>

std::shared_ptr<normal::connector::MiniCatalogue> normal::cache::beladyMiniCatalogue;

using namespace normal::cache;

bool lessKeyValue (const std::shared_ptr<SegmentKey> &key1, const std::shared_ptr<SegmentKey> &key2) {
  // Used to ensure weak ordering, can't have a key be less than itself
  int currentQuery = beladyMiniCatalogue->getCurrentQueryNum();
  // note that this is -1 if the key is never used again
  int key1NextUse = beladyMiniCatalogue->querySegmentNextUsedIn(key1, currentQuery);
  int key2NextUse = beladyMiniCatalogue->querySegmentNextUsedIn(key2, currentQuery);

  // For Belady key1 has lessValue than key2 if key2 is next used before key 1 is next used
  // If they are both next used at the same time, then key1 has lessValue if it is smaller
  if (key1NextUse == key2NextUse) {
    size_t key1Size = beladyMiniCatalogue->getSegmentSize(key1);
    size_t key2Size = beladyMiniCatalogue->getSegmentSize(key2);
    //return key1Size > key2Size;
    return key1Size < key2Size;
  } else if (key1NextUse == -1 || (key2NextUse != -1 && key2NextUse < key1NextUse)) {
    return true;
  } else if(key2NextUse == -1 || (key1NextUse != -1 && key1NextUse < key2NextUse)) {
    return false;
  }
  throw std::runtime_error("Error, lessKeyValue reached code that should not be reachable");
}

BeladyCachingPolicy::BeladyCachingPolicy(size_t maxSize, std::shared_ptr<normal::plan::operator_::mode::Mode> mode) :
        CachingPolicy(maxSize, std::move(mode)) {}

std::shared_ptr<BeladyCachingPolicy> BeladyCachingPolicy::make(size_t maxSize, std::shared_ptr<normal::plan::operator_::mode::Mode> mode) {
  return std::make_shared<BeladyCachingPolicy>(maxSize, mode);
}

void BeladyCachingPolicy::onLoad(const std::shared_ptr<SegmentKey> &key) {
  // Nothing to do for Belady caching policy 
}

void BeladyCachingPolicy::onRemove(const std::shared_ptr<SegmentKey> &key) {
  erase(key);
}

std::optional<std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>>>
BeladyCachingPolicy::onStore(const std::shared_ptr<SegmentKey> &key) {
  auto startTime = std::chrono::steady_clock::now();
  auto removableKeys = std::make_shared<std::vector<std::shared_ptr<SegmentKey>>>();

  auto segmentSize = key->getMetadata()->size();
  auto preComputedSize = beladyMiniCatalogue->getSegmentSize(key);
  // make sure segmentSize within 1% of our expected size, if not then something went wrong
  int absSizeDiff = abs((int) (segmentSize - preComputedSize));
  int onePercentSizeDiff = (int) ((float)segmentSize * 0.001);
  if (absSizeDiff > onePercentSizeDiff) {
    throw std::runtime_error("Error, segment has wrong size when compared to expected size");
    std::cout<<"segmentSize:"<<segmentSize<<"  preComputedSize:"<<preComputedSize<<"  "<<key->toString()<<std::endl;
  }
  if (maxSize_ < segmentSize) {
    return std::nullopt;
  }

  int currentQueryNum = beladyMiniCatalogue->getCurrentQueryNum();
  auto queryKey = queryNumToKeysInCache_.find(currentQueryNum);
  if (queryKey == queryNumToKeysInCache_.end()) {
    throw std::runtime_error("Error, query " + std::to_string(currentQueryNum) + " not populated in queryNumToKeysInCache_ in BeladyCachingPolicy.cpp");
  }
  auto keysThatShouldBeCached = queryKey->second;

  // Shouldn't cache segments that aren't supposed to be in the cache at the end of this query
  if (keysThatShouldBeCached->find(key) == keysThatShouldBeCached->end()) {
    return std::nullopt;
  }

  // if room for this key no need to evict anything
  if (segmentSize < freeSize_) {
    keysInCache_.insert(key);
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

  // Make sure we never use more than our cache size
  if (freeSize_ < 0) {
    throw std::runtime_error("Error, freeSize_ < 0, is: " + std::to_string(freeSize_));
  }
  auto stopTime = std::chrono::steady_clock::now();
  onStoreTime += std::chrono::duration_cast<std::chrono::nanoseconds>(stopTime - startTime).count();

  return std::optional(removableKeys);
}

std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>>
BeladyCachingPolicy::onToCache(std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> segmentKeys) {
  auto startTime = std::chrono::steady_clock::now();
  if (mode_->id() == normal::plan::operator_::mode::ModeId::PullupCaching) {
    return segmentKeys;
  }

  auto keysToCache = std::make_shared<std::vector<std::shared_ptr<SegmentKey>>>();
  int currentQueryNum = beladyMiniCatalogue->getCurrentQueryNum();
  auto queryKey = queryNumToKeysInCache_.find(currentQueryNum);
  if (queryKey == queryNumToKeysInCache_.end()) {
    throw std::runtime_error("Error, " + std::to_string(currentQueryNum) + " not populated in queryNumToKeysInCache_ in BeladyCachingPolicy.cpp");
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

void BeladyCachingPolicy::erase(const std::shared_ptr<SegmentKey> &key) {
  keysInCache_.erase(std::find(keysInCache_.begin(), keysInCache_.end(), key));
}

std::shared_ptr<std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate>>
BeladyCachingPolicy::getKeysetInCachePolicy() {
  auto keysetInCachePolicy = std::make_shared<std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate>>();
  keysetInCachePolicy->insert(keysInCache_.begin(), keysInCache_.end());
  return keysetInCachePolicy;
}

std::string BeladyCachingPolicy::showCurrentLayout() {
  std::stringstream ss;
  ss << "Total numbers: " << keysInCache_.size() << std::endl;
  for (auto const &segmentKey: keysInCache_) {
    int queryNum = beladyMiniCatalogue->getCurrentQueryNum();
    int keyNextUse = beladyMiniCatalogue->querySegmentNextUsedIn(segmentKey, queryNum);
    size_t keySize = beladyMiniCatalogue->getSegmentSize(segmentKey);
    ss << fmt::format("Key: {};\tSize: {}\n Next Use: {}", segmentKey->toString(), keySize, keyNextUse) << std::endl;
  }
  ss << "Max size: " << maxSize_ << std::endl;
  ss << "Free size: " << freeSize_ << std::endl;
  return ss.str();
}

void BeladyCachingPolicy::generateCacheDecisions(int numQueries) {
  auto keysInCache = std::make_shared<std::vector<std::shared_ptr<SegmentKey>>>();
  for (int queryNum = 1; queryNum <= numQueries; ++queryNum) {
    beladyMiniCatalogue->setCurrentQueryNum(queryNum);

    // Create list of potential keys to have in our cache after this query from keys already in the cache
    // and keys that are about to be queried
    auto keysInCurrentQuery = beladyMiniCatalogue->getSegmentsInQuery(queryNum);
    // add keys in Current Query that aren't already in the cache
    auto allPotentialKeysToCache = std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate>();
    allPotentialKeysToCache.insert(keysInCache->begin(), keysInCache->end());
    allPotentialKeysToCache.insert(keysInCurrentQuery->begin(), keysInCurrentQuery->end());

    auto potentialKeysToCache = std::make_shared<std::vector<std::shared_ptr<SegmentKey>>>();
    potentialKeysToCache->insert(potentialKeysToCache->end(), allPotentialKeysToCache.begin(), allPotentialKeysToCache.end());

    // reset our keysInCache to be empty for this query and populate it
    keysInCache = std::make_shared<std::vector<std::shared_ptr<SegmentKey>>>();
    std::sort(potentialKeysToCache->begin(), potentialKeysToCache->end(), lessKeyValue);
    // Reverse this ordering as we want keys with the greatest value first
    std::reverse(potentialKeysToCache->begin(), potentialKeysToCache->end());

    // Multiply by 0.99 as pre computed segment sizes are sometimes slightly off (< 1%), so this buffer
    // ensures we never store more in our cache than the max cache size
    size_t remainingCacheSize = maxSize_ * 0.99;
    for (const auto& segmentKey : *potentialKeysToCache) {
      size_t segmentSize = beladyMiniCatalogue->getSegmentSize(segmentKey);
      int keyNextUse = beladyMiniCatalogue->querySegmentNextUsedIn(segmentKey, queryNum);
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


CachingPolicyId BeladyCachingPolicy::id() {
  return BELADY; 
}

void BeladyCachingPolicy::onNewQuery() {

}
