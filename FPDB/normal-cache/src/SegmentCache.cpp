//
//  19/5/20.
//

#include "normal/cache/SegmentCache.h"

#include <utility>
#include <normal/cache/LRUCachingPolicy.h>
#include <normal/plan/Planner.h>
#include <iostream>
#include <normal/connector/s3/S3SelectPartition.h>

using namespace normal::cache;

SegmentCache::SegmentCache(std::shared_ptr<CachingPolicy> cachingPolicy) :
	cachingPolicy_(std::move(cachingPolicy)) {
}

std::shared_ptr<SegmentCache> SegmentCache::make() {
  return std::make_shared<SegmentCache>(LRUCachingPolicy::make());
}

std::shared_ptr<SegmentCache> SegmentCache::make(const std::shared_ptr<CachingPolicy>& cachingPolicy) {
  return std::make_shared<SegmentCache>(cachingPolicy);
}

void SegmentCache::store(const std::shared_ptr<SegmentKey> &key, const std::shared_ptr<SegmentData> &data) {
  auto removableKeys = cachingPolicy_->onStore(key);

  if (removableKeys.has_value()) {
    for (auto const &removableKey: *removableKeys.value()) {
      map_.erase(removableKey);
    }
    map_.emplace(key, data);
  }

}

std::vector<std::string> SegmentCache::split(const std::string& str, const std::string& splitStr) {
  std::vector<std::string> res;
  std::string::size_type pos1, pos2;
  pos2 = str.find(splitStr);
  pos1 = 0;
  while (pos2 != std::string::npos)
  {
    res.push_back(str.substr(pos1, pos2 - pos1));
    pos1 = pos2 + 1;
    pos2 = str.find(splitStr, pos1);
  }
  if (pos1 < str.length()) {
    res.push_back(str.substr(pos1));
  }
  return res;
}

tl::expected<std::shared_ptr<SegmentData>,
			 std::string> SegmentCache::load(const std::shared_ptr<SegmentKey> &key) {

  cachingPolicy_->onLoad(key);

  auto mapIterator = map_.find(key);
  if (mapIterator == map_.end()) {
	  return tl::unexpected(fmt::format("Segment for key '{}' not found", key->toString()));
  } else {
    auto cacheEntry = mapIterator->second;
    return mapIterator->second;
  }
}

unsigned long SegmentCache::remove(const std::shared_ptr<SegmentKey> &key) {
  return map_.erase(key);
}

unsigned long SegmentCache::remove(const std::function<bool(const SegmentKey &)> &predicate) {
  unsigned long erasedCount = 0;
  for (const auto &mapEntry: map_) {
    if (predicate(*mapEntry.first)) {
      cachingPolicy_->onRemove(mapEntry.first);
      map_.erase(mapEntry.first);
      ++erasedCount;
    }
  }
  return erasedCount;
}

size_t SegmentCache::getSize() const {
  return map_.size();
}

std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>>
SegmentCache::toCache(std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> segmentKeys) {
  return cachingPolicy_->onToCache(segmentKeys);
}

int SegmentCache::hitNum() const {
  return hitNum_;
}

int SegmentCache::missNum() const {
  return missNum_;
}

int SegmentCache::shardHitNum() const {
  return shardHitNum_;
}

int SegmentCache::shardMissNum() const {
  return shardMissNum_;
}


void SegmentCache::clearMetrics() {
  hitNum_ = 0;
  missNum_ = 0;
  shardHitNum_ = 0;
  shardMissNum_ = 0;
}

const std::shared_ptr<CachingPolicy> &SegmentCache::getCachingPolicy() const {
  return cachingPolicy_;
}

int SegmentCache::crtQueryHitNum() const {
  return crtQueryHitNum_;
}

int SegmentCache::crtQueryMissNum() const {
  return crtQueryMissNum_;
}

int SegmentCache::crtQueryShardHitNum() const {
  return crtQueryShardHitNum_;
}

int SegmentCache::crtQueryShardMissNum() const {
  return crtQueryShardMissNum_;
}

void SegmentCache::clearCrtQueryMetrics() {
  crtQueryHitNum_ = 0;
  crtQueryMissNum_ = 0;
}

void SegmentCache::clearCrtQueryShardMetrics() {
  crtQueryShardHitNum_ = 0;
  crtQueryShardMissNum_ = 0;
}

void SegmentCache::addHitNum(size_t hitNum) {
  hitNum_ += hitNum;
}

void SegmentCache::addMissNum(size_t missNum) {
  missNum_ += missNum;
}

void SegmentCache::addShardHitNum(size_t shardHitNum) {
  shardHitNum_ += shardHitNum;
}

void SegmentCache::addShardMissNum(size_t shardMissNum) {
  shardMissNum_ += shardMissNum;
}

void SegmentCache::addCrtQueryHitNum(size_t hitNum) {
  crtQueryHitNum_ += hitNum;
}

void SegmentCache::addCrtQueryMissNum(size_t missNum) {
  crtQueryMissNum_ += missNum;
}

void SegmentCache::addCrtQueryShardHitNum(size_t shardHitNum) {
  crtQueryShardHitNum_ += shardHitNum;
}

void SegmentCache::addCrtQueryShardMissNum(size_t shardMissNum) {
  crtQueryShardMissNum_ += shardMissNum;
}

void SegmentCache::checkCacheConsistensyWithCachePolicy() {
  auto keysInCachePolicy = cachingPolicy_->getKeysetInCachePolicy();
  if (keysInCachePolicy->size() != map_.size()) {
    throw std::runtime_error("Error, cache policy key set has different size than segment cache keyset");
  }

  // make sure all keys in caching policy cache are in segment cache
  // don't need to worry about checking the other way around since we are comparing sets and they are the same size
  // so checking one way will show any errors
  for (auto segmentKey : *keysInCachePolicy) {
    if (map_.find(segmentKey) == map_.end()) {
      throw std::runtime_error("Error, cache policy key set has a key not present in the segment cache");
    }
  }
}
