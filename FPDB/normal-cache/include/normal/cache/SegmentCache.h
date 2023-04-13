//
//  19/5/20.
//

#ifndef NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SEGMENTCACHE_H
#define NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SEGMENTCACHE_H

#include <unordered_map>
#include <memory>

#include "SegmentKey.h"
#include "SegmentData.h"
#include "CachingPolicy.h"
#include <vector>

namespace normal::cache {

class SegmentCache {

public:
  explicit SegmentCache(std::shared_ptr<CachingPolicy> cachingPolicy_);

  std::vector<std::string> split(const std::string& str, const std::string& splitStr);

  static std::shared_ptr<SegmentCache> make();
  static std::shared_ptr<SegmentCache> make(const std::shared_ptr<CachingPolicy>& cachingPolicy);

  void store(const std::shared_ptr<SegmentKey>& key, const std::shared_ptr<SegmentData>& data);
  tl::expected<std::shared_ptr<SegmentData>, std::string> load(const std::shared_ptr<SegmentKey>& key);
  unsigned long remove(const std::shared_ptr<SegmentKey>& key);
  unsigned long remove(const std::function<bool(const SegmentKey& entry)>& predicate);
  std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> toCache(std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> segmentKeys);

  size_t getSize() const;
  const std::shared_ptr<CachingPolicy> &getCachingPolicy() const;
  int hitNum() const;
  int missNum() const;
  int shardHitNum() const;
  int shardMissNum() const;

  void addHitNum(size_t hitNum);
  void addMissNum(size_t missNum);
  void addShardHitNum(size_t shardHitNum);
  void addShardMissNum(size_t shardMissNum);

  int crtQueryHitNum() const;
  int crtQueryMissNum() const;
  int crtQueryShardHitNum() const;
  int crtQueryShardMissNum() const;

  void addCrtQueryHitNum(size_t hitNum);
  void addCrtQueryMissNum(size_t missNum);
  void addCrtQueryShardHitNum(size_t shardHitNum);
  void addCrtQueryShardMissNum(size_t shardMissNum);

  void clearMetrics();
  void clearCrtQueryMetrics();
  void clearCrtQueryShardMetrics();

private:
  void checkCacheConsistensyWithCachePolicy();

  std::unordered_map<std::shared_ptr<SegmentKey>, std::shared_ptr<SegmentData>, SegmentKeyPointerHash, SegmentKeyPointerPredicate> map_;
	std::shared_ptr<CachingPolicy> cachingPolicy_;
	int hitNum_ = 0;
	int missNum_ = 0;
	int shardHitNum_ = 0;
	int shardMissNum_ = 0;

	int crtQueryHitNum_ = 0;
	int crtQueryMissNum_ = 0;
	int crtQueryShardHitNum_ = 0;
	int crtQueryShardMissNum_ = 0;
};

}

#endif //NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_SEGMENTCACHE_H
