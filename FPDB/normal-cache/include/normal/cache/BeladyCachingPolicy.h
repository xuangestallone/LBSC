//
//  on 9/22/20.
//

#ifndef NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_BELADYCACHINGPOLICY_H
#define NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_BELADYCACHINGPOLICY_H

#include <memory>
#include <list>
#include <forward_list>
#include <unordered_set>
#include <unordered_map>
#include <vector>

#include "SegmentKey.h"
#include "CachingPolicy.h"
#include "normal/connector/MiniCatalogue.h"

namespace normal::cache {

// This must be populated with SegmentKey->[Query #s Segment is used in] and
// QueryNumber->[Involved Segment Keys] prior to executing any queries
extern std::shared_ptr<connector::MiniCatalogue> beladyMiniCatalogue;

class BeladyCachingPolicy: public CachingPolicy {

public: 
  explicit BeladyCachingPolicy(size_t maxSize, std::shared_ptr<normal::plan::operator_::mode::Mode> mode);
  static std::shared_ptr<BeladyCachingPolicy> make(size_t maxSize, std::shared_ptr<normal::plan::operator_::mode::Mode> mode);

  void onLoad(const std::shared_ptr<SegmentKey> &key) override;
  void onRemove(const std::shared_ptr<SegmentKey> &key) override;
  std::optional<std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>>> onStore(const std::shared_ptr<SegmentKey> &key) override;
  std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> onToCache(std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> segmentKeys) override;
  std::shared_ptr<std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate>> getKeysetInCachePolicy() override;
  std::string showCurrentLayout() override;

  void generateCacheDecisions(int numQueries);
  

  CachingPolicyId id() override;
  void onNewQuery() override;

private:
  std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate> keysInCache_;
  std::unordered_map<int, std::shared_ptr<std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate>>> queryNumToKeysInCache_;

  void erase(const std::shared_ptr<SegmentKey> &key);
};

}

#endif //NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_BELADYCACHINGPOLICY_H
