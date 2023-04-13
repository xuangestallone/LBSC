#ifndef NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_BELADYSIZECOSTCACHINGPOLICY_H
#define NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_BELADYSIZECOSTCACHINGPOLICY_H

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

extern std::shared_ptr<connector::MiniCatalogue> beladySCMiniCatalogue;
extern size_t bldscLessValue1Gloal;
extern size_t bldscLessValue2Gloal;
extern size_t bldscLessValue3Gloal;
extern size_t bldscLessValue4Gloal;

class BeladySizeCostCachingPolicy: public CachingPolicy {

public:
  explicit BeladySizeCostCachingPolicy(size_t maxSize, std::shared_ptr<normal::plan::operator_::mode::Mode> mode);
  static std::shared_ptr<BeladySizeCostCachingPolicy> make(size_t maxSize, std::shared_ptr<normal::plan::operator_::mode::Mode> mode);

  void onLoad(const std::shared_ptr<SegmentKey> &key) override;
  void onRemove(const std::shared_ptr<SegmentKey> &key) override;
  std::optional<std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>>> onStore(const std::shared_ptr<SegmentKey> &key) override;
  std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> onToCache(std::shared_ptr<std::vector<std::shared_ptr<SegmentKey>>> segmentKeys) override;
  std::shared_ptr<std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate>> getKeysetInCachePolicy() override;
  std::string showCurrentLayout() override;

  void generateCacheDecisions(int numQueries);

  void onWeight(const std::shared_ptr<std::unordered_map<std::shared_ptr<SegmentKey>, double>> &weightMap, long queryId);
  void updateWeight(long queryNum);

  CachingPolicyId id() override;
  void onNewQuery() override;
  void setWeightMethod(int wMethod);

private:
  std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate> keysInCache_;
  std::unordered_map<int, std::shared_ptr<std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate>>> queryNumToKeysInCache_;
  std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate> keySet_;

  long currentQueryId_;
  std::unordered_set<std::shared_ptr<SegmentKey>, SegmentKeyPointerHash, SegmentKeyPointerPredicate> weightUpdatedKeys_;

  void erase(const std::shared_ptr<SegmentKey> &key);
  int weightMethod_;
  double delta_;
};

}

#endif //NORMAL_NORMAL_CACHE_INCLUDE_NORMAL_CACHE_BELADYSIZECOSTCACHINGPOLICY_H
