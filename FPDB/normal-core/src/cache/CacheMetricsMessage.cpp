//
//  on 11/2/20.
//

#include "normal/core/cache/CacheMetricsMessage.h"

using namespace normal::core::cache;

// CacheMetricsMessage::CacheMetricsMessage(size_t hitNum, size_t missNum, 
//   size_t shardHitNum, size_t shardMissNum, 
//   double partialHitNum, double partialMissNum,
//   const std::string &sender) :
//   Message("CacheMetricsMessage", sender),
//   hitNum_(hitNum),
//   missNum_(missNum),
//   shardHitNum_(shardHitNum),
//   shardMissNum_(shardMissNum)
//   partialHitNum_(partialHitNum),
//   partialMissNum_(partialMissNum){}

CacheMetricsMessage::CacheMetricsMessage(size_t hitNum, size_t missNum, 
  size_t shardHitNum, size_t shardMissNum, 
  const std::string &sender) :
  Message("CacheMetricsMessage", sender),
  hitNum_(hitNum),
  missNum_(missNum),
  shardHitNum_(shardHitNum),
  shardMissNum_(shardMissNum){}

// std::shared_ptr<CacheMetricsMessage>
// CacheMetricsMessage::make(size_t hitNum, size_t missNum, 
//   size_t shardHitNum, size_t shardMissNum, 
//   double partialHitNum, double partialMissNum,
//   const std::string &sender) {
//   return std::make_shared<CacheMetricsMessage>(hitNum, missNum, shardHitNum, shardMissNum, partialHitNum, partialMissNum, sender);
// }

std::shared_ptr<CacheMetricsMessage>
CacheMetricsMessage::make(size_t hitNum, size_t missNum, 
  size_t shardHitNum, size_t shardMissNum, 
  const std::string &sender) {
  return std::make_shared<CacheMetricsMessage>(hitNum, missNum, shardHitNum, shardMissNum, sender);
}

size_t CacheMetricsMessage::getHitNum() const {
  return hitNum_;
}

size_t CacheMetricsMessage::getMissNum() const {
  return missNum_;
}

size_t CacheMetricsMessage::getShardHitNum() const {
  return shardHitNum_;
}

size_t CacheMetricsMessage::getShardMissNum() const {
  return shardMissNum_;
}

//new
// double CacheMetricsMessage::getPartialHitNum() const {
//   return partialHitNum_;
// }

// double CacheMetricsMessage::getPartialMissNum() const {
//   return partialMissNum_;
// }
