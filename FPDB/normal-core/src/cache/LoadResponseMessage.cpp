//
//  20/5/20.
//

#include "normal/core/cache/LoadResponseMessage.h"

#include <utility>

using namespace normal::core::cache;

LoadResponseMessage::LoadResponseMessage(std::unordered_map<std::shared_ptr<SegmentKey>,
															std::shared_ptr<SegmentData>,
															SegmentKeyPointerHash,
															SegmentKeyPointerPredicate> segments,
										 const std::string &sender,
										 std::vector<std::shared_ptr<SegmentKey>> segmentKeysToCache) :
	Message("LoadResponseMessage", sender),
	segments_(std::move(segments)),
	segmentKeysToCache_(std::move(segmentKeysToCache)) {}

std::shared_ptr<LoadResponseMessage>

LoadResponseMessage::make(std::unordered_map<std::shared_ptr<SegmentKey>,
											 std::shared_ptr<SegmentData>,
											 SegmentKeyPointerHash,
											 SegmentKeyPointerPredicate> segments,
						  const std::string &sender,
						  std::vector<std::shared_ptr<SegmentKey>> segmentKeysToCache) {
  return std::make_shared<LoadResponseMessage>(std::move(segments), sender, std::move(segmentKeysToCache));
}

const std::unordered_map<std::shared_ptr<SegmentKey>,
						 std::shared_ptr<SegmentData>,
						 SegmentKeyPointerHash,
						 SegmentKeyPointerPredicate> &LoadResponseMessage::getSegments() const {
  return segments_;
}

std::string LoadResponseMessage::toString() const {

  std::string s = "Segments loaded: {";
  for (auto it = segments_.begin(); it != segments_.end(); ++it) {
	s += fmt::format("{}", it->first->toString());
	s += ": ";
	s += fmt::format("{}", it->second->getColumn()->toString());
	if (std::next(it) != segments_.end())
	  s += ", ";
  }

  s += "}, Segments to cache: {";
  for (auto it = segmentKeysToCache_.begin(); it != segmentKeysToCache_.end(); ++it) {
	s += fmt::format("{}", it->get()->toString());
	if (std::next(it) != segmentKeysToCache_.end()) {
	  s += ", ";
	}
  }
  s += "}";

  return s;
}

const std::vector<std::shared_ptr<SegmentKey>> &LoadResponseMessage::getSegmentKeysToCache() const {
  return segmentKeysToCache_;
}
