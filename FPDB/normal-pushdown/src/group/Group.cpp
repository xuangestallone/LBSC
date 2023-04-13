//
//  13/5/20.
//

#include <normal/pushdown/group/Group.h>

using namespace normal::tuple;

namespace normal::pushdown::group {

Group::Group(const std::string &Name,
			 const std::vector<std::string> &GroupColumnNames,
			 const std::vector<std::string> &AggregateColumnNames,
			 const std::shared_ptr<std::vector<std::shared_ptr<aggregate::AggregationFunction>>> &AggregateFunctions,
			 long queryId) :
	Operator(Name, "Group", queryId),
  kernel2_(std::make_unique<GroupKernel2>(GroupColumnNames, AggregateColumnNames, *AggregateFunctions)) {
}

std::shared_ptr<Group> Group::make(const std::string &Name,
								   const std::vector<std::string> &groupColumnNames,
                   const std::vector<std::string> &aggregateColumnNames,
								   const std::shared_ptr<std::vector<std::shared_ptr<aggregate::AggregationFunction>>> &AggregateFunctions,
								   long queryId) {

  return std::make_shared<Group>(Name, groupColumnNames, aggregateColumnNames, AggregateFunctions, queryId);
}

void Group::onReceive(const normal::core::message::Envelope &msg) {
  if (msg.message().type() == "StartMessage") {
	this->onStart();
  } else if (msg.message().type() == "TupleMessage") {
	auto tupleMessage = dynamic_cast<const normal::core::message::TupleMessage &>(msg.message());
	this->onTuple(tupleMessage);
  } else if (msg.message().type() == "CompleteMessage") {
	auto completeMessage = dynamic_cast<const normal::core::message::CompleteMessage &>(msg.message());
	this->onComplete(completeMessage);
  } else {
	// FIXME: Propagate error properly
	throw std::runtime_error("Unrecognized message type " + msg.message().type());
  }
}

void Group::onStart() {
  SPDLOG_DEBUG("Starting");
}

void Group::onTuple(const normal::core::message::TupleMessage &message) {
  auto tupleSet = normal::tuple::TupleSet2::create(message.tuples());
  auto startTime = std::chrono::steady_clock::now();

  auto expectedGroupResult = kernel2_->group(*tupleSet);
  if(!expectedGroupResult)
    throw std::runtime_error(expectedGroupResult.error());

  auto stopTime = std::chrono::steady_clock::now();
  auto time = std::chrono::duration_cast<std::chrono::nanoseconds>(stopTime - startTime).count();
  groupTime_ += time;
  numRows_ += tupleSet->numRows();
  bytesGrouped_ += tupleSet->size();
}

void Group::onComplete(const normal::core::message::CompleteMessage &) {
  if (!ctx()->isComplete() && this->ctx()->operatorMap().allComplete(core::OperatorRelationshipType::Producer)) {

    if (kernel2_->hasInput()) {
      auto startTime = std::chrono::steady_clock::now();

      auto expectedGroupedTupleSet = kernel2_->finalise();
      if (!expectedGroupedTupleSet)
        throw std::runtime_error(expectedGroupedTupleSet.error());

      auto stopTime = std::chrono::steady_clock::now();
      auto time = std::chrono::duration_cast<std::chrono::nanoseconds>(stopTime - startTime).count();
      groupTime_ += time;

      double speed = (((double) bytesGrouped_) / 1024.0 / 1024.0) / (((double) groupTime_) / 1000000000);
      SPDLOG_DEBUG("Group time: {}, numBytes: {}, speed: {}MB/s, numRows: {}, {}", groupTime_, bytesGrouped_, speed, numRows_, name());

      std::shared_ptr<normal::core::message::Message>
              tupleMessage =
              std::make_shared<normal::core::message::TupleMessage>(expectedGroupedTupleSet.value()->toTupleSetV1(),
                                                                    this->name());
      ctx()->tell(tupleMessage);
    }

    ctx()->notifyComplete();
  }
}

}
