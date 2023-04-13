//
//  14/4/20.
//

#include "normal/pushdown/project/Project.h"

#include <utility>

#include <normal/core/message/TupleMessage.h>
#include <normal/core/message/CompleteMessage.h>
#include <normal/expression/gandiva/Projector.h>
#include <normal/expression/gandiva/Expression.h>
#include <normal/expression/gandiva/Column.h>

#include "normal/pushdown/Globals.h"

using namespace normal::core;

namespace normal::pushdown::project {

Project::Project(const std::string &Name,
                 std::vector<std::shared_ptr<normal::expression::gandiva::Expression>> Expressions,
                 long queryId)
    : Operator(Name, "Project", queryId),
      expressions_(std::move(Expressions)) {}

void Project::onStart() {
  SPDLOG_DEBUG("Starting operator  |  name: '{}'", this->name());
}

void Project::onReceive(const normal::core::message::Envelope &message) {
  if (message.message().type() == "StartMessage") {
    this->onStart();
  } else if (message.message().type() == "TupleMessage") {
    auto tupleMessage = dynamic_cast<const normal::core::message::TupleMessage &>(message.message());
    this->onTuple(tupleMessage);
  } else if (message.message().type() == "CompleteMessage") {
    auto completeMessage = dynamic_cast<const normal::core::message::CompleteMessage &>(message.message());
    this->onComplete(completeMessage);
  } else {
    // FIXME: Propagate error properly
    throw std::runtime_error("Unrecognized message type " + message.message().type());
  }
}

void Project::projectAndSendTuples() {
  if(tuples_ && tuples_->numRows() > 0) {

    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<std::shared_ptr<arrow::ChunkedArray>> arrowColumns;
    for (auto const &expression: expressions_) {
      auto columnName = std::static_pointer_cast<normal::expression::gandiva::Column>(expression)->getColumnName();
      auto arrowColumn = tuples_->table()->GetColumnByName(columnName);
      arrowColumns.emplace_back(arrowColumn);
      fields.emplace_back(std::make_shared<arrow::Field>(columnName, arrowColumn->type()));
    }
    auto projectedTuples = TupleSet::make(std::make_shared<arrow::Schema>(fields), arrowColumns);

    sendTuples(projectedTuples);

    // FIXME: Either set tuples to size 0 or use an optional
    tuples_ = nullptr;
  }
}

void Project::onTuple(const core::message::TupleMessage &message) {
  // Set the input schema if not yet set
  cacheInputSchema(message);

  // Add the tuples to the internal buffer
  bufferTuples(message);

  // Project and send if the buffer is full enough
  if (tuples_->numRows() > DefaultBufferSize) {
    projectAndSendTuples();
  }

}

void Project::cacheInputSchema(const core::message::TupleMessage &message) {
  if(!inputSchema_.has_value()){
	inputSchema_ = message.tuples()->table()->schema();
  }
}

void Project::sendTuples(std::shared_ptr<TupleSet> &projected) {
	std::shared_ptr<core::message::Message>
		tupleMessage = std::make_shared<core::message::TupleMessage>(projected, name());
	ctx()->tell(tupleMessage);
}

void Project::bufferTuples(const core::message::TupleMessage &message) {
  if (!tuples_) {
    // Initialise tuples buffer with message contents
    tuples_ = message.tuples();
  } else {
    // Append message contents to tuples buffer
    auto tables = {tuples_->table(), message.tuples()->table()};
    auto res = arrow::ConcatenateTables(tables);
    if (!res.ok()) {
      tuples_->table(*res);
    } else {
      // FIXME: Propagate error properly
      throw std::runtime_error(res.status().message());
    }
  }
}

void Project::onComplete(const normal::core::message::CompleteMessage &) {
  if(!ctx()->isComplete() && ctx()->operatorMap().allComplete(OperatorRelationshipType::Producer)){
    // Project and send any remaining tuples
    projectAndSendTuples();
	  ctx()->notifyComplete();
  }
}

}
