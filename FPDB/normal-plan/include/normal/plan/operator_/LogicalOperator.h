//
//  1/4/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_LOGICALOPERATOR_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_LOGICALOPERATOR_H

#include <memory>
#include <string>

#include <normal/core/Operator.h>
#include <normal/plan/operator_/type/OperatorType.h>
#include <normal/plan/mode/Mode.h>
#include <normal/cache/SegmentKey.h>

namespace normal::plan::operator_ {

class LogicalOperator {

public:
  explicit LogicalOperator(std::shared_ptr<type::OperatorType> type);
  virtual ~LogicalOperator() = default;

  std::shared_ptr<type::OperatorType> type();

  virtual std::shared_ptr<std::vector<std::shared_ptr<core::Operator>>> toOperators() = 0;

  virtual std::shared_ptr<std::vector<std::shared_ptr<normal::cache::SegmentKey>>> extractSegmentKeys();

  const std::string &getName() const;
  void setName(const std::string &Name);
  const std::shared_ptr<LogicalOperator> &getConsumer() const;
  void setConsumer(const std::shared_ptr<LogicalOperator> &Consumer);
  void setMode(const std::shared_ptr<normal::plan::operator_::mode::Mode> &mode);
  const std::shared_ptr<normal::plan::operator_::mode::Mode> &getMode() const;
  void setQueryId(long queryId);
  long getQueryId() const;

private:
  std::shared_ptr<type::OperatorType> type_;
  std::string name_;
  long queryId_{};
  std::shared_ptr<LogicalOperator> consumer_;
  std::shared_ptr<normal::plan::operator_::mode::Mode> mode_;
};

}

#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_LOGICALOPERATOR_H
