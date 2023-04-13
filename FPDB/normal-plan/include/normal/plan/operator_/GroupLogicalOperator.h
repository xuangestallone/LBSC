//
//  on 7/17/20.
//

#ifndef NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_GROUPLOGICALOPERATOR_H
#define NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_GROUPLOGICALOPERATOR_H

#include <normal/core/Operator.h>
#include <normal/plan/operator_/LogicalOperator.h>
#include <normal/plan/function/AggregateLogicalFunction.h>

namespace normal::plan::operator_ {

class GroupLogicalOperator : public LogicalOperator {

public:
  GroupLogicalOperator(std::shared_ptr<std::vector<std::string>> groupColumnNames,
                       std::shared_ptr<std::vector<std::string>> aggregateColumnNames,
                       std::shared_ptr<std::vector<std::shared_ptr<function::AggregateLogicalFunction>>> functions,
                       std::shared_ptr<std::vector<std::shared_ptr<expression::gandiva::Expression>>> projectExpression,
                       std::shared_ptr<LogicalOperator> producer);

  std::shared_ptr<std::vector<std::shared_ptr<core::Operator>>> toOperators() override;

  [[nodiscard]] const std::shared_ptr<LogicalOperator> &getProducer() const;

  void setNumConcurrentUnits(int numConcurrentUnits);

private:
  std::shared_ptr<std::vector<std::string>> groupColumnNames_;
  std::shared_ptr<std::vector<std::string>> aggregateColumnNames_;
  std::shared_ptr<std::vector<std::shared_ptr<function::AggregateLogicalFunction>>> functions_;
  std::shared_ptr<std::vector<std::shared_ptr<expression::gandiva::Expression>>> projectExpression_;

  std::shared_ptr<LogicalOperator> producer_;
  int numConcurrentUnits_{};
};

}


#endif //NORMAL_NORMAL_SQL_INCLUDE_NORMAL_SQL_LOGICAL_GROUPLOGICALOPERATOR_H
