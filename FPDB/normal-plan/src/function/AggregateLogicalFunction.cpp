//
//  2/4/20.
//

#include <normal/plan/function/AggregateLogicalFunction.h>

using namespace normal::plan::function;

AggregateLogicalFunction::AggregateLogicalFunction(std::string type)
    : type_(std::move(type)) {
}

std::shared_ptr<normal::expression::gandiva::Expression> AggregateLogicalFunction::expression() {
  return expression_;
}

void AggregateLogicalFunction::expression(const std::shared_ptr<normal::expression::gandiva::Expression> &expression) {
  expression_ = expression;
}

void AggregateLogicalFunction::setName(const std::string &name) {
  name_ = name;
}

const std::string &AggregateLogicalFunction::getName() const {
  return name_;
}
