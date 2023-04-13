//
//  28/4/20.
//

#include <gandiva/tree_expr_builder.h>
#include "normal/expression/gandiva/Add.h"

using namespace normal::expression::gandiva;

Add::Add(const std::shared_ptr<Expression>& left, const std::shared_ptr<Expression>& right)
	: BinaryExpression(left, right) {
}

void Add::compile(std::shared_ptr<arrow::Schema> schema) {
  left_->compile(schema);
  right_->compile(schema);

  // FIXME: Verify both left and right are compatible types
  returnType_ = left_->getReturnType();

  auto function = "add";
  gandivaExpression_ = ::gandiva::TreeExprBuilder::MakeFunction(function, {left_->getGandivaExpression(), right_->getGandivaExpression()}, returnType_);

}

std::string Add::alias() {
  return "?column?";
}

std::shared_ptr<Expression> normal::expression::gandiva::plus(const std::shared_ptr<Expression>& left,
															  const std::shared_ptr<Expression>& right) {
  return std::make_shared<Add>(left, right);
}
