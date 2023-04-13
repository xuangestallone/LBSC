//
//  11/6/20.
//

#include "normal/expression/gandiva/GreaterThanOrEqualTo.h"

#include <utility>

#include "gandiva/selection_vector.h"
#include <gandiva/tree_expr_builder.h>

using namespace normal::expression::gandiva;

GreaterThanOrEqualTo::GreaterThanOrEqualTo(std::shared_ptr<Expression> Left, std::shared_ptr<Expression> Right)
	: BinaryExpression(std::move(Left), std::move(Right)) {}

void GreaterThanOrEqualTo::compile(std::shared_ptr<arrow::Schema> Schema) {

  left_->compile(Schema);
  right_->compile(Schema);

  auto leftGandivaExpression = left_->getGandivaExpression();
  auto rightGandivaExpression = right_->getGandivaExpression();

  auto greaterThanOrEqualToExpression = ::gandiva::TreeExprBuilder::MakeFunction(
	  "greater_than_or_equal_to",
	  {leftGandivaExpression, rightGandivaExpression},
	  ::arrow::boolean());

  gandivaExpression_ = greaterThanOrEqualToExpression;
  returnType_ = ::arrow::boolean();
}

std::string GreaterThanOrEqualTo::alias() {
  return genAliasForComparison(">=");
}

std::shared_ptr<Expression> normal::expression::gandiva::gte(const std::shared_ptr<Expression>& Left, const std::shared_ptr<Expression>& Right) {
  return std::make_shared<GreaterThanOrEqualTo>(Left, Right);
}
