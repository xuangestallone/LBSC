//
//  7/3/20.
//

#include <sstream>
#include <utility>

#include "arrow/visitor_inline.h"
#include "arrow/scalar.h"

#include <normal/tuple/arrow/ScalarHelperBuilder.h>
#include <normal/expression/gandiva/Projector.h>
#include "normal/pushdown/aggregate/Sum.h"

namespace normal::pushdown::aggregate {

Sum::Sum(std::string columnName, std::shared_ptr<normal::expression::gandiva::Expression> expression) :
    AggregationFunction(std::move(columnName)),
    expression_(std::move(expression)) {}


void normal::pushdown::aggregate::Sum::apply(std::shared_ptr<aggregate::AggregationResult> result, std::shared_ptr<TupleSet> tuples) {
  // Set the input schema if not yet set
  cacheInputSchema(*tuples);

  // Build and set the expression projector if not yet set
  buildAndCacheProjector();

  auto resultType = this->expression_->getReturnType();

  std::shared_ptr<arrow::Scalar> batchSum = tuples->visit([&](auto accum, auto &batch) -> auto{

    auto arrayVector = projector_.value()->evaluate(batch);
    auto array = arrayVector.at(0);

    // Initialise accumulator
    if(accum == nullptr) {
      if (resultType->id() == arrow::float64()->id()) {
        accum = arrow::MakeScalar(arrow::float64(), 0.0).ValueOrDie();
      } else if (resultType->id() == arrow::int32()->id()) {
        accum = arrow::MakeScalar(arrow::int32(), 0).ValueOrDie();
      } else if (resultType->id() == arrow::int64()->id()) {
        accum = arrow::MakeScalar(arrow::int64(), 0).ValueOrDie();
      } else {
        throw std::runtime_error("Accumulator init for type " + accum->type->name() + " not implemented yet");
      }
    }

    // FIXME: Dont think this if/then else against arrow types is necessary

    auto colType = array->type();
    if (colType->Equals(arrow::Int32Type())) {
      auto typedArray = std::static_pointer_cast<arrow::Int32Array>(array);
      auto typedAccum = std::static_pointer_cast<arrow::Int32Scalar>(accum);
      for (int i = 0; i < batch.num_rows(); ++i) {
        int val = typedArray->Value(i);
        typedAccum->value += val;
      }
    } else if (colType->Equals(arrow::Int64Type())) {
      auto typedArray = std::static_pointer_cast<arrow::Int64Array>(array);
      auto typedAccum = std::static_pointer_cast<arrow::Int64Scalar>(accum);
      for (int i = 0; i < batch.num_rows(); ++i) {
        long val = typedArray->Value(i);
        typedAccum->value += val;
      }
    } else if (colType->Equals(arrow::StringType())) {
      throw std::runtime_error("Can't sum strings, cast first");
    } else if (colType->Equals(arrow::DoubleType())) {
      auto typedArray = std::static_pointer_cast<arrow::DoubleArray>(array);
      auto typedAccum = std::static_pointer_cast<arrow::DoubleScalar>(accum);

      for (int i = 0; i < batch.num_rows(); ++i) {
        double val = typedArray->Value(i);
        typedAccum->value += val;
      }
    }
    else {
      throw std::runtime_error("Unrecognized type " + colType->name());
    }

    return accum;
  });

  if (!batchSum) {
    throw std::runtime_error("No batchSum result");
  }

  // Update the current running sum
  auto expectedCurrentSum = result->get(SUM_RESULT_KEY);
  if (!expectedCurrentSum.has_value()) {
    result->put(SUM_RESULT_KEY, batchSum);
  } else {
    // Wrap batchSum and currentSum
    auto expectedBatchSumWrapped = ScalarHelperBuilder::make(batchSum);
    if (!expectedBatchSumWrapped.has_value()) {
      throw std::runtime_error(expectedBatchSumWrapped.error());
    }
    auto batchSumWrapped = expectedBatchSumWrapped.value();

    auto expectedCurrentSumWrapped = ScalarHelperBuilder::make(expectedCurrentSum.value());
    if (!expectedCurrentSumWrapped.has_value()) {
      throw std::runtime_error(expectedBatchSumWrapped.error());
    }
    auto currentSumWrapped = expectedCurrentSumWrapped.value();

    // Update
    currentSumWrapped->operator+=(batchSumWrapped);
    result->put(SUM_RESULT_KEY, currentSumWrapped->asScalar());
  }
}

std::shared_ptr<arrow::DataType> Sum::returnType() {
  return expression_->getReturnType();
}

void Sum::finalize(std::shared_ptr<aggregate::AggregationResult> result) {
  result->finalize(result->get(SUM_RESULT_KEY).value());
}

void Sum::buildAndCacheProjector() {
  if(!projector_.has_value()){
	auto expressionsVec = {this->expression_};
	projector_ = std::make_shared<expression::gandiva::Projector>(expressionsVec);
	projector_.value()->compile(inputSchema_.value());
  }
}

void Sum::cacheInputSchema(const TupleSet &tuples) {
  if(!inputSchema_.has_value()){
	inputSchema_ = tuples.table()->schema();
  }
}

}
