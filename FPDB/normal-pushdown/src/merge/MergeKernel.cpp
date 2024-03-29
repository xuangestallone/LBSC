//
//  20/7/20.
//

#include "normal/pushdown/merge/MergeKernel.h"
#include <tl/expected.hpp>

using namespace normal::pushdown::merge;

tl::expected<void, std::string> MergeKernel::validateTupleSets(const std::shared_ptr<TupleSet2> &tupleSet1,
														  const std::shared_ptr<TupleSet2> &tupleSet2) {

  // If either of the tuple sets contain columns (meaning they need to be merged) then check
  // they are merge-able. If either tuple set contains no columns, then merging them simply
  if (tupleSet1->numColumns() > 0 && tupleSet2->numColumns() > 0) {

	if (tupleSet1->numRows() != tupleSet2->numRows()) {
	  return tl::unexpected(fmt::format(
            "Cannot merge TupleSets, number of rows must be equal. "
            "tupleSet1.numRows: {} != tupleSet2.numRows: {}",
            tupleSet1->numRows(), tupleSet2->numRows()));
	}

	// Check field names do not contain duplicates
	if (tupleSet1->getArrowTable().has_value() && tupleSet2->getArrowTable().has_value()) {
	  for (const auto &field1: tupleSet1->getArrowTable().value()->schema()->fields()) {
		for (const auto &field2: tupleSet2->getArrowTable().value()->schema()->fields()) {
		  if (field1->name() == field2->name()) {
		    return tl::unexpected(fmt::format(
                "Cannot merge TupleSets, field names must not contain duplicates. "
                "Multiple fields with name: {}",
                field1->name()));
		  }
		}
	  }
	}
  }

  return {};
}

std::shared_ptr<Schema> MergeKernel::mergeSchema(const std::shared_ptr<TupleSet2> &tupleSet1,
												 const std::shared_ptr<TupleSet2> &tupleSet2) {

  auto mergedFields = std::vector<std::shared_ptr<arrow::Field>>{};

  if (tupleSet1->getArrowTable().has_value()){
	const auto &fields1 = tupleSet1->getArrowTable().value()->schema()->fields();
	mergedFields.insert(std::end(mergedFields), std::begin(fields1), std::end(fields1));
  }

  if (tupleSet2->getArrowTable().has_value()) {
	const auto &fields2 = tupleSet2->getArrowTable().value()->schema()->fields();
	mergedFields.insert(std::end(mergedFields), std::begin(fields2), std::end(fields2));
  }

  const auto &mergedArrowSchema = std::make_shared<::arrow::Schema>(mergedFields);
  const auto &mergedSchema = Schema::make(mergedArrowSchema);

  return mergedSchema;
}

std::vector<std::shared_ptr<::arrow::ChunkedArray>> MergeKernel::mergeArrays(const std::shared_ptr<TupleSet2> &tupleSet1,
																			 const std::shared_ptr<TupleSet2> &tupleSet2) {

  auto mergedArrays = std::vector<std::shared_ptr<::arrow::ChunkedArray>>{};

  if (tupleSet1->getArrowTable().has_value()) {
	const auto &arrowArrays1 = tupleSet1->getArrowTable().value()->columns();
	mergedArrays.insert(std::end(mergedArrays), std::begin(arrowArrays1), std::end(arrowArrays1));
  }

  if (tupleSet2->getArrowTable().has_value()) {
	const auto &arrowArrays2 = tupleSet2->getArrowTable().value()->columns();
	mergedArrays.insert(std::end(mergedArrays), std::begin(arrowArrays2), std::end(arrowArrays2));
  }

  return mergedArrays;
}

tl::expected<std::shared_ptr<TupleSet2>, std::string>
MergeKernel::merge(const std::shared_ptr<TupleSet2> &tupleSet1,
				   const std::shared_ptr<TupleSet2> &tupleSet2) {

  // Validate the tuple sets
  const auto valid = validateTupleSets(tupleSet1, tupleSet2);
  if (!valid)
	return tl::unexpected(valid.error());

  // Merge schema
  const auto &mergedSchema = mergeSchema(tupleSet1, tupleSet2);

  // Merge arrays
  const auto &mergedArrays = mergeArrays(tupleSet1, tupleSet2);

  // Create the merged tuple set

  /**
   * FIXME: System is interchangeably using 0-length vectors and null optionals to represent
   *  an empty tupleset. Need to standardise on one way or the other. Preferring null optional
   *  for the time being as its less likely to hide empty tables not being handled properly.
   */
  if (!mergedSchema->fields().empty() && !mergedArrays.empty()) {
	const auto &mergedTable = ::arrow::Table::Make(mergedSchema->getSchema(), mergedArrays);
	const auto &mergedTupleSet = TupleSet2::make(mergedTable);
	return mergedTupleSet;
  } else {
	const auto &mergedTupleSet = TupleSet2::make2();
	return mergedTupleSet;
  }

}





