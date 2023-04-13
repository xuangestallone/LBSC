//
//  13/8/20.
//

#ifndef NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_ARRAYAPPENDERWRAPPER_H
#define NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_ARRAYAPPENDERWRAPPER_H

#include <utility>

#include <tl/expected.hpp>

#include "normal/tuple/ArrayAppender.h"
#include "normal/tuple/Globals.h"

namespace normal::tuple {

template<typename CType, typename ArrowType>
class ArrayAppenderWrapper : public ArrayAppender {

  using ArrowArrayType = typename ::arrow::TypeTraits<ArrowType>::ArrayType;
  using ArrowBuilderType = typename ::arrow::TypeTraits<ArrowType>::BuilderType;

public:
  explicit ArrayAppenderWrapper(size_t expectedSize = 0) :
	  ArrayAppender() {
	buffer_.reserve(expectedSize);
	builder_ = std::make_shared<ArrowBuilderType>();
  }

  static std::shared_ptr<ArrayAppenderWrapper> make(size_t expectedSize = 0) {
	return std::make_shared<ArrayAppenderWrapper>(expectedSize);
  }

  void appendValue(const std::shared_ptr<::arrow::Array> &array, size_t i) override {

	assert(array);
	assert(array->length() >= 0);
	assert(i < (size_t)array->length());

	buffer_.push_back(std::static_pointer_cast<ArrowArrayType>(array)->Value(i));
  }

  tl::expected<void, std::string> safeAppendValue(const std::shared_ptr<::arrow::Array> &array, size_t i) override {
	if (!array)
	  return tl::make_unexpected(
		  fmt::format("Cannot append value. Null source array"));
	if (array->length() < 0)
	  return tl::make_unexpected(
		  fmt::format("Cannot append value. Source array with negative length {}", array->length()));
	if (i >= (size_t)array->length())
	  return tl::make_unexpected(
		  fmt::format("Cannot append value. Source index {} outside bounds of source array {}", i, array->length()));

	appendValue(array, i);

	return {};
  }

  /**
   * Need this to compile on Mac, not sure why???
   *
   * @param builder
   * @param buffer
   * @return
   */
  ::arrow::Status appendValues(const std::shared_ptr<ArrowBuilderType> &builder, const std::vector<CType> &buffer);

  tl::expected<std::shared_ptr<arrow::Array>, std::string> finalize() override {
	::arrow::Status status;
	std::shared_ptr<ArrowArrayType> array;

	buffer_.shrink_to_fit();

	status = appendValues(builder_, buffer_);
	if (!status.ok()) {
	  return tl::make_unexpected(status.message());
	}

	status = builder_->Finish(&array);
	if (!status.ok()) {
	  return tl::make_unexpected(status.message());
	}

	buffer_.clear();

	return array;
  }

private:
  std::vector<CType> buffer_;
  std::shared_ptr<ArrowBuilderType> builder_;

};

template<>
void ArrayAppenderWrapper<std::string, ::arrow::StringType>::appendValue(const std::shared_ptr<::arrow::Array> &array,
																		 size_t i);

template<>
tl::expected<void, std::string>
ArrayAppenderWrapper<std::string, ::arrow::StringType>::safeAppendValue(const std::shared_ptr<::arrow::Array> &array,
																		size_t i);

class ArrayAppenderBuilder {
public:
  static tl::expected<std::shared_ptr<ArrayAppender>, std::string>
  make(const std::shared_ptr<::arrow::DataType> &type, size_t expectedSize = 0) {
	if (type->id() == ::arrow::StringType::type_id) {
	  return ArrayAppenderWrapper<std::string, ::arrow::StringType>::make(expectedSize);
	} else if (type->id() == ::arrow::Int32Type::type_id) {
	  return ArrayAppenderWrapper<::arrow::Int32Type::c_type, ::arrow::Int32Type>::make(expectedSize);
	} else if (type->id() == ::arrow::Int64Type::type_id) {
	  return ArrayAppenderWrapper<::arrow::Int64Type::c_type, ::arrow::Int64Type>::make(expectedSize);
	} else if (type->id() == ::arrow::DoubleType::type_id) {
	  return ArrayAppenderWrapper<::arrow::DoubleType::c_type, ::arrow::DoubleType>::make(expectedSize);
	} else {
	  return tl::make_unexpected(
		  fmt::format("ArrayAppender not implemented for type '{}'", type->name()));
	}
  }
};

}

#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_ARRAYAPPENDERWRAPPER_H
