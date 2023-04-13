//
//  8/5/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_TYPE_INTEGER64TYPE_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_TYPE_INTEGER64TYPE_H

#include <arrow/api.h>

#include <normal/core/type/Type.h>

namespace normal::core::type {

class Integer64Type: public Type {
private:

public:
  explicit Integer64Type() : Type("Int64") {}

  std::string asGandivaTypeString() override {
	return "BIGINT";
  }

  std::shared_ptr<arrow::DataType> asArrowType() override {
	return arrow::int64();
  }

};

std::shared_ptr<Type> integer64Type();

}

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_TYPE_INTEGER64TYPE_H
