//
//  30/7/20.
//

#include "normal/tuple/StringArrayHasher.h"

using namespace normal::tuple;

StringArrayHasher::StringArrayHasher(const std::shared_ptr<::arrow::Array> &array) :
	ArrayHasher(array) {
  stringArray_ = std::static_pointer_cast<::arrow::StringArray>(array_);
}

size_t StringArrayHasher::hash(int64_t i) {
  return hash_(stringArray_->GetView(i));
}

