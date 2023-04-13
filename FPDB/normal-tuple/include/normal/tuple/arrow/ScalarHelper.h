//
//  10/4/20.
//

#ifndef NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_ARROW_SCALARHELPER_H
#define NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_ARROW_SCALARHELPER_H

class ScalarHelper {
public:
  virtual ~ScalarHelper() = default;

  virtual ScalarHelper &operator+=(const std::shared_ptr<ScalarHelper> &rhs) = 0;

  virtual std::shared_ptr<arrow::Scalar> asScalar() = 0;

  virtual std::string toString() = 0;
};

#endif //NORMAL_NORMAL_CORE_INCLUDE_NORMAL_CORE_ARROW_SCALARHELPER_H
