//
//  6/5/20.
//

#ifndef NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_COLUMNNAME_H
#define NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_COLUMNNAME_H

#include <string>
#include <vector>

namespace normal::tuple {

class ColumnName {

public:

  /**
   * Converts column name to its canonical form - i.e. lower case
   *
   * @param columnName
   * @return
   */
  static std::string canonicalize(const std::string &columnName);

  static std::vector<std::string> canonicalize(const std::vector<std::string> &columnNames);

};

}

#endif //NORMAL_NORMAL_TUPLE_INCLUDE_NORMAL_TUPLE_COLUMNNAME_H
