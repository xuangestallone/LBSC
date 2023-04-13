//
//  21/7/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_SCAN_SCANMESSAGE_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_SCAN_SCANMESSAGE_H

#include <string>
#include <vector>

#include <normal/core/message/Message.h>

using namespace normal::core::message;

namespace normal::pushdown::scan {

/**
 * A message to a scan operator telling it to scan a subset of columns from its data source
 */
class ScanMessage : public Message {

public:
  ScanMessage(std::vector<std::string> columnNames_, const std::string &sender, bool resultNeeded);
  [[nodiscard]] const std::vector<std::string> &getColumnNames() const;
  [[nodiscard]] bool isResultNeeded() const;

private:
  std::vector<std::string> columnNames_;
  bool resultNeeded_;

};

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_SCAN_SCANMESSAGE_H
