//
//  11/12/19.
//

#ifndef NORMAL_NORMAL_NORMAL_CORE_SRC_TUPLEMESSAGE_H
#define NORMAL_NORMAL_NORMAL_CORE_SRC_TUPLEMESSAGE_H

#include <memory>

#include <caf/all.hpp>

#include "normal/core/message/Message.h"
#include "normal/tuple/TupleSet.h"

using namespace normal::tuple;

namespace normal::core::message {

/**
 * Message containing a list of tuples
 */
class TupleMessage : public Message {

private:
  std::shared_ptr<TupleSet> tuples_;

public:
  explicit TupleMessage(std::shared_ptr<TupleSet> tuples, std::string sender);
  ~TupleMessage() override = default;

  [[nodiscard]] std::shared_ptr<TupleSet> tuples() const;

};

}

#endif //NORMAL_NORMAL_NORMAL_CORE_SRC_TUPLEMESSAGE_H
