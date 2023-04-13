//
//  4/3/20.
//

#include "normal/core/message/Envelope.h"

#include <memory>
#include <utility>

namespace normal::core::message {

Envelope::Envelope(std::shared_ptr<Message> message) :
    message_(std::move(message)) {}

const Message &Envelope::message() const {
  return *message_;
}

} // namespace