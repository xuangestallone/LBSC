//
//  8/5/20.
//

#ifndef NORMAL_NORMAL_EXPRESSION_GANDIVA_TEST_GLOBALS_H
#define NORMAL_NORMAL_EXPRESSION_GANDIVA_TEST_GLOBALS_H

#define LOG_LEVEL SPDLOG_LEVEL_DEBUG

#define SPDLOG_ACTIVE_LEVEL LOG_LEVEL
#include "spdlog/spdlog.h"

#define BACKWARD_HAS_BFD 1
#include <backward.hpp>

namespace normal::expression::gandiva::test {
}

#endif //NORMAL_NORMAL_EXPRESSION_GANDIVA_TEST_GLOBALS_H
