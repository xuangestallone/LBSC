//
//  20/5/20.
//

#define DOCTEST_CONFIG_IMPLEMENT
#include <doctest/doctest.h>

#include "Globals.h"

/**
 * Returns the name of the current test case
 *
 * @return
 */
const char* getCurrentTestName() { return doctest::detail::g_cs->currentTest->m_name; }

backward::SignalHandling sh;

/**
 * Tests entry point
 *
 * @param argc
 * @param argv
 * @return
 */
int main(int argc, char **argv) {

  spdlog::set_level(spdlog::level::debug);
  spdlog::set_pattern("[%H:%M:%S.%e] [thread %t] [%! (%s:%#)] [%l]  %v");
  spdlog::enable_backtrace(32);

  doctest::Context context;

  context.applyCommandLine(argc, argv);
  int rc = context.run();

  if (context.shouldExit()) // important - query flags (and --exit) rely on the user doing this
	return rc;

  return rc;
}
