//
//  on 7/7/20.
//

#define DOCTEST_CONFIG_IMPLEMENT
#include <doctest/doctest.h>
#include "normal/ssb/Globals.h"
#include "Tests.h"
#include "MathModelTest.h"

using namespace normal::ssb;

#define BACKWARD_HAS_BFD 1
#include <backward.hpp>
#include <normal/connector/MiniCatalogue.h>

backward::SignalHandling sh;

const char* getCurrentTestName() { return doctest::detail::g_cs->currentTest->m_name; }
const char* getCurrentTestSuiteName() { return doctest::detail::g_cs->currentTest->m_test_suite; }

int main(int argc, char **argv) {

  normal::connector::defaultMiniCatalogue = normal::connector::MiniCatalogue::defaultMiniCatalogue("pushdowndb", "ssb-sf10-sortlineorder/csv/");
  // math model test
  if (std::string(argv[1]) == "-m") {
    auto networkLimit = (size_t) (atof(argv[2]) * 1024 * 1024 * 1024 / 8);
    auto chunkSize = (size_t) (atol(argv[3]));
    auto numRuns = atoi(argv[4]);
    MathModelTest mathModelTest(networkLimit, chunkSize, numRuns);
    mathModelTest.runTest();
  }
  else if(std::string(argv[1]) == "-cs"){
    std::string dirPrefix = argv[2];
    calSegmetnInfo(dirPrefix);

  }
  // means we are specifying a different defaultMiniCatalogue directory to use
  else if (std::string(argv[1]) == "-d") {
    std::string dirPrefix = argv[2];
    auto cacheSize = (size_t) (atof(argv[3]) * 1024 * 1024 * 1024);
    auto modeType = atoi(argv[4]);
    auto cachingPolicyType = atoi(argv[5]);
    auto executeBatchSize = atoi(argv[6]);
    auto showQueryMetrics = atoi(argv[7]);
    auto wMethod = atoi(argv[8]); 
    bool sQMetrics = false;
    if(showQueryMetrics != 0){
      sQMetrics=true;
    }
    auto autoSplit = atoi(argv[9]);
    bool isSplit = false;
    if(autoSplit != 0){
      isSplit=true;
    }

    auto autoBitMap = atoi(argv[10]);
    bool isBitMap = false;
    if(autoBitMap != 0){
      isBitMap=true;
    }

    auto autoDP = atoi(argv[11]);
    bool isDP = false;
    if(autoDP != 0){
      isDP=true;
    } 

    auto autoMultiModel = atoi(argv[12]);
    bool isMultiModel = false;
    if(autoMultiModel != 0){
      isMultiModel = true;
    }   

    auto autoFeature = atoi(argv[12]);
    
    SPDLOG_INFO("Cache size: {}", cacheSize);
    SPDLOG_INFO("Mode type: {}", modeType);
    SPDLOG_INFO("CachingPolicy type: {}", cachingPolicyType);
    mainTest(cacheSize, modeType, cachingPolicyType, dirPrefix, 0, false, executeBatchSize, sQMetrics, wMethod, 
        isSplit, isBitMap, isDP, autoFeature, isMultiModel);
  }
  
  return 0;
}
