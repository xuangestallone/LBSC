//
//  on 10/10/20.
//

#ifndef NORMAL_TESTS_H
#define NORMAL_TESTS_H

#include <cstddef>
#include <iostream>
#include <vector>

namespace normal::ssb {

  std::vector<std::string> split(const std::string& str, const std::string& splitStr);

  void mainTest(size_t cacheSize, int modeType, int cachingPolicyType, const std::string& dirPrefix,
                size_t networkLimit, bool writeResults, int executeBatchSize_, bool showQueryMetrics, int wMethod,
                bool isSplit, bool isBitMap, bool isDP, int featureLabel, bool multiModel);
  
  void calSegmetnInfo(const std::string& dirPrefix);
}

#endif //NORMAL_TESTS_H
