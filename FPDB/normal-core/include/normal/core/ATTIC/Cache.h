//
//  20/01/19.
//

#ifndef NORMAL_NORMAL_NORMAL_CORE_SRC_CACHE_H
#define NORMAL_NORMAL_NORMAL_CORE_SRC_CACHE_H

#include <memory>
#include <string>
#include <unordered_map> 

#include "../Operator.h"
#include "normal/tuple/TupleSet.h"

using namespace normal::tuple;

class Cache {
private:
 
  


public:
	//name/id->tupleSets
  std::unordered_map<std::string, std::shared_ptr<TupleSet>> m_cacheData;

  ~Cache() = default;
};

#endif //NORMAL_NORMAL_NORMAL_CORE_SRC_CACHE_H
