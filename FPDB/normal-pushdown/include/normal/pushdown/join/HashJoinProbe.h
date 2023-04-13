//
//  29/4/20.
//

#ifndef NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_HASHJOINPROBE_H
#define NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_HASHJOINPROBE_H

#include <normal/core/Operator.h>
#include <normal/core/message/TupleMessage.h>
#include <normal/core/message/CompleteMessage.h>

#include "JoinPredicate.h"
#include "normal/pushdown/join/ATTIC/HashTableMessage.h"
#include "normal/pushdown/join/ATTIC/HashTable.h"
#include "normal/pushdown/join/ATTIC/HashJoinProbeKernel.h"
#include "HashJoinProbeKernel2.h"
#include "TupleSetIndexMessage.h"

namespace normal::pushdown::join {

/**
 * Operator implementing probe phase of a hash join
 *
 * Takes hashtable produced from build phase on one of the relations in the join (ideall the smaller) and uses it
 * to select rows from the both relations to include in the join.
 *
 */
class HashJoinProbe : public normal::core::Operator {

public:
  HashJoinProbe(const std::string &name, JoinPredicate pred, std::set<std::string> neededColumnNames, long queryId = 0);

  void onReceive(const core::message::Envelope &msg) override;

private:

  HashJoinProbeKernel2 kernel_;

  void onStart();
  void onTuple(const core::message::TupleMessage &msg);
  void onHashTable(const TupleSetIndexMessage &msg);
  void onComplete(const core::message::CompleteMessage &msg);
  void send(bool force);

};

}

#endif //NORMAL_NORMAL_PUSHDOWN_INCLUDE_NORMAL_PUSHDOWN_JOIN_HASHJOINPROBE_H
