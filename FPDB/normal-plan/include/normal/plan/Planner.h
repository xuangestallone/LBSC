//
//  16/4/20.
//

#ifndef NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PLANNER_H
#define NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PLANNER_H

#include <memory>

#include "PhysicalPlan.h"
#include "LogicalPlan.h"
#include <iostream>
#include <vector>
#include <set>
#include <bitset>


namespace normal::plan {

/**
 * Query planner, takes a logical plan and produces a physical one
 *
 * It has two functionalities:
 * 1) add parallelism
 * 2) add hybrid framework (pushdown + caching)
 *
 */
class Planner {

public:

  /**
   * Generate the physical plan from the logical plan
   *
   * @param logicalPlan
   * @param mode: full pushdown, pullup caching, hybrid caching
   * @return physicalPlan
   */
  static std::shared_ptr<PhysicalPlan> generate(const LogicalPlan &logicalPlan,
                                                const std::shared_ptr<normal::plan::operator_::mode::Mode>& mode);

  static void setQueryId(long queryId);
  static long getQueryId();

  static void setDP(bool isDP);
  static bool getDP();

  static void setSplit(bool isSplit);
  static bool getSplit();

  static void setBitmap(bool isBitmap);
  static bool getBitmap();

  static void setVKeyDate(std::vector<std::set<int>> vKeyDate);  
  static void setSLoCommitdate(std::map<std::string,int> sLoCommitdate);
  static void setSLoCommitdateInt(std::map<int,int> sLoCommitdateInt);
  static void setVBitmap(std::vector<std::vector<unsigned long long>> vBitMap);

  // static void setLoQuantity(long loQuantity);
  // static long getLoQuantity();

private:
  static inline long queryId_;
  static inline bool isDP_;
  static inline bool isSplit_;
  static inline bool isBitmap_;
public:
  static inline std::vector<std::set<int>> vKeyDate_;
  static inline std::map<std::string,int> sLoCommitdate_;
  static inline std::map<int,int> sLoCommitdateInt_;
  static inline std::vector<std::vector<unsigned long long>> vBitMap_;
  // static inline long loQuantity_;

};

}

#endif //NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_PLANNER_H
