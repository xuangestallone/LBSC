//
//  1/4/20.
//

#include <normal/plan/operator_/ScanLogicalOperator.h>
#include <normal/plan/operator_/type/OperatorTypes.h>
#include <normal/plan/Planner.h>
#include <normal/expression/gandiva/And.h>
#include <normal/expression/gandiva/LessThan.h>
#include <normal/expression/gandiva/Column.h>
#include <normal/expression/gandiva/StringLiteral.h>
#include <normal/expression/gandiva/NumericLiteral.h>
#include <normal/connector/MiniCatalogue.h>
#include <normal/expression/gandiva/LessThanOrEqualTo.h>
#include <normal/expression/gandiva/GreaterThan.h>
#include <normal/expression/gandiva/GreaterThanOrEqualTo.h>
#include <normal/expression/gandiva/EqualTo.h>
#include <normal/expression/gandiva/Or.h>

#include <bitset>
#include <iostream>

using namespace normal::plan::operator_;
using namespace normal::expression::gandiva;

ScanLogicalOperator::ScanLogicalOperator(
	std::shared_ptr<PartitioningScheme> partitioningScheme) :
	LogicalOperator(type::OperatorTypes::scanOperatorType()),
	partitioningScheme_(std::move(partitioningScheme)) {}

const std::shared_ptr<PartitioningScheme> &ScanLogicalOperator::getPartitioningScheme() const {
  return partitioningScheme_;
}

void ScanLogicalOperator::setPredicates(const std::shared_ptr<std::vector<std::shared_ptr<expression::gandiva::Expression>>> &predicates) {
  predicates_ = predicates;
}

void
ScanLogicalOperator::setProjectedColumnNames(const std::shared_ptr<std::vector<std::string>> &projectedColumnNames) {
  projectedColumnNames_ = projectedColumnNames;
}

const std::shared_ptr<std::vector<std::shared_ptr<normal::core::Operator>>> &
ScanLogicalOperator::streamOutPhysicalOperators() const {
  return streamOutPhysicalOperators_;
}

// FIXME: if numeric, compare using double in default
std::optional<double> getNumericValue(const std::shared_ptr<normal::expression::gandiva::Expression>& expr) {
  if (typeid(*expr) == typeid(normal::expression::gandiva::NumericLiteral<::arrow::Int32Type>)) {
    auto typedExpr = std::static_pointer_cast<normal::expression::gandiva::NumericLiteral<::arrow::Int32Type>>(expr);
    return (double) typedExpr->value();
  } else if (typeid(*expr) == typeid(normal::expression::gandiva::NumericLiteral<::arrow::Int64Type>)) {
    auto typedExpr = std::static_pointer_cast<normal::expression::gandiva::NumericLiteral<::arrow::Int64Type>>(expr);
    return (double) typedExpr->value();
  } else if (typeid(*expr) == typeid(normal::expression::gandiva::NumericLiteral<::arrow::FloatType>)) {
    auto typedExpr = std::static_pointer_cast<normal::expression::gandiva::NumericLiteral<::arrow::FloatType>>(expr);
    return (double) typedExpr->value();
  } else if (typeid(*expr) == typeid(normal::expression::gandiva::NumericLiteral<::arrow::DoubleType>)) {
    auto typedExpr = std::static_pointer_cast<normal::expression::gandiva::NumericLiteral<::arrow::DoubleType>>(expr);
    return (double) typedExpr->value();
  } else {
    return std::nullopt;
  }
}


bool checkPartitionForBitMapValid(const std::shared_ptr<Partition>& partition,
                         const std::shared_ptr<normal::expression::gandiva::Expression>& predicate){
  auto sortedColumns = normal::connector::defaultMiniCatalogue->sortedColumns();
  if (typeid(*predicate) == typeid(And)) {
    auto andExpr = std::static_pointer_cast<And>(predicate);
    return checkPartitionForBitMapValid(partition, andExpr->getLeft()) && checkPartitionForBitMapValid(partition, andExpr->getRight());
  }

  else if (typeid(*predicate) == typeid(Or)) { // a>10 or a<5  ?
    auto orExpr = std::static_pointer_cast<Or>(predicate);
    return checkPartitionForBitMapValid(partition, orExpr->getLeft()) || checkPartitionForBitMapValid(partition, orExpr->getRight());
  }

  else if (typeid(*predicate) == typeid(LessThan) ||  //<
           typeid(*predicate) == typeid(LessThanOrEqualTo) ||  //<=
           typeid(*predicate) == typeid(GreaterThan) || //>
           typeid(*predicate) == typeid(GreaterThanOrEqualTo) || //>=
           typeid(*predicate) == typeid(EqualTo)) {
    auto biExpr = std::static_pointer_cast<BinaryExpression>(predicate);

    std::shared_ptr<Expression> colExpr, valExpr;
    bool reverse;

    if (typeid(*biExpr->getLeft()) == typeid(Column)) { 
      colExpr = biExpr->getLeft();
      valExpr = biExpr->getRight();
      reverse = false;
    } else if (typeid(*biExpr->getRight()) == typeid(Column)) { 
      colExpr = biExpr->getRight();
      valExpr = biExpr->getLeft();
      reverse = true;
    } else {
      return true; 
    }
    auto columnName = std::static_pointer_cast<Column>(colExpr)->getColumnName();
    auto sortedColumnValuesIt = sortedColumns->find(columnName);
    if (!(sortedColumnValuesIt != sortedColumns->end() && columnName == "lo_commitdate")) {
      return true;
    }
    auto sortedColumnValues = sortedColumnValuesIt->second;
    auto dataValuePair = sortedColumnValues->find(partition)->second;
    int partitionIndex = std::stoi(dataValuePair.first);
    auto partitionBitMap = normal::plan::Planner::vBitMap_[partitionIndex];  

    if (typeid(*valExpr) == typeid(StringLiteral)) {
      std::cout<<"type cannot string"<<std::endl;
      std::exit(-1);
    } else {
      auto numericValue = getNumericValue(valExpr);
      if (!numericValue.has_value()) {
        return true;
      }
      auto predicateValue = numericValue.value();
      if (typeid(*predicate) == typeid(LessThan)) {
        //lo_commitdate < predicateValue
        if (!reverse) {
          std::vector<unsigned long long> vecBitMap;
          vecBitMap.resize(39);
          std::string strBitMap(2496,'0');
          auto iter = normal::plan::Planner::sLoCommitdateInt_.lower_bound(predicateValue);
          if(iter!=normal::plan::Planner::sLoCommitdateInt_.end()){
            int indexIter = iter->second -1; 
            for(int j=0;j<indexIter;j++){
              strBitMap[j]='1';
            }
            for(int j=0;j<39;j++){
              std::string tmp_str=strBitMap.substr(j*64,64);
              std::bitset<64> bitTmp(tmp_str);
              vecBitMap[j] = bitTmp.to_ullong();
            }
            for(int j=0;j<39;j++){
              if((vecBitMap[j] & partitionBitMap[j]) !=0){
                return true;
              }
            }
            return false;
  
          }else{
            return true;
          }
        } else {
          //lo_commitdate > predicateValue 
          std::vector<unsigned long long> vecBitMap;
          vecBitMap.resize(39);
          std::string strBitMap(2496,'1');
          auto iter = normal::plan::Planner::sLoCommitdateInt_.upper_bound(predicateValue);
          if(iter!=normal::plan::Planner::sLoCommitdateInt_.end()){
            int indexIter = iter->second; 
            for(int j=0;j<indexIter;j++){
              strBitMap[j]='0';
            }
            for(int j=0;j<39;j++){
              std::string tmp_str=strBitMap.substr(j*64,64);
              std::bitset<64> bitTmp(tmp_str);
              vecBitMap[j] = bitTmp.to_ullong();
            }
          
            for(int j=0;j<39;j++){
              if((vecBitMap[j] & partitionBitMap[j]) !=0){
                return true;
              }
            }
            return false;
  
          }else{
            return false;
          }

        }
      } else if (typeid(*predicate) == typeid(LessThanOrEqualTo)) {
        if (!reverse) {
          // <=
          std::vector<unsigned long long> vecBitMap;
          vecBitMap.resize(39);
          std::string strBitMap(2496,'0');
          auto iter = normal::plan::Planner::sLoCommitdateInt_.lower_bound(predicateValue);
          if(iter!=normal::plan::Planner::sLoCommitdateInt_.end()){
            int indexIter = iter->second -1;  
            if(normal::plan::Planner::sLoCommitdateInt_.find(predicateValue) != normal::plan::Planner::sLoCommitdateInt_.end()){
              indexIter+=1;
            }
            for(int j=0;j<indexIter;j++){
              strBitMap[j]='1';
            }
            for(int j=0;j<39;j++){
              std::string tmp_str=strBitMap.substr(j*64,64);
              std::bitset<64> bitTmp(tmp_str);
              vecBitMap[j] = bitTmp.to_ullong();
            }
            for(int j=0;j<39;j++){
              if((vecBitMap[j] & partitionBitMap[j]) !=0){
                return true;
              }
            }
            return false;
          }else{
            return true;
          }
        } else {
          //>=
          std::vector<unsigned long long> vecBitMap;
          vecBitMap.resize(39);
          std::string strBitMap(2496,'1');
          auto iter = normal::plan::Planner::sLoCommitdateInt_.upper_bound(predicateValue);
          if(iter!=normal::plan::Planner::sLoCommitdateInt_.end()){
            int indexIter = iter->second; 
            if(normal::plan::Planner::sLoCommitdateInt_.find(predicateValue) != normal::plan::Planner::sLoCommitdateInt_.end()){
              indexIter-=1;
            }
            for(int j=0;j<indexIter;j++){
              strBitMap[j]='0';
            }
            for(int j=0;j<39;j++){
              std::string tmp_str=strBitMap.substr(j*64,64);
              std::bitset<64> bitTmp(tmp_str);
              vecBitMap[j] = bitTmp.to_ullong();
            }
            for(int j=0;j<39;j++){
              if((vecBitMap[j] & partitionBitMap[j]) !=0){
                return true;
              }
            }
            return false;
          }else{
            return false;
          }
        }
      } else if (typeid(*predicate) == typeid(GreaterThan)) {
        if (!reverse) {
          //>
          std::vector<unsigned long long> vecBitMap;
          vecBitMap.resize(39);
          std::string strBitMap(2496,'1');
          auto iter = normal::plan::Planner::sLoCommitdateInt_.upper_bound(predicateValue);
          if(iter!=normal::plan::Planner::sLoCommitdateInt_.end()){
            int indexIter = iter->second; 
            for(int j=0;j<indexIter;j++){
              strBitMap[j]='0';
            }
            for(int j=0;j<39;j++){
              std::string tmp_str=strBitMap.substr(j*64,64);
              std::bitset<64> bitTmp(tmp_str);
              vecBitMap[j] = bitTmp.to_ullong();
            }
            for(int j=0;j<39;j++){
              if((vecBitMap[j] & partitionBitMap[j]) !=0){
                return true;
              }
            }
            return false;
          }else{
            return false;
          }
        } else {
          //<
          std::vector<unsigned long long> vecBitMap;
          vecBitMap.resize(39);
          std::string strBitMap(2496,'0');
          auto iter = normal::plan::Planner::sLoCommitdateInt_.lower_bound(predicateValue);
          if(iter!=normal::plan::Planner::sLoCommitdateInt_.end()){
            int indexIter = iter->second -1;  
            for(int j=0;j<indexIter;j++){
              strBitMap[j]='1';
            }
            for(int j=0;j<39;j++){
              std::string tmp_str=strBitMap.substr(j*64,64);
              std::bitset<64> bitTmp(tmp_str);
              vecBitMap[j] = bitTmp.to_ullong();
            }
            for(int j=0;j<39;j++){
              if((vecBitMap[j] & partitionBitMap[j]) !=0){
                return true;
              }
            }
            return false;
          }else{
            return true;
          }
        }
      } else if (typeid(*predicate) == typeid(GreaterThanOrEqualTo)) {
        if (!reverse) {
          //>=
          std::vector<unsigned long long> vecBitMap;
          vecBitMap.resize(39);
          std::string strBitMap(2496,'1');
          auto iter = normal::plan::Planner::sLoCommitdateInt_.upper_bound(predicateValue);
          if(iter!=normal::plan::Planner::sLoCommitdateInt_.end()){
            int indexIter = iter->second;  
            if(normal::plan::Planner::sLoCommitdateInt_.find(predicateValue) != normal::plan::Planner::sLoCommitdateInt_.end()){
              indexIter-=1;
            }
            for(int j=0;j<indexIter;j++){
              strBitMap[j]='0';
            }
            for(int j=0;j<39;j++){
              std::string tmp_str=strBitMap.substr(j*64,64);
              std::bitset<64> bitTmp(tmp_str);
              vecBitMap[j] = bitTmp.to_ullong();
            }
            for(int j=0;j<39;j++){
              if((vecBitMap[j] & partitionBitMap[j]) !=0){
                return true;
              }
            }
            return false;
          }else{
            return false;
          }
        } else {
          //<=
          std::vector<unsigned long long> vecBitMap;
          vecBitMap.resize(39);
          std::string strBitMap(2496,'0');
          auto iter = normal::plan::Planner::sLoCommitdateInt_.lower_bound(predicateValue);
          if(iter!=normal::plan::Planner::sLoCommitdateInt_.end()){
            int indexIter = iter->second -1;  
            if(normal::plan::Planner::sLoCommitdateInt_.find(predicateValue) != normal::plan::Planner::sLoCommitdateInt_.end()){
              indexIter+=1;
            }
            for(int j=0;j<indexIter;j++){
              strBitMap[j]='1';
            }
            for(int j=0;j<39;j++){
              std::string tmp_str=strBitMap.substr(j*64,64);
              std::bitset<64> bitTmp(tmp_str);
              vecBitMap[j] = bitTmp.to_ullong();
            }
            for(int j=0;j<39;j++){
              if((vecBitMap[j] & partitionBitMap[j]) !=0){
                return true;
              }
            }
            return false;
          }else{
            return true;
          }
        }
      } else {//==
        //std::cout<<"test2"<<std::endl;
        std::vector<unsigned long long> vecBitMap;
        vecBitMap.resize(39);
        auto iter = normal::plan::Planner::sLoCommitdateInt_.find(predicateValue);
        if(iter==normal::plan::Planner::sLoCommitdateInt_.end()){
          return false;
        }
        std::string strBitMap(2496,'0');
        int indexIter=iter->second;
        strBitMap[indexIter]='1';
        //std::cout<<"test indexIter="<<indexIter<<std::endl;
        for(int j=0;j<39;j++){
          std::string tmp_str=strBitMap.substr(j*64,64);
          std::bitset<64> bitTmp(tmp_str);
          vecBitMap[j] = bitTmp.to_ullong();
        }
        for(int j=0;j<39;j++){
          //std::cout<<"vector size:   "<<vecBitMap.size()<<"   "<<partitionBitMap.size()<<std::endl;
          if((vecBitMap[j] & partitionBitMap[j]) !=0){
            return true;
          }
        }
        return false;

      }
    }
  }
  else {
    throw std::runtime_error(fmt::format("Bad filter For DP predicate: {}", predicate->alias()));
  }

}

bool checkPartitionForDPValid(const std::shared_ptr<Partition>& partition,
                         const std::shared_ptr<normal::expression::gandiva::Expression>& predicate){
  auto sortedColumns = normal::connector::defaultMiniCatalogue->sortedColumns();
  if (typeid(*predicate) == typeid(And)) {
    auto andExpr = std::static_pointer_cast<And>(predicate);
    return checkPartitionForDPValid(partition, andExpr->getLeft()) && checkPartitionForDPValid(partition, andExpr->getRight());
  }

  else if (typeid(*predicate) == typeid(Or)) { // a>10 or a<5  ?
    auto orExpr = std::static_pointer_cast<Or>(predicate);
    return checkPartitionForDPValid(partition, orExpr->getLeft()) || checkPartitionForDPValid(partition, orExpr->getRight());
  }

  else if (typeid(*predicate) == typeid(LessThan) ||  //<
           typeid(*predicate) == typeid(LessThanOrEqualTo) ||  //<=
           typeid(*predicate) == typeid(GreaterThan) || //>
           typeid(*predicate) == typeid(GreaterThanOrEqualTo) || //>=
           typeid(*predicate) == typeid(EqualTo)) {
    auto biExpr = std::static_pointer_cast<BinaryExpression>(predicate);

    std::shared_ptr<Expression> colExpr, valExpr;
    bool reverse;

    if (typeid(*biExpr->getLeft()) == typeid(Column)) { 
      colExpr = biExpr->getLeft();
      valExpr = biExpr->getRight();
      reverse = false;
    } else if (typeid(*biExpr->getRight()) == typeid(Column)) { 
      colExpr = biExpr->getRight();
      valExpr = biExpr->getLeft();
      reverse = true;
    } else {
      return true; 
    }
    auto columnName = std::static_pointer_cast<Column>(colExpr)->getColumnName();
    auto sortedColumnValuesIt = sortedColumns->find(columnName);
    if (!(sortedColumnValuesIt != sortedColumns->end() && columnName == "lo_orderkey")) {
      return true; 
    }
    auto sortedColumnValues = sortedColumnValuesIt->second;
    auto dataValuePair = sortedColumnValues->find(partition)->second;
    int partitionIndex = std::stoi(dataValuePair.first);
    auto partitionScope = normal::plan::Planner::vKeyDate_[partitionIndex];

    if (typeid(*valExpr) == typeid(StringLiteral)) {
      std::cout<<"cannot be string type"<<std::endl;
      std::exit(-1);
    } else {
      auto numericValue = getNumericValue(valExpr);
      if (!numericValue.has_value()) {
        return false; 
      }
      auto predicateValue = numericValue.value();
      if (typeid(*predicate) == typeid(LessThan)) {
        if (!reverse) {
          auto iter=partitionScope.lower_bound(predicateValue);
          if(iter == partitionScope.begin()){
            return false;
          }else{
            return true;
          }
        } else {
          return partitionScope.upper_bound(predicateValue) != partitionScope.end();
        }
      } else if (typeid(*predicate) == typeid(LessThanOrEqualTo)) { //<=
        if (!reverse) {
          auto iter=partitionScope.lower_bound(predicateValue);
          if(iter == partitionScope.begin() && partitionScope.find(predicateValue) == partitionScope.end()){
            return false;
          }else{
            return true;
          }
        } else {
          return (partitionScope.upper_bound(predicateValue) != partitionScope.end()) || (partitionScope.find(predicateValue) != partitionScope.end());
        }
      } else if (typeid(*predicate) == typeid(GreaterThan)) {
        if (!reverse) {
          return partitionScope.upper_bound(predicateValue) != partitionScope.end();
        } else {
          auto iter=partitionScope.lower_bound(predicateValue);
          if(iter == partitionScope.begin()){
            return false;
          }else{
            return true;
          }
        }
      } else if (typeid(*predicate) == typeid(GreaterThanOrEqualTo)) {
        if (!reverse) {
          return (partitionScope.upper_bound(predicateValue) != partitionScope.end()) || (partitionScope.find(predicateValue) != partitionScope.end());
        } else {
          auto iter=partitionScope.lower_bound(predicateValue);
          if(iter == partitionScope.begin() && partitionScope.find(predicateValue) == partitionScope.end()){
            return false;
          }else{
            return true;
          }
        }
      } else {//==
        return partitionScope.find(predicateValue) != partitionScope.end();
      }

    }


  }
  else {
    throw std::runtime_error(fmt::format("Bad filter For DP predicate: {}", predicate->alias()));
  }
}

bool checkPartitionValid(const std::shared_ptr<Partition>& partition,
                         const std::shared_ptr<normal::expression::gandiva::Expression>& predicate) {
  auto sortedColumns = normal::connector::defaultMiniCatalogue->sortedColumns();

  if (typeid(*predicate) == typeid(And)) {
    auto andExpr = std::static_pointer_cast<And>(predicate);
    return checkPartitionValid(partition, andExpr->getLeft()) && checkPartitionValid(partition, andExpr->getRight());
  }

  else if (typeid(*predicate) == typeid(Or)) { // a>10 or a<5  ?
    auto orExpr = std::static_pointer_cast<Or>(predicate);
    return checkPartitionValid(partition, orExpr->getLeft()) || checkPartitionValid(partition, orExpr->getRight());
  }

  else if (typeid(*predicate) == typeid(LessThan) ||  //<
           typeid(*predicate) == typeid(LessThanOrEqualTo) ||  //<=
           typeid(*predicate) == typeid(GreaterThan) || //>
           typeid(*predicate) == typeid(GreaterThanOrEqualTo) || //>=
           typeid(*predicate) == typeid(EqualTo)) {  //==
    auto biExpr = std::static_pointer_cast<BinaryExpression>(predicate);

    std::shared_ptr<Expression> colExpr, valExpr;
    bool reverse;

    if (typeid(*biExpr->getLeft()) == typeid(Column)) {
      colExpr = biExpr->getLeft();
      valExpr = biExpr->getRight();
      reverse = false;
    } else if (typeid(*biExpr->getRight()) == typeid(Column)) { 
      colExpr = biExpr->getRight();
      valExpr = biExpr->getLeft();
      reverse = true;
    } else {
      return true; 
    }

    auto columnName = std::static_pointer_cast<Column>(colExpr)->getColumnName();  
    auto sortedColumnValuesIt = sortedColumns->find(columnName);
    if (!(sortedColumnValuesIt != sortedColumns->end() && columnName == "lo_orderdate")) {
      return true;
    }
    auto sortedColumnValues = sortedColumnValuesIt->second;
    auto dataValuePair = sortedColumnValues->find(partition)->second; 
    if (typeid(*valExpr) == typeid(StringLiteral)) {
      auto predicateValue = std::static_pointer_cast<StringLiteral>(valExpr)->value();
      if (typeid(*predicate) == typeid(LessThan)) {
        if (!reverse) {
          return dataValuePair.first.compare(predicateValue) < 0;
        } else {
          return dataValuePair.second.compare(predicateValue) > 0;
        }
      } else if (typeid(*predicate) == typeid(LessThanOrEqualTo)) {
        if (!reverse) {
          return dataValuePair.first.compare(predicateValue) <= 0;
        } else {
          return dataValuePair.second.compare(predicateValue) >= 0;
        }
      } else if (typeid(*predicate) == typeid(GreaterThan)) {
        if (!reverse) {
          return dataValuePair.second.compare(predicateValue) > 0;
        } else {
          return dataValuePair.first.compare(predicateValue) < 0;
        }
      } else if (typeid(*predicate) == typeid(GreaterThanOrEqualTo)) {
        if (!reverse) {
          return dataValuePair.second.compare(predicateValue) >= 0;
        } else {
          return dataValuePair.first.compare(predicateValue) <= 0;
        }
      } else {
        //==    minValue <=  predicateValue <= maxValue
        return dataValuePair.first.compare(predicateValue) <= 0 && dataValuePair.second.compare(predicateValue) >= 0;
      }
    } else {
      auto numericValue = getNumericValue(valExpr);
      if (!numericValue.has_value()) {
        return true;
      }
      auto predicateValue = numericValue.value();
      if (typeid(*predicate) == typeid(LessThan)) {
        if (!reverse) {
          return std::stod(dataValuePair.first) < predicateValue;
        } else {
          return std::stod(dataValuePair.second) > predicateValue;
        }
      } else if (typeid(*predicate) == typeid(LessThanOrEqualTo)) {
        if (!reverse) {
          return std::stod(dataValuePair.first) <= predicateValue;
        } else {
          return std::stod(dataValuePair.second) >= predicateValue;
        }
      } else if (typeid(*predicate) == typeid(GreaterThan)) {
        if (!reverse) {
          return std::stod(dataValuePair.second) > predicateValue;
        } else {
          return std::stod(dataValuePair.first) < predicateValue;
        }
      } else if (typeid(*predicate) == typeid(GreaterThanOrEqualTo)) {
        if (!reverse) {
          return std::stod(dataValuePair.second) >= predicateValue;
        } else {
          return std::stod(dataValuePair.first) <= predicateValue;
        }
      } else {
        return std::stod(dataValuePair.first) <= predicateValue && std::stod(dataValuePair.second) >= predicateValue;
      }
    }
  }

  else {
    throw std::runtime_error(fmt::format("Bad filter predicate: {}", predicate->alias()));
  }
}

bool checkPredicateNeeded(const std::shared_ptr<Partition>& partition,
                          const std::shared_ptr<Expression>& predicate) {
  auto sortedColumns = normal::connector::defaultMiniCatalogue->sortedColumns();

  if (typeid(*predicate) == typeid(And)) {
    auto andExpr = std::static_pointer_cast<And>(predicate);
    return checkPredicateNeeded(partition, andExpr->getLeft()) || checkPredicateNeeded(partition, andExpr->getRight());
  }

  else if (typeid(*predicate) == typeid(Or)) {
    auto orExpr = std::static_pointer_cast<Or>(predicate);
    return checkPredicateNeeded(partition, orExpr->getLeft()) && checkPredicateNeeded(partition, orExpr->getRight());
  }

  else if (typeid(*predicate) == typeid(LessThan) ||
           typeid(*predicate) == typeid(LessThanOrEqualTo) ||
           typeid(*predicate) == typeid(GreaterThan) ||
           typeid(*predicate) == typeid(GreaterThanOrEqualTo) ||
           typeid(*predicate) == typeid(EqualTo)) {
    auto biExpr = std::static_pointer_cast<BinaryExpression>(predicate);

    std::shared_ptr<Expression> colExpr, valExpr;
    bool reverse;

    if (typeid(*biExpr->getLeft()) == typeid(Column)) {
      colExpr = biExpr->getLeft();
      valExpr = biExpr->getRight();
      reverse = false;
    } else if (typeid(*biExpr->getRight()) == typeid(Column)) {
      colExpr = biExpr->getRight();
      valExpr = biExpr->getLeft();
      reverse = true;
    } else {
      return true;
    }

    auto columnName = std::static_pointer_cast<Column>(colExpr)->getColumnName();
    auto sortedColumnValuesIt = sortedColumns->find(columnName);
    if (!(sortedColumnValuesIt != sortedColumns->end() && columnName == "lo_orderdate")) {
      return true;
    }
    auto sortedColumnValues = sortedColumnValuesIt->second;
    auto dataValuePair = sortedColumnValues->find(partition)->second;

    if (typeid(*valExpr) == typeid(StringLiteral)) {
      auto predicateValue = std::static_pointer_cast<StringLiteral>(valExpr)->value();
      if (typeid(*predicate) == typeid(LessThan)) {
        if (!reverse) {
          return dataValuePair.second.compare(predicateValue) >= 0;
        } else {
          return dataValuePair.first.compare(predicateValue) <= 0;
        }
      } else if (typeid(*predicate) == typeid(LessThanOrEqualTo)) {
        if (!reverse) {
          return dataValuePair.second.compare(predicateValue) > 0;
        } else {
          return dataValuePair.first.compare(predicateValue) < 0;
        }
      } else if (typeid(*predicate) == typeid(GreaterThan)) {
        if (!reverse) {
          return dataValuePair.first.compare(predicateValue) <= 0;
        } else {
          return dataValuePair.second.compare(predicateValue) >= 0;
        }
      } else if (typeid(*predicate) == typeid(GreaterThanOrEqualTo)) {
        if (!reverse) {
          return dataValuePair.first.compare(predicateValue) < 0;
        } else {
          return dataValuePair.second.compare(predicateValue) > 0;
        }
      } else {
        return dataValuePair.first == predicateValue && dataValuePair.second == predicateValue;
      }
    } else {
      auto numericValue = getNumericValue(valExpr);
      if (!numericValue.has_value()) {
        return true;
      }
      auto predicateValue = numericValue.value();
      if (typeid(*predicate) == typeid(LessThan)) {
        if (!reverse) {
          return std::stod(dataValuePair.second) >= predicateValue;
        } else {
          return std::stod(dataValuePair.first) <= predicateValue;
        }
      } else if (typeid(*predicate) == typeid(LessThanOrEqualTo)) {
        if (!reverse) {
          return std::stod(dataValuePair.second) > predicateValue;
        } else {
          return std::stod(dataValuePair.first) < predicateValue;
        }
      } else if (typeid(*predicate) == typeid(GreaterThan)) {
        if (!reverse) {
          return std::stod(dataValuePair.first) <= predicateValue;
        } else {
          return std::stod(dataValuePair.second) >= predicateValue;
        }
      } else if (typeid(*predicate) == typeid(GreaterThanOrEqualTo)) {
        if (!reverse) {
          return std::stod(dataValuePair.first) < predicateValue;
        } else {
          return std::stod(dataValuePair.second) > predicateValue;
        }
      } else {
        return std::stod(dataValuePair.first) == predicateValue && std::stod(dataValuePair.second) == predicateValue;
      }
    }
  }

  else {
    throw std::runtime_error(fmt::format("Bad filter predicate: {}", predicate->alias()));
  }
}


void ScanLogicalOperator::checkPartitionValidForBitMap(
      std::shared_ptr<std::vector<std::shared_ptr<Partition>>> partitions,
      std::vector<bool>& finalPartition){
    int partitionIndex=0;
    for(const auto &partition: *partitions){
      if(finalPartition[partitionIndex]){
        partitionIndex++;
        continue;
      }
      bool valid = true;
      for (auto const &predicate: *predicates_) {
        if(!::checkPartitionForBitMapValid(partition, predicate)){
          valid=false;
          break;
        }
      }
      //std::cout<<"partition:"<<partitionIndex<<"  checkValid:"<<valid<<std::endl;
      if(!valid){
        finalPartition[partitionIndex]=true;
      }
      partitionIndex++;
    }
    assert(partitionIndex == partitions->size());
}

void ScanLogicalOperator::checkPartitionValidForDP(
  std::shared_ptr<std::vector<std::shared_ptr<Partition>>> partitions,
  std::vector<bool>& finalPartition){
  
  int partitionIndex=0;
  for(const auto &partition: *partitions){
    bool valid = true;
    for (auto const &predicate: *predicates_) {
      if(!::checkPartitionForDPValid(partition, predicate)){
        valid=false;
        break;
      }
    }
    if(!valid){
      finalPartition[partitionIndex]=true;
    }
    partitionIndex++;
  }
  assert(partitionIndex == partitions->size());
}

std::pair<bool, std::shared_ptr<normal::expression::gandiva::Expression>>
ScanLogicalOperator::checkPartitionValid(const std::shared_ptr<Partition>& partition) {

  std::shared_ptr<normal::expression::gandiva::Expression> finalPredicate = nullptr;
  bool valid = true;
  for (auto const &predicate: *predicates_) {//filter 
    if (!::checkPartitionValid(partition, predicate)) {
      valid = false;
      break;
    }
    if (!::checkPredicateNeeded(partition, predicate)) {
      continue;
    } else {
      if (finalPredicate == nullptr) {
        finalPredicate = predicate;
      } else {
        finalPredicate = and_(finalPredicate, predicate);
      }
    }
  }
  return std::pair<bool, std::shared_ptr<normal::expression::gandiva::Expression>>(valid, finalPredicate);
}
