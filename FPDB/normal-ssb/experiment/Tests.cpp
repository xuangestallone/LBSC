//
//  on 7/7/20.
//

#include <doctest/doctest.h>
#include <normal/sql/Interpreter.h>
#include <normal/pushdown/collate/Collate.h>
#include <normal/connector/s3/S3SelectConnector.h>
#include <normal/plan/mode/Modes.h>
#include <normal/cache/LRUCachingPolicy.h>
#include <normal/cache/FBRCachingPolicy.h>
#include <normal/cache/FBRSCachingPolicy.h>
#include <normal/cache/WFBRCachingPolicy.h>
#include <normal/cache/BeladyCachingPolicy.h>
#include <normal/cache/BeladySizeCostCachingPolicy.h>
#include <normal/cache/LearningCachingPolicy.h>
#include "ExperimentUtil.h"
#include "Tests.h"
#include <normal/ssb/SqlGenerator.h>
#include <normal/plan/Globals.h>
#include <normal/plan/Planner.h>
#include <normal/cache/Globals.h>
#include <normal/connector/MiniCatalogue.h>
#include <normal/ssb/SSBSchema.h>
#include <normal/pushdown/Globals.h>

#include <aws/s3/model/GetObjectRequest.h>                  // for GetObj...
#include <aws/s3/S3Client.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/s3/model/SelectObjectContentRequest.h>
#include <aws/s3/model/CSVInput.h>                          // for CSVInput
#include <aws/s3/model/CSVOutput.h>                         // for CSVOutput
#include <aws/s3/model/ExpressionType.h>                    // for Expressio...
#include <aws/s3/model/FileHeaderInfo.h>                    // for FileHeade...
#include <aws/s3/model/InputSerialization.h>                // for InputSeri...
#include <aws/s3/model/OutputSerialization.h>               // for OutputSer...
#include <aws/s3/model/RecordsEvent.h>                      // for RecordsEvent
#include <aws/s3/model/SelectObjectContentHandler.h>        // for SelectObj...
#include <aws/s3/model/StatsEvent.h>                        // for StatsEvent
#include <aws/s3/model/ListObjectsRequest.h>


#include <iostream>
#include <vector>
#include <fstream>
#include <map>
#include <set>
#include <bitset>


#include <arrow/csv/reader.h>                               // for TableReader
#include <arrow/type_fwd.h>                                 // for default_m...
#include <arrow/io/buffered.h> 


#ifdef __AVX2__
#include <normal/tuple/arrow/CSVToArrowSIMDStreamParser.h>
#include <normal/tuple/arrow/CSVToArrowSIMDChunkParser.h>
#endif

#define SKIP_SUITE false

using namespace normal::ssb;
using namespace normal::pushdown;
using namespace normal::pushdown::collate;
using namespace normal::pushdown::s3;


std::vector<std::string> split_str(const std::string& str, const std::string& splitStr) {
  std::vector<std::string> res;
  std::string::size_type pos1, pos2;
  pos2 = str.find(splitStr);
  pos1 = 0;
  while (pos2 != std::string::npos)
  {
    res.push_back(str.substr(pos1, pos2 - pos1));
    pos1 = pos2 + 1;
    pos2 = str.find(splitStr, pos1);
  }
  if (pos1 < str.length()) {
    res.push_back(str.substr(pos1));
  }
  return res;
}



void calSize(std::string tableName, std::shared_ptr<Aws::S3::S3Client> s3Client_, std::string& s3Object_, 
  std::ofstream& seg_file, std::vector<std::string>& vec_part, long long fileSize){

  std::string s3Bucket_ = "pushdowndb";
  Aws::String bucketName = Aws::String(s3Bucket_);

  /*
  generate parser
  */
  std::vector<std::shared_ptr<::arrow::Field>> fields;
  std::shared_ptr<arrow::Schema> schema_ = std::move(normal::connector::defaultMiniCatalogue->getSchema(tableName));
  std::vector<std::string> tmp_vec_part = vec_part;
  for (auto const &columnName: vec_part) {
    fields.emplace_back(schema_->GetFieldByName(columnName));
  }
  auto parser = std::make_shared<S3CSVParser>(tmp_vec_part, std::make_shared<::arrow::Schema>(fields),',');

  Aws::S3::Model::SelectObjectContentRequest selectObjectContentRequest;
  selectObjectContentRequest.SetBucket(bucketName);
  selectObjectContentRequest.SetKey(Aws::String(s3Object_));

  selectObjectContentRequest.SetExpressionType(Aws::S3::Model::ExpressionType::SQL);

  std::string sql = "select * from s3Object";
  selectObjectContentRequest.SetExpression(sql.c_str());
  Aws::S3::Model::InputSerialization inputSerialization;
  Aws::S3::Model::CSVInput csvInput;
  csvInput.SetFileHeaderInfo(Aws::S3::Model::FileHeaderInfo::USE);
  std::string delimiter = ",";
  csvInput.SetFieldDelimiter(Aws::String(delimiter));
  csvInput.SetRecordDelimiter("\n");
  inputSerialization.SetCSV(csvInput);

  selectObjectContentRequest.SetInputSerialization(inputSerialization);

  Aws::S3::Model::CSVOutput csvOutput;
  Aws::S3::Model::OutputSerialization outputSerialization;
  outputSerialization.SetCSV(csvOutput);
  selectObjectContentRequest.SetOutputSerialization(outputSerialization);

  Aws::S3::Model::SelectObjectContentHandler handler;
  Aws::Vector<unsigned char> s3Result_{};
  handler.SetRecordsEventCallback([&](const Aws::S3::Model::RecordsEvent &recordsEvent) {
    auto payload = recordsEvent.GetPayload();
    if (!payload.empty()) {
      s3Result_.insert(s3Result_.end(), payload.begin(), payload.end());
    }
  });

  selectObjectContentRequest.SetEventStreamHandler(handler);
  auto selectObjectContentOutcome = s3Client_->SelectObjectContent(selectObjectContentRequest);
  if (selectObjectContentOutcome.IsSuccess()) {

   std::shared_ptr<TupleSet2> tupleSet;
   if (s3Result_.size() > 0) {std::shared_ptr<TupleSet> tupleSetV1 = parser->parseCompletePayload(s3Result_.begin(), s3Result_.end());
    auto tupleSet = TupleSet2::create(tupleSetV1);
    for (int64_t c = 0; c < tupleSet->numColumns(); ++c) {
	    auto column = tupleSet->getColumnByIndex(c).value();
      int cSize = column->size();
      std::string cName = column->getName();
      seg_file<<s3Object_<<","<<cName<<",0,"<<fileSize<<","<<cSize<<std::endl;
  }

    } else {
      tupleSet = TupleSet2::make2();
      std::cout<<"tupleSet is empty  file:"<<s3Object_<<std::endl;
    }

  }else{
    std::cout<<"error   file:"<<s3Object_<<std::endl;
  }



}




void normal::ssb::calSegmetnInfo(const std::string& dirPrefix){
  std::string bucket_name = "pushdowndb";
  auto s3Client = AWSClient::defaultS3Client();

  std::string str_supplier = "s_suppkey,s_name,s_address,s_city,s_nation,s_region,s_phone";
  std::string str_date = "d_datekey,d_date,d_dayofweek,d_month,d_year,d_yearmonthnum,d_yearmonth,d_daynuminweek,d_daynuminmonth,d_daynuminyear,d_monthnuminyear,d_weeknuminyear,d_sellingseason,d_lastdayinweekfl,d_lastdayinmonthfl,d_holidayfl,d_weekdayfl";
  std::string str_part = "p_partkey,p_name,p_mfgr,p_category,p_brand1,p_color,p_type,p_size,p_container";
  std::string str_customer = "c_custkey,c_name,c_address,c_city,c_nation,c_region,c_phone,c_mktsegment";
  std::string str_lineorder = "lo_orderkey,lo_linenumber,lo_custkey,lo_partkey,lo_suppkey,lo_orderdate,lo_orderpriority,lo_shippriority,lo_quantity,lo_extendedprice,lo_ordtotalprice,lo_discount,lo_revenue,lo_supplycost,lo_tax,lo_commitdate,lo_shipmode";

  std::vector<std::string> vec_supplier = split_str(str_supplier, ",");
  std::vector<std::string> vec_date = split_str(str_date, ",");
  std::vector<std::string> vec_part = split_str(str_part, ",");
  std::vector<std::string> vec_customer = split_str(str_customer, ",");
  std::vector<std::string> vec_lineorder = split_str(str_lineorder, ",");
  
  Aws::S3::Model::ListObjectsRequest listObjectsRequest;
  listObjectsRequest.WithBucket(bucket_name.c_str());
  listObjectsRequest.WithPrefix(dirPrefix.c_str());
  std::cout<<"bucket:"<<bucket_name<<"   prefix:"<<dirPrefix<<std::endl;

  auto res = s3Client->ListObjects(listObjectsRequest);
  std::string segInfo = "segment_info";
  std::ofstream  seg_file(segInfo, std::ios::app|std::ios::out);
  if(!seg_file){
    std::cout << "Exception opening/reading file " << segInfo << std::endl;
    exit(-1);
  }

  if (res.IsSuccess()) {
    Aws::Vector<Aws::S3::Model::Object> objectList = res.GetResult().GetContents();
    for (auto const &object: objectList) {
      std::string fileName = static_cast<std::string>(object.GetKey());
      std::cout<<"fileName:"<<fileName<<std::endl;
      long long fileSize = object.GetSize();
      if(fileName.find("part") != std::string::npos){
        calSize("part",s3Client, fileName, seg_file, vec_part, fileSize);

      }else if(fileName.find("customer") != std::string::npos){
        calSize("customer", s3Client, fileName, seg_file, vec_customer, fileSize);

      }else if(fileName.find("date") != std::string::npos){
        calSize("date", s3Client, fileName, seg_file, vec_date, fileSize);

      }else if(fileName.find("supplier") != std::string::npos){
        calSize("supplier", s3Client, fileName, seg_file, vec_supplier, fileSize);

      }else if(fileName.find("lineorder") != std::string::npos){
        calSize("lineorder", s3Client, fileName, seg_file, vec_lineorder, fileSize);

      }else{
        std::cout<<"fileName can not match:"<<fileName<<std::endl;
      }
    }
    seg_file.close();
    
  }else {
    std::cout<<"s3 ListObjects error"<<std::endl;
    throw std::runtime_error(res.GetError().GetMessage().c_str());
  }

}

std::vector<std::string> normal::ssb::split(const std::string& str, const std::string& splitStr) {
  std::vector<std::string> res;
  std::string::size_type pos1, pos2;
  pos2 = str.find(splitStr);
  pos1 = 0;
  while (pos2 != std::string::npos)
  {
    res.push_back(str.substr(pos1, pos2 - pos1));
    pos1 = pos2 + 1;
    pos2 = str.find(splitStr, pos1);
  }
  if (pos1 < str.length()) {
    res.push_back(str.substr(pos1));
  }
  return res;
}




void normal::ssb::mainTest(size_t cacheSize, int modeType, int cachingPolicyType, const std::string& dirPrefix,
                           size_t networkLimit, bool writeResults, int executeBatchSize_, bool showQueryMetrics, int wMethod,
                           bool isSplit, bool isBitMap, bool isDP, int featureLabel, bool multiModel) {
  spdlog::set_level(spdlog::level::warn);
  //spdlog::set_level(spdlog::level::debug);
  // parameters
  const int warmBatchSize = 50;
  const int executeBatchSize = executeBatchSize_;
  std::string bucket_name = "pushdowndb";
  normal::connector::defaultMiniCatalogue = normal::connector::MiniCatalogue::defaultMiniCatalogue(bucket_name, dirPrefix);
  normal::cache::beladyMiniCatalogue = normal::connector::MiniCatalogue::defaultMiniCatalogue(bucket_name, dirPrefix);
  normal::cache::beladySCMiniCatalogue = normal::connector::MiniCatalogue::defaultMiniCatalogue(bucket_name, dirPrefix);
  normal::cache::beladyLearningMiniCatalogue = normal::connector::MiniCatalogue::defaultMiniCatalogue(bucket_name, dirPrefix);
  normal::cache::keyProb_;
  normal::cache::bldscLessValue1Gloal = 0;
  normal::cache::bldscLessValue2Gloal = 0;
  normal::cache::bldscLessValue3Gloal = 0;
  normal::cache::bldscLessValue4Gloal = 0;
  normal::cache::max_n_past_timestamps = 32;
  normal::cache::max_n_past_distances = 31;
  normal::cache::memory_window = 50;  
  if(featureLabel == 1 || featureLabel ==2){
    normal::cache::n_extra_fields = 0;
  }else if(featureLabel == 3 || featureLabel == 4){
    normal::cache::n_extra_fields = 3;
  } else{
    std::cout<<"featureLabel type error"<<std::endl;
    std::exit(-1);
  }

  normal::cache::batch_size = 30000;
  normal::cache::n_feature;

  if (networkLimit > 0) {
    NetworkLimit = networkLimit;
  }

  normal::plan::Planner::setDP(isDP);
  normal::plan::Planner::setBitmap(isBitMap);
  normal::plan::Planner::setSplit(isSplit);
  if(isDP){
    std::vector<std::set<int>> vKeyDate;
    vKeyDate.resize(400);
    std::ifstream inFile("key_date");
    std::string one_record;
    while(getline(inFile, one_record)){
      std::vector<std::string> split_str = normal::ssb::split(one_record, ",");
      std::string orderKey = split_str[0];
      int i_orderkey = std::stoi(orderKey);
      std::string partition_str = split_str[1];
      int i_partition = std::stoi(partition_str);
      vKeyDate[i_partition].insert(i_orderkey);
    }
    inFile.close();
    normal::plan::Planner::setVKeyDate(vKeyDate);
  }

  if(isBitMap){

    std::map<std::string,int> sLoCommitdate;
    std::map<int,int> sLoCommitdateInt;
    std::ifstream inFile("columnScope/lo_commitdate.txt");
    std::string one_record;
    int index=0;
    while(getline(inFile, one_record)){
      sLoCommitdate.insert({one_record,index});
      sLoCommitdateInt.insert({std::stoi(one_record),index});
      index++;
    }
    inFile.close();
    normal::plan::Planner::setSLoCommitdate(sLoCommitdate);
    normal::plan::Planner::setSLoCommitdateInt(sLoCommitdateInt);

    std::vector<std::vector<unsigned long long>> vBitMap;
    vBitMap.resize(400);
    std::ifstream inFileBitMap("lo_commitdate_bitmap.txt");
    std::string one_record_BitMap;
    std::vector<unsigned long long> vTmpBitMap;
    vTmpBitMap.resize(39);
    int indexBitMap=0;
    while(getline(inFileBitMap, one_record_BitMap)){
      vTmpBitMap.clear();
      vTmpBitMap.resize(39);
      //vBitMap[indexBitMap].resize(32);
      for(int j=0;j<39;j++){
        std::string tmp_str=one_record_BitMap.substr(j*64,64);
        std::bitset<64> bitTmp(tmp_str);
        vTmpBitMap[j] = bitTmp.to_ullong();
      }
      vBitMap[indexBitMap]=vTmpBitMap;
      indexBitMap++;
    }
    inFileBitMap.close();
    normal::plan::Planner::setVBitmap(vBitMap);
  }

  normal::plan::DefaultS3Client = AWSClient::defaultS3Client();

  std::shared_ptr<normal::plan::operator_::mode::Mode> mode;
  std::string modeAlias;
  switch (modeType) {
    case 1: mode = normal::plan::operator_::mode::Modes::fullPullupMode(); modeAlias = "fpu"; break;
    case 2: mode = normal::plan::operator_::mode::Modes::fullPushdownMode(); modeAlias = "fpd"; break;
    case 3: mode = normal::plan::operator_::mode::Modes::pullupCachingMode(); modeAlias = "pc"; break;
    case 4: mode = normal::plan::operator_::mode::Modes::hybridCachingMode(); modeAlias = "hc"; break;
    default: throw std::runtime_error("Mode not found, type: " + std::to_string(modeType));
  }

  std::shared_ptr<normal::cache::CachingPolicy> cachingPolicy;
  std::string cachingPolicyAlias;
  switch (cachingPolicyType) {
    case 1: cachingPolicy = LRUCachingPolicy::make(cacheSize, mode); cachingPolicyAlias = "lru"; break;
    case 2: cachingPolicy = FBRSCachingPolicy::make(cacheSize, mode); cachingPolicyAlias = "lfu"; break;
    case 3: cachingPolicy = WFBRCachingPolicy::make(cacheSize, mode); cachingPolicyAlias = "wlfu"; break;
    case 4: cachingPolicy = BeladyCachingPolicy::make(cacheSize, mode); cachingPolicyAlias = "bldy"; break;
    case 5: cachingPolicy = BeladySizeCostCachingPolicy::make(cacheSize, mode); cachingPolicyAlias = "blsc"; break;
    case 6: cachingPolicy = LearningCachingPolicy::make(cacheSize, mode); cachingPolicyAlias = "learn"; break;
    case 7: cachingPolicy = FBRCachingPolicy::make(cacheSize, mode); cachingPolicyAlias = "olfu"; break;
    default: throw std::runtime_error("CachingPolicy not found, type: " + std::to_string(cachingPolicyType));
  }

  auto currentPath = std::filesystem::current_path();
  auto sql_file_dir_path = currentPath.append("sql/generated");

  if (cachingPolicy->id() == BELADY) { 
    auto beladyCachingPolicy = std::static_pointer_cast<normal::cache::BeladyCachingPolicy>(cachingPolicy);
    generateBeladySegmentKeyAndSqlQueryMappings(mode, beladyCachingPolicy, bucket_name, dirPrefix, warmBatchSize + executeBatchSize, sql_file_dir_path);
    beladyCachingPolicy->generateCacheDecisions(warmBatchSize + executeBatchSize);

  } else if(cachingPolicy->id() == BELADYSC){
    auto beladySizeCostCachingPolicy = std::static_pointer_cast<normal::cache::BeladySizeCostCachingPolicy>(cachingPolicy);
    generateBeladySCSegmentKeyAndSqlQueryMappings(mode, beladySizeCostCachingPolicy, bucket_name, dirPrefix, warmBatchSize + executeBatchSize, sql_file_dir_path);
    beladySizeCostCachingPolicy->setWeightMethod(wMethod);
    beladySizeCostCachingPolicy->generateCacheDecisions(warmBatchSize + executeBatchSize);
  } 
  
  else if(cachingPolicy->id() == LEARN){
    auto learningCachingPolicy = std::static_pointer_cast<normal::cache::LearningCachingPolicy>(cachingPolicy);
    generateLearningSegmentKeyAndSqlQueryMappings(mode, learningCachingPolicy, bucket_name, dirPrefix, warmBatchSize + executeBatchSize, sql_file_dir_path);
    learningCachingPolicy->setWeightMethod(wMethod);
    learningCachingPolicy->generateCacheDecisions(warmBatchSize + executeBatchSize);

    learningCachingPolicy->setMultiModel(multiModel);
    if(featureLabel == 1){
      learningCachingPolicy->setExtraFeature(false);
      learningCachingPolicy->setLabelLog(false);
    }else if(featureLabel == 2){
      learningCachingPolicy->setExtraFeature(false);
      learningCachingPolicy->setLabelLog(true);
    }else if(featureLabel == 3){
      learningCachingPolicy->setExtraFeature(true);
      learningCachingPolicy->setLabelLog(false);
    }else if(featureLabel == 4){ 
      learningCachingPolicy->setExtraFeature(true);
      learningCachingPolicy->setLabelLog(true);
    } else{
      std::cout<<"featureLabel type error"<<std::endl;
      std::exit(-1);
    }
    
  }

  // interpreter
  normal::sql::Interpreter i(mode, cachingPolicy, showQueryMetrics);
  configureS3ConnectorMultiPartition(i, bucket_name, dirPrefix);

  // execute
  // FIXME: has to make a new one other wise with Airmettle sometimes a req has no end event, unsure why
  normal::plan::DefaultS3Client = AWSClient::defaultS3Client();
  i.boot();
  SPDLOG_CRITICAL("{} mode start", mode->toString());
  if (mode->id() != normal::plan::operator_::mode::ModeId::FullPullup &&
      mode->id() != normal::plan::operator_::mode::ModeId::FullPushdown) {
    SPDLOG_CRITICAL("Cache warm phase:");
    for (auto index = 1; index <= warmBatchSize; ++index) {
      SPDLOG_CRITICAL("sql {}", index);
      if (cachingPolicy->id() == BELADY) {
        normal::cache::beladyMiniCatalogue->setCurrentQueryNum(index);
      } else if(cachingPolicy->id() == BELADYSC){
        normal::cache::beladySCMiniCatalogue->setCurrentQueryNum(index);
      } else if(cachingPolicy->id() == LEARN){
        normal::cache::beladyLearningMiniCatalogue->setCurrentQueryNum(index);
      }
      auto sql_file_path = sql_file_dir_path.append(fmt::format("{}.sql", index));
      auto sql = read_file(sql_file_path.string());
      executeSql(i, sql, true, writeResults, fmt::format("{}output.txt", index));
      if(cachingPolicy->id() == BELADYSC){
        auto beladySizeCostCachingPolicy = std::static_pointer_cast<normal::cache::BeladySizeCostCachingPolicy>(cachingPolicy);
        beladySizeCostCachingPolicy->updateWeight(index+1);
      } else if(cachingPolicy->id() == LEARN){
        auto learningCachingPolicy = std::static_pointer_cast<normal::cache::LearningCachingPolicy>(cachingPolicy);
        learningCachingPolicy->updateWeight(index+1);
      }
      sql_file_dir_path = sql_file_dir_path.parent_path();
    }
    SPDLOG_CRITICAL("Cache warm phase finished");
  } else {
    // execute one query to avoid first-run latency
    SPDLOG_CRITICAL("First-run query:");
    auto sql_file_path = sql_file_dir_path.append(fmt::format("{}.sql", 1));
    auto sql = read_file(sql_file_path.string());
    executeSql(i, sql, false, false, fmt::format("{}output.txt", index));
    sql_file_dir_path = sql_file_dir_path.parent_path();
  }

  // collect warmup metrics for later output
  std::string warmupMetrics = i.showMetrics();
  std::string warmupCacheMetrics = i.getOperatorManager()->showCacheMetrics();
  i.clearMetrics();

  i.getOperatorManager()->clearCacheMetrics();

  // script to collect resource usage
//  system("./scripts/measure_usage_start.sh ~/.aws/my-key-pair.pem scripts/iplist.txt");
  bool startInference = false;
  SPDLOG_CRITICAL("Execution phase:");
  for (auto index = warmBatchSize + 1; index <= warmBatchSize + executeBatchSize; ++index) {
    SPDLOG_CRITICAL("sql {}", index - warmBatchSize);
    if (cachingPolicy->id() == BELADY) {
      normal::cache::beladyMiniCatalogue->setCurrentQueryNum(index);
    } else if(cachingPolicy->id() == BELADYSC){
      normal::cache::beladySCMiniCatalogue->setCurrentQueryNum(index);
    } else if(cachingPolicy->id() == LEARN){
      normal::cache::beladyLearningMiniCatalogue->setCurrentQueryNum(index);
    }
    auto sql_file_path = sql_file_dir_path.append(fmt::format("{}.sql", index));
    auto sql = read_file(sql_file_path.string());
    executeSql(i, sql, true, writeResults, fmt::format("{}output.txt", index));
    if(cachingPolicy->id() == BELADYSC && index<(warmBatchSize + executeBatchSize)){
      auto beladySizeCostCachingPolicy = std::static_pointer_cast<normal::cache::BeladySizeCostCachingPolicy>(cachingPolicy);
      beladySizeCostCachingPolicy->updateWeight(index+1);
    } else if(cachingPolicy->id() == LEARN && index<(warmBatchSize + executeBatchSize)){
      auto learningCachingPolicy = std::static_pointer_cast<normal::cache::LearningCachingPolicy>(cachingPolicy);
      if(learningCachingPolicy->getStartInference()){
        if(!startInference){
          std::cout<<"start Inference   query Num:"<<index<<std::endl;
        }
        startInference=true;
        learningCachingPolicy->updateWeightAfterInference(index+1);
      }else{
        learningCachingPolicy->updateWeight(index+1);
      }
    }
    sql_file_dir_path = sql_file_dir_path.parent_path();
  }
  SPDLOG_CRITICAL("Execution phase finished");

  auto metricsFilePath = std::filesystem::current_path().append("metrics-" + modeAlias + "-" + 
    cachingPolicyAlias + "-"+std::to_string(cacheSize) + "-" + std::to_string(wMethod) + "-" + 
    std::to_string(isSplit) + "-" +std::to_string(isBitMap) + "-" + std::to_string(isDP) + "-" + std::to_string(featureLabel)
    + "-" +std::to_string(multiModel));
  std::ofstream fout(metricsFilePath.string());
  fout << mode->toString() << " mode finished in dirPrefix:" << dirPrefix << "\n";
  fout << "Warmup metrics:\n" << warmupMetrics << "\n";
  fout << "Warmup Cache metrics:\n" << warmupCacheMetrics << "\n";
  fout << "Execution metrics:\n" << i.showMetrics() << "\n";
  fout << "Execution Cache metrics:\n" << i.getOperatorManager()->showCacheMetrics() << "\n";
  fout << "All Cache hit ratios:\n" << i.showHitRatios() << "\n";

  fout << "OnLoad time: " << (double)( i.getCachingPolicy()->onLoadTime) / 1000000000.0 <<"\n";
  fout << "OnStore time: " << (double)( i.getCachingPolicy()->onStoreTime) / 1000000000.0 <<"\n";
  fout << "OnToCache time: " <<(double)( i.getCachingPolicy()->onToCacheTime) / 1000000000.0 <<"\n";
  fout << "OnWeight time:  "<< (double)( i.getCachingPolicy()->onWeightTime) / 1000000000.0<< "\n";
  fout << "updateWeight time:  "<< (double)( i.getCachingPolicy()->updateWeightTime) / 1000000000.0<< "\n";
  
  fout << "bldscLessValue1 num:  "<< bldscLessValue1Gloal<< "\n";
  fout << "bldscLessValue2 num:  "<< bldscLessValue2Gloal<< "\n";
  fout << "bldscLessValue3 num:  "<< bldscLessValue3Gloal<< "\n";
  fout << "bldscLessValue4 num:  "<< bldscLessValue4Gloal<< "\n";

  fout.flush();
  fout.close();

  SPDLOG_CRITICAL("test1");   
  i.clearMetrics();
  i.getOperatorManager()->clearCacheMetrics();
  SPDLOG_CRITICAL("test2");

  i.getOperatorGraph().reset();
  i.stop();
}
