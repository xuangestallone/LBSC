project("normal-pushdown" VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)

# need to add and link libraries on mac
LINK_DIRECTORIES(/usr/local/opt/llvm@11/lib)
LINK_DIRECTORIES(/usr/local/opt/binutils/lib)
INCLUDE_DIRECTORIES(/usr/local/include)

#-----------------------------------------------------------------------------------------------------------------------
# Target
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-pushdown STATIC
        include/normal/pushdown/Globals.h
        src/collate/Collate.cpp include/normal/pushdown/collate/Collate.h
        src/s3/S3SelectScan.cpp include/normal/pushdown/s3/S3SelectScan.h
        src/file/FileScan.cpp include/normal/pushdown/file/FileScan.h
        src/aggregate/Aggregate.cpp include/normal/pushdown/aggregate/Aggregate.h
        src/s3/S3CSVParser.cpp include/normal/pushdown/s3/S3CSVParser.h
        src/aggregate/AggregationResult.cpp include/normal/pushdown/aggregate/AggregationResult.h
        src/aggregate/Sum.cpp include/normal/pushdown/aggregate/Sum.h
        src/aggregate/AggregationFunction.cpp include/normal/pushdown/aggregate/AggregationFunction.h
        src/AWSClient.cpp include/normal/pushdown/AWSClient.h
        src/project/Project.cpp include/normal/pushdown/project/Project.h
        src/join/JoinPredicate.cpp include/normal/pushdown/join/JoinPredicate.h
        src/join/HashJoinProbe.cpp include/normal/pushdown/join/HashJoinProbe.h
        src/join/HashJoinBuild.cpp include/normal/pushdown/join/HashJoinBuild.h
        src/join/ATTIC/HashTableMessage.cpp include/normal/pushdown/join/ATTIC/HashTableMessage.h
        src/join/ATTIC/HashTable.cpp include/normal/pushdown/join/ATTIC/HashTable.h
        src/join/ATTIC/Joiner.cpp include/normal/pushdown/join/ATTIC/Joiner.h
        src/filter/Filter.cpp include/normal/pushdown/filter/Filter.h
        src/filter/FilterPredicate.cpp include/normal/pushdown/filter/FilterPredicate.h
        src/ATTIC/Sort.cpp include/normal/pushdown/ATTIC/Sort.h
        src/ATTIC/cacheScan.cpp include/normal/pushdown/ATTIC/cacheScan.h
        src/group/Group.cpp include/normal/pushdown/group/Group.h
        src/group/GroupKey.cpp include/normal/pushdown/group/GroupKey.h
        src/cache/CacheHelper.cpp include/normal/pushdown/cache/CacheHelper.h
        src/s3/S3SelectCSVParseOptions.cpp include/normal/pushdown/s3/S3SelectCSVParseOptions.h
        src/Util.cpp include/normal/pushdown/Util.h
        src/shuffle/Shuffle.cpp include/normal/pushdown/shuffle/Shuffle.h
        src/shuffle/ATTIC/Shuffler.cpp include/normal/pushdown/shuffle/ATTIC/Shuffler.h
        src/cache/CacheLoad.cpp include/normal/pushdown/cache/CacheLoad.h
        src/merge/Merge.cpp include/normal/pushdown/merge/Merge.h
        src/merge/MergeKernel.cpp include/normal/pushdown/merge/MergeKernel.h
        src/scan/ScanMessage.cpp include/normal/pushdown/scan/ScanMessage.h
        src/shuffle/ATTIC/ShuffleKernel.cpp include/normal/pushdown/shuffle/ATTIC/ShuffleKernel.h
        src/shuffle/ATTIC/RecordBatchShuffler.cpp include/normal/pushdown/shuffle/ATTIC/RecordBatchShuffler.h
        src/shuffle/ShuffleKernel2.cpp include/normal/pushdown/shuffle/ShuffleKernel2.h
        src/join/ATTIC/HashJoinBuildKernel.cpp include/normal/pushdown/join/ATTIC/HashJoinBuildKernel.h
        src/join/ATTIC/HashJoinProbeKernel.cpp include/normal/pushdown/join/ATTIC/HashJoinProbeKernel.h
        src/join/HashJoinProbeKernel2.cpp include/normal/pushdown/join/HashJoinProbeKernel2.h
        src/join/HashJoinBuildKernel2.cpp include/normal/pushdown/join/HashJoinBuildKernel2.h
        src/join/RecordBatchJoiner.cpp include/normal/pushdown/join/RecordBatchJoiner.h
        src/join/TupleSetIndexMessage.cpp include/normal/pushdown/join/TupleSetIndexMessage.h
        src/bloomjoin/BloomCreateOperator.cpp include/normal/pushdown/bloomjoin/BloomCreateOperator.h
        src/bloomjoin/FileScanBloomUseOperator.cpp include/normal/pushdown/bloomjoin/FileScanBloomUseOperator.h
        src/bloomjoin/BloomCreateKernel.cpp include/normal/pushdown/bloomjoin/BloomCreateKernel.h
        src/bloomjoin/FileScanBloomUseKernel.cpp include/normal/pushdown/bloomjoin/FileScanBloomUseKernel.h
        src/bloomjoin/SlicedBloomFilter.cpp include/normal/pushdown/bloomjoin/SlicedBloomFilter.h
        src/bloomjoin/UniversalHashFunction.cpp include/normal/pushdown/bloomjoin/UniversalHashFunction.h
        src/bloomjoin/UniversalSQLHashFunction.cpp include/normal/pushdown/bloomjoin/UniversalSQLHashFunction.h
        src/bloomjoin/BloomFilterMessage.cpp include/normal/pushdown/bloomjoin/BloomFilterMessage.h
        src/file/FileScanKernel.cpp include/normal/pushdown/file/FileScanKernel.h
        src/s3/S3SelectScanKernel.cpp include/normal/pushdown/s3/S3SelectScanKernel.h
        src/s3/S3SelectScan2.cpp include/normal/pushdown/s3/S3SelectScan2.h
        include/normal/pushdown/Forward.h
        include/normal/pushdown/scan/ScanOperator.h
        src/file/FileScan2.cpp include/normal/pushdown/file/FileScan2.h
        src/collate/Collate2.cpp include/normal/pushdown/collate/Collate2.h
        src/group/GroupKernel.cpp include/normal/pushdown/group/GroupKernel.h
        src/group/GroupKernel2.cpp include/normal/pushdown/group/GroupKernel2.h
        include/normal/pushdown/group/GroupKey2.h
        include/normal/pushdown/aggregate/AggregateBuilder.cpp include/normal/pushdown/aggregate/AggregateBuilder.h
        include/normal/pushdown/aggregate/AggregateBuilderWrapper.cpp include/normal/pushdown/aggregate/AggregateBuilderWrapper.h
        include/normal/pushdown/s3/S3Select.h include/normal/pushdown/s3/S3Get.h src/s3/S3Select.cpp src/s3/S3Get.cpp
        include/normal/pushdown/shuffle/RecordBatchShuffler2.h src/shuffle/RecordBatchShuffler2.cpp)

target_include_directories(normal-pushdown PUBLIC include)

target_link_libraries(normal-pushdown PUBLIC pthread)
target_link_libraries(normal-pushdown PRIVATE spdlog::spdlog)
target_link_libraries(normal-pushdown PRIVATE primesieve::libprimesieve-static)
target_link_libraries(normal-pushdown PRIVATE fmt::fmt)
target_link_libraries(normal-pushdown PUBLIC aws-cpp-sdk-core)
target_link_libraries(normal-pushdown PUBLIC aws-cpp-sdk-s3)
target_link_libraries(normal-pushdown PUBLIC arrow_static)
target_link_libraries(normal-pushdown PUBLIC gandiva_static)
target_link_libraries(normal-pushdown PUBLIC normal-core)
target_link_libraries(normal-pushdown PUBLIC normal-tuple)
target_link_libraries(normal-pushdown PUBLIC normal-expression)
target_link_libraries(normal-pushdown PUBLIC normal-expression-gandiva)


#-----------------------------------------------------------------------------------------------------------------------
# Test
#-----------------------------------------------------------------------------------------------------------------------



#-----------------------------------------------------------------------------------------------------------------------
# Micro Benchmark
#-----------------------------------------------------------------------------------------------------------------------


#-----------------------------------------------------------------------------------------------------------------------
# Test Data
#-----------------------------------------------------------------------------------------------------------------------


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------

#showTargetProps(normal-pushdown)
