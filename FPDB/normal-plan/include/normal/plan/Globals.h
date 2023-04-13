//
//  15/4/20.
//

#ifndef NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_GLOBALS_H
#define NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_GLOBALS_H

#define LOG_LEVEL SPDLOG_LEVEL_DEBUG

#define SPDLOG_ACTIVE_LEVEL LOG_LEVEL
#include <spdlog/spdlog.h>
#include <aws/s3/S3Client.h>
#include <normal/pushdown/AWSClient.h>

namespace normal::plan {

enum S3ClientType {
  S3,
  Airmettle,
  Minio
};

inline constexpr int NumRanges = 1;
inline constexpr int JoinParallelDegree = 48;
inline S3ClientType s3ClientType = Minio;
inline std::shared_ptr<Aws::S3::S3Client> DefaultS3Client;
}

#endif //NORMAL_NORMAL_PLAN_INCLUDE_NORMAL_PLAN_GLOBALS_H
