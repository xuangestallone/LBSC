#pragma once
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <unistd.h>

namespace parser_competitors {

    typedef float float32_t;

    struct Request {
        int32_t appId;
        int64_t valueSize;
        int64_t id;
        int64_t valueCost;

        inline int64_t size() const { return valueSize; }
        inline int64_t cost() const { return valueCost; }
    } __attribute__((packed));


}
