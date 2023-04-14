#ifndef WEBCACHESIM_ANNOTATE_H
#define WEBCACHESIM_ANNOTATE_H

#include <string>
#include <random>
#include <cmath>

void annotate(const std::string &trace_file, const int &n_extra_fields, int offline_num,int offline_threshold);
void annotate_cost(const std::string &trace_file, const int &n_extra_fields, bool is_offline, int offline_num,
    int64_t delta_ratio, int64_t fixed_byte, int64_t min_ratio);

#endif //WEBCACHESIM_ANNOTATE_H
