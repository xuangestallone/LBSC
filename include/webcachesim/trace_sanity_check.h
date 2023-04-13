//
//  on 2/21/20.
//

#ifndef WEBCACHESIM_TRACE_SANITY_CHECK_H
#define WEBCACHESIM_TRACE_SANITY_CHECK_H

#include <string>
#include <map>

bool trace_sanity_check(const std::string& trace_file, std::map<std::string, std::string> &params, uint64_t& req_num, uint64_t& max_data_size);


#endif //WEBCACHESIM_TRACE_SANITY_CHECK_H
