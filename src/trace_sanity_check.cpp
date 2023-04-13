//
//  on 2/21/20.
//

#include "trace_sanity_check.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <unordered_map>
#include "utils.h"
#include "file_hash.h"
#include "bsoncxx/builder/basic/document.hpp"
#include "bsoncxx/json.hpp"
#include "mongocxx/client.hpp"
#include "mongocxx/instance.hpp"
#include "mongocxx/uri.hpp"

using namespace std;
using bsoncxx::builder::basic::kvp;

//max object size is 4GB
const uint64_t max_obj_size = 0xffffffff;
//max n req in a trace is 4 Billion
const uint64_t max_n_req = 0xffffffff;
//max n_extra_field = 4, as this field is statically allocated
const int max_n_extra_fields = 4;
//extra_value is uint16_t
const int max_extra = 0xffff;

const string file_hash_collection = "file_hash";
using bsoncxx::builder::basic::make_document;

bool trace_sanity_check(const string &trace_file, map<string, string> &params, uint64_t& req_num, uint64_t& max_data_size) {
    /*
     * cache the sanity check results
     * {hash: , filename: , valid: }
     * */
    uint32_t hash;
    if (params.find("dburi") != params.end()) {
        hash = get_crc32(trace_file);
        try {
            mongocxx::client client = mongocxx::client{mongocxx::uri(params["dburi"])};
            auto db = client[mongocxx::uri(params["dburi"]).database()];
            auto cursor = db[file_hash_collection].find_one(make_document(kvp("hash", to_string(hash))));
            if (cursor && !(cursor->view().empty())) {
                auto a = cursor->view().find("valid")->get_utf8().value.to_string();
                if ("1" == a) {
                    cerr<<"sanity check pass by querying cache"<<endl;
                    return true;
                } else {
                    cerr<<"sanity check fail by querying cache"<<endl;
                    return false;
                }
            }
        } catch (const std::exception &xcp) {
            throw std::runtime_error("warning: db connection failed: " + string(xcp.what()));
        }
    }

    cerr << "running sanity check on trace: " << trace_file << endl;
    bool if_pass = true;

    ifstream infile(trace_file);
    if (!infile) {
        throw std::runtime_error("Exception opening file " + trace_file);
    }

    auto it = params.find("n_extra_fields");
    if (it == params.end()) {
        throw std::runtime_error("n_extra_fields not available");
    }
    int n_extra_fields = stoi(it->second);
    //assume format: t id size extra0 extra1 ...
    if (n_extra_fields > max_n_extra_fields) { 
        cerr<<"error: n_extra_fields "<<n_extra_fields<<" > max_n_extra_fields "<<max_n_extra_fields<<endl;
        if_pass = false;
    }
    cerr<<"n_extra_fields: "<<n_extra_fields<<endl;

    uint64_t t, key, size;
    vector<uint64_t > extra_features(n_extra_fields);
    //key -> size
    unordered_map<uint64_t, uint32_t> size_map;
    uint64_t n_req = 0;
    uint64_t max_size = 0;
    uint64_t min_size = -1;
    uint64_t mean_size = 0;

    //uint64_t byte_million_req = 0;
    int index = 0;
    uint64_t total_req_size = 0;
    uint64_t unique_req_size = 0;

    while (infile >> t >> key >> size) { 
        total_req_size += size;
        if(min_size == - 1 || size < min_size){
            min_size = size;
        }
        if(max_size < size){
            max_size = size;
        }
        //mean_size = (mean_size * index + size)/(index+1);
        index++;
        
        for (int i = 0; i < n_extra_fields; ++i) {
            infile >> extra_features[i];
        }

        for (int i = 0; i < n_extra_fields; ++i) {
            if (extra_features[i] > max_extra) { 
                cerr<<"req: "<<n_req<<" extra "<<i<<":"<<extra_features[i]<<" > max_extra "<<max_extra<<endl;
                if_pass = false;
            }
        }

        if (size > max_obj_size) {
            cerr<<"req: "<<n_req<<" size "<<size<<" > max_obj_size "<<max_obj_size<<endl;
            if_pass = false;
        }
        if (size == 0) { 
            cerr<<"req: "<<n_req<<" size == 0"<<endl;
            if_pass = false;
        }

        auto it = size_map.find(key);
        if (it == size_map.end()) {
            size_map.insert({key, size});
            unique_req_size+=size;
        } else {
            if (it->second != size) {
                cerr<<"req: "<<n_req<<", key: "  <<key<<" size inconsistent. Old size: "<<it->second<<" new size: "<<size<<endl;
                if_pass = false;
            }
        }
        if (!(n_req%1000000)) {
            cerr<<"n_req: "<<n_req<<endl;
        }
        ++n_req;
    }
    if (n_req > max_n_req) {
        cerr<<"n_req "<<n_req<<" > max_n_req "<<max_n_req<<endl;
        if_pass = false;
    }

    {
        mean_size = unique_req_size/size_map.size();
        cerr <<"statistic information:"<<endl;
        cerr <<"total request:"<<index<<" unique request: "<<size_map.size()<<endl;
        cerr <<"total request size: "<< total_req_size <<" bytes;   unique request size: "<<unique_req_size <<" bytes"<<endl;
        cerr <<"max object size: "<<max_size<<" bytes;    min object size: "<<min_size<<" bytes;   mean object size: "<<mean_size<<" bytes"<<endl;

    }

    max_data_size = max_size;
    req_num = index;


    if (params.find("dburi") != params.end()) {
        cerr<<"caching sanity check result"<<endl;
        bsoncxx::builder::basic::document key_builder{};
        bsoncxx::builder::basic::document value_builder{};
        key_builder.append(kvp("hash", to_string(hash)));
        value_builder.append(kvp("filename", trace_file));
        value_builder.append(kvp("valid", to_string(if_pass)));
        for (bsoncxx::document::element ele: key_builder.view()) {
            value_builder.append(kvp(ele.key(), ele.get_value()));
        }
        try {
            mongocxx::client client = mongocxx::client{mongocxx::uri(params["dburi"])};
            auto db = client[mongocxx::uri(params["dburi"]).database()];
            mongocxx::options::replace option;
            db[file_hash_collection].replace_one(key_builder.extract(), value_builder.extract(), option.upsert(true));
        } catch (const std::exception &xcp) {
            throw std::runtime_error("warning: db connection failed: " + string(xcp.what()));
        }
    }

    infile.close();
    return if_pass;
}

