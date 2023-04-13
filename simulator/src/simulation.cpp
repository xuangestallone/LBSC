//
//  on 10/30/18.
//

#include "simulation.h"
#include "annotate.h"
#include "trace_sanity_check.h"
#include "simulation_tinylfu.h"
#include <sstream>
#include "utils.h"
#include "rss.h"
#include <cstdint>
#include <unordered_map>
#include <numeric>
#include "bsoncxx/builder/basic/document.hpp"
#include "bsoncxx/json.hpp"
// #define  MAX_REQ_NUM 100000000

// #include <Eigen/Dense>

using namespace std;
using namespace chrono;
using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::sub_array;



FrameWork::FrameWork(const string &trace_file, const string &cache_type, const uint64_t &cache_size,
                     map<string, string> &params) {
    _trace_file = trace_file;
    _cache_type = cache_type;
    _cache_size = cache_size;
    is_offline = offline_algorithms.count(_cache_type);
    is_cost = cost_algorithms.count(_cache_type);
    _offline_num=1;
    _cost_threshold=0;
    _offline_threshold=10000000;

    _delta_ratio=20;
    _fixed_byte=1024;
    _min_ratio=40;


    for (auto it = params.cbegin(); it != params.cend();) {
        if (it->first == "uni_size") {
            uni_size = static_cast<bool>(stoi(it->second));
            it = params.erase(it);
        } else if (it->first == "is_metadata_in_cache_size") {
            is_metadata_in_cache_size = static_cast<bool>(stoi(it->second));
#ifdef EVICTION_LOGGING
            if (true == is_metadata_in_cache_size) {
                throw invalid_argument(
                        "error: set is_metadata_in_cache_size while EVICTION_LOGGING. Must not consider metadata overhead");
            }
#endif
            it = params.erase(it);
        } else if (it->first == "offline_num"){
            _offline_num = stoi((it->second));
            cerr<<"offline_num:"<<_offline_num<<endl;
            it = params.erase(it);
        }
        else if (it->first == "delta_ratio"){
            _delta_ratio = stoi((it->second));
            cerr<<"_delta_ratio:"<<_delta_ratio<<endl;
            it = params.erase(it);
        }
        else if (it->first == "min_ratio"){
            _min_ratio = stoi((it->second));
            cerr<<"_min_ratio:"<<_min_ratio<<endl;
            it = params.erase(it);
        }
        else if (it->first == "fixed_byte"){
            _fixed_byte = stoi((it->second));
            cerr<<"_fixed_byte:"<<_fixed_byte<<endl;
            it = params.erase(it);
        } else if(it->first == "is_statistical"){
            uint uint_is_statistical = stoul(it->second);
            if(uint_is_statistical != 0){
                is_statistical = true;
            }else{
                is_statistical = false;
            }
            cerr << "is_statistical: " << is_statistical <<endl;
            it = params.erase(it);
        } else if (it->first == "cost_threshold"){
            _cost_threshold = stoi((it->second));
            it = params.erase(it);
        }else if (it->first == "offline_threshold"){
            _offline_threshold = stoi((it->second));
            it = params.erase(it);
        }else if (it->first == "bloom_filter") {
            bloom_filter = static_cast<bool>(stoi(it->second));
            it = params.erase(it);
        } else if (it->first == "segment_window") {
            segment_window = stoull((it->second));
            ++it;
        } else if (it->first == "n_extra_fields") {
            n_extra_fields = stoi((it->second));
            cout<<"n_extra_fields:"<<n_extra_fields<<endl;
            ++it;
        } else if (it->first == "real_time_segment_window") {
            real_time_segment_window = stoull((it->second));
            it = params.erase(it);
        } else if (it->first == "n_early_stop") {
            n_early_stop = stoll((it->second));
            ++it;
        } else if (it->first == "seq_start") {
            seq_start = stoll((it->second));
            ++it;
        } else {
            ++it;
        }
    }
#ifdef EVICTION_LOGGING  
    //logging eviction requires next_seq information
    is_offline = true;
#endif

    if(is_statistical){
        cout<<"trace_file: "<<_trace_file<<endl;
        write_cost_size_ratio.open(trace_file+"_cost_size_ratio.txt");
        write_size_dist.open(trace_file+"_size_dist.txt");
        write_re_dist.open(trace_file+"_reuse_distance.txt");
        write_map_obj.open(trace_file+"_map_address_object.txt");

        if (!write_cost_size_ratio || !write_size_dist || !write_re_dist  ||
            !write_map_obj ) {
            cerr << "open write file error" << endl;
            exit(-1);
        }

        ifstream readFile(_trace_file);
        if (!readFile) {
            cerr << "Exception opening/reading file " << _trace_file << endl;
            exit(-1);
        }

        uint64_t id;
        int64_t i = 0;
        uint64_t t, size, cost;
        uint64_t extra_feature;
        uint64_t next_use;
        uint64_t current_size = 0;
        uint64_t current_cost = 0;

        vector<vector<uint64_t>> tmp_datas;
        uint64_t last_index = 0;
        uint64_t total_cost = 0;
        uint64_t total_size = 0;

        while(readFile>> cost >> next_use >> t >> id >> size) {
            readFile >> extra_feature;
            if(size==0){
                cerr<<cost<<" "<<next_use<<" "<<t<<" "<<id<<" "<<size<<" "<<endl;
                exit(-1);
            }
            total_cost+=cost;
            total_size+=size;
            if (!(++i%1000000)){
                cerr<<"reading origin trace: "<<i<<"  partial statistical information as following:"<<endl;
                cerr<<"cost_size  max:"<<max_cost_size<<"  min:"<<min_cost_size<<endl;
                cerr<<"size  max:"<<max_size << "   min:"<<min_size<<endl;
                cerr<<"next use  max:"<<max_re_dist<<"  min:"<<min_re_dist<<endl;
                cerr<<"unique items:"<<last_index<<endl;
            }

            long double tmp_cost_size_rate = static_cast<double>(cost)/static_cast<double>(size);
            size_cost_ratios.insert(tmp_cost_size_rate);
            if(min_cost_size < 0 || min_cost_size > tmp_cost_size_rate){
                min_cost_size = tmp_cost_size_rate;
            }
            if(max_cost_size < 0 || max_cost_size < tmp_cost_size_rate){
                max_cost_size = tmp_cost_size_rate;
            }
            size_dist.insert(size);
            if(min_size == 0 || min_size > size){
                min_size = size;
            }
            if(max_size == 0 || max_size < size){
                max_size = size;
            }


            //reuse distance
            auto iter = re_dist_ratios.find(next_use-i);
            if(iter == re_dist_ratios.end()){
                re_dist_ratios[next_use-i+1] = 1;
            }else{
                re_dist_ratios[next_use-i+1]++;
            }
            if((next_use < 4294967295) && (min_re_dist == 0 || min_re_dist>(next_use-i+1))){
                min_re_dist = (next_use-i+1);
            }
            if((next_use <4294967295) && (max_re_dist == 0|| max_re_dist<(next_use-i+1))){
                max_re_dist = (next_use-i+1);
            }


            auto iterM = m_map_obj.find(id);
            if(iterM == m_map_obj.end()){
                m_map_obj[id] = last_index;
                ++last_index;
            }
            write_map_obj<<m_map_obj[id]<<endl;
        }
        write_map_obj.close();
        readFile.close();

        for(auto itSCR = size_cost_ratios.begin();itSCR!=size_cost_ratios.end();itSCR++){
            write_cost_size_ratio<<*itSCR<<endl;
        }
        cerr<<"write_cost_size_ratio write done"<<endl;

        for(auto itSD = size_dist.begin();itSD!=size_dist.end();itSD++){
            write_size_dist<<*itSD<<endl;
        }
        cerr<<"write_size_dist write done"<<endl;

        for(auto itRDR = re_dist_ratios.begin();itRDR!=re_dist_ratios.end();itRDR++){
            write_re_dist<<itRDR->first<<" "<<itRDR->second<<endl;
        }
        cerr<<"write_re_dist write done"<<endl;
        write_cost_size_ratio.close();
        write_size_dist.close();
        write_re_dist.close();
        cerr<<"genearet statistical information done.....exit"<<endl;
        cerr<<"total cost:"<<total_cost<<endl;
        cerr<<"total size:"<<total_size<<endl;
        exit(0);
    }




    //trace_file related init 
    if (is_offline) {
        cerr<<"offline_num:"<<_offline_num<<endl;
        annotate(_trace_file, n_extra_fields, _offline_num, _offline_threshold);
    } 

    if (is_offline) {
        _trace_file = _trace_file+"_"+to_string(_offline_num) + ".ant";
    }
    if(is_cost){
        cerr<<"offline_num:"<<_offline_num<<endl;
        annotate_cost(_trace_file, n_extra_fields, is_offline, _offline_num,_delta_ratio, _fixed_byte, _min_ratio);
    }


    if(is_cost){
        _trace_file = _trace_file + ".blc_d"+
        to_string(_delta_ratio)+"_f"+to_string(_fixed_byte)+"_m"+to_string(_min_ratio);
    }

    infile.open(_trace_file);
    cerr<<"_trace_file:"<<_trace_file<<endl;
    if (!infile) {
        cerr << "Exception opening/reading file " << _trace_file << endl;
        exit(-1);
    }
    webcache = move(Cache::create_unique(cache_type));
    if (webcache == nullptr) throw runtime_error("Error: cache type " + cache_type + " not implemented");
    webcache->setSize(cache_size);
    webcache->init_with_params(params);

    adjust_real_time_offset();
    extra_features = vector<uint16_t>(n_extra_fields);
}

void FrameWork::adjust_real_time_offset() {
    if (is_offline) {
        if(is_cost){
            infile >> cost >> next_seq >> t;
        }else{
            infile >> next_seq >> t;
        }
    } else {
        if(is_cost){
            infile >> cost >> t;
        }else{
            infile >> t;
        }
    }
    time_window_end =
            real_time_segment_window * (t / real_time_segment_window + (t % real_time_segment_window != 0));
    infile.clear();
    infile.seekg(0, ios::beg);
}


void FrameWork::update_real_time_stats() {
    rt_seg_byte_miss.emplace_back(rt_byte_miss);
    rt_seg_byte_req.emplace_back(rt_byte_req);
    rt_seg_object_miss.emplace_back(rt_obj_miss);
    rt_seg_object_req.emplace_back(rt_obj_req);
    rt_byte_miss = rt_obj_miss = rt_byte_req = rt_obj_req = 0;
    auto metadata_overhead = get_rss();
    rt_seg_rss.emplace_back(metadata_overhead);
    time_window_end += real_time_segment_window;
}

void FrameWork::update_stats() {
    auto _t_now = chrono::system_clock::now();
    cerr << "\nseq: " << seq << endl 
         << "cache size: " << webcache->_currentSize << "/" << webcache->_cacheSize
         << " (" << ((double) webcache->_currentSize) / webcache->_cacheSize << ")" << endl
         << "delta t: " << chrono::duration_cast<std::chrono::milliseconds>(_t_now - t_now).count() / 1000 << "s"
         << endl;
    t_now = _t_now;
    cerr << "segment bmr: " << double(byte_miss) / byte_req << endl;
    cerr << "segment omr: " << double(obj_miss) / obj_req << endl;
    cerr << "segment read_size: " << segment_read_size << endl;
    cerr << "segment get_time: " << interval_get_time/1000 << "ms" << endl;
    segment_read_size=0;
    interval_get_time=0; 
    seg_byte_miss.emplace_back(byte_miss);
    seg_byte_req.emplace_back(byte_req);
    seg_object_miss.emplace_back(obj_miss);
    seg_object_req.emplace_back(obj_req);
    seg_byte_in_cache.emplace_back(webcache->_currentSize);
    byte_miss = obj_miss = byte_req = obj_req = 0;
    auto metadata_overhead = get_rss(); 
    seg_rss.emplace_back(metadata_overhead);
    if (is_metadata_in_cache_size) { 
        webcache->setSize(_cache_size - metadata_overhead);
    }
    cerr << "rss: " << metadata_overhead << endl;
    webcache->update_stat_periodic();
}


bsoncxx::builder::basic::document FrameWork::simulate() {
    cerr << "simulating" << endl;

    unordered_map<uint64_t, uint32_t> future_timestamps;
    vector<uint8_t> eviction_qualities;
    vector<uint16_t> eviction_logic_timestamps;
    if (bloom_filter) {
        filter = new AkamaiBloomFilter;
    }

    SimpleRequest *req;
    if (is_offline){
        if(_cache_type=="BeladySampleSizeCostMulti"){
            vector<int64_t> test;
            req = new AnnotatedCostRequest(0,0,0, test);
        }else{
            req = new AnnotatedRequest(0, 0, 0, 0);
        }
    }else{
        req = new SimpleRequest(0, 0, 0);
    }
    t_now = system_clock::now(); 

    int64_t seq_start_counter = 0;
    cerr<<"cache size:"<<webcache->_cacheSize<<endl;
    vector<int64_t> next_seqs(_offline_num, 0);
    while (true) {
        if (is_offline) {
            if(is_cost){
                if(_cache_type=="BeladySampleSizeCostMulti"){
                    if(!(infile>>cost)){
                        break;
                    }
                    for(int i=0;i<_offline_num;i++){
                        infile >> next_seqs[i];
                    }
                    infile >> t >> id >> size;

                }else{
                    if (!(infile >> cost >> next_seq >> t >> id >> size))
                        break;
                }
            }else{
                if(_cache_type=="BeladySampleSizeCostMulti"){
                    for(int i=0;i<_offline_num;i++){
                        if(!(infile >> next_seqs[i])){
                            break;
                        }
                    }
                    if(!(infile >> t >> id >> size)){
                        break;
                    }

                }else{
                    if (!(infile >> next_seq >> t >> id >> size))
                        break;
                }
            }
        } else {
            if(is_cost){
                if (!(infile >> cost >> t >> id >> size))  
                    break;
            }else{
                if (!(infile >> t >> id >> size))
                    break;
            }
        }

        if (seq_start_counter++ < seq_start) { 
            continue;
        }
        if (seq == n_early_stop) 
            break;

        for (int i = 0; i < n_extra_fields; ++i) 
            infile >> extra_features[i];
        if (uni_size) 
            size = 1;

        while (t >= time_window_end) { 
            update_real_time_stats();
        }
        if (seq && !(seq % segment_window)) {
            update_stats();
        }
        update_metric_req(byte_req, obj_req, size);
        update_metric_req(rt_byte_req, rt_obj_req, size)

        if (is_offline){
            if(is_cost){
                if(_cache_type=="BeladySampleSizeCostMulti"){
                    dynamic_cast<AnnotatedCostRequest *>(req)->reinit(seq, id, size, next_seqs, cost, &extra_features);
                }else{
                    dynamic_cast<AnnotatedRequest *>(req)->reinit(seq, id, size, next_seq, cost, &extra_features);
                }
            }else{
                if(_cache_type=="BeladySampleSizeCostMulti"){
                    dynamic_cast<AnnotatedCostRequest *>(req)->reinit(seq, id, size, next_seqs, &extra_features);
                }else{
                    dynamic_cast<AnnotatedRequest *>(req)->reinit(seq, id, size, next_seq, &extra_features);
                }
            } 
        } else {
            if(is_cost){
                req->reinit(seq, id, size, cost, &extra_features); 
            }else{
                req->reinit(seq, id, size, &extra_features); 
            }
        }

        bool is_admitting = true;
        if (true == bloom_filter) {
            bool exist_in_cache = webcache->exist(req->id);
            //in cache object, not consider bloom_filter 
            if (false == exist_in_cache) {
                is_admitting = filter->exist_or_insert(id);
            }
        }
        if (is_admitting) {
            bool is_hit = webcache->lookup(*req);

            if (!is_hit) {
                update_metric_req(byte_miss, obj_miss, size);
                update_metric_req(rt_byte_miss, rt_obj_miss, size);
                
                interval_get_time+=req->cost;
                if(seq>_cost_threshold){
                    total_get_time+=req->cost;
                }
                total_get_time_no_threshold+=req->cost;
                read_size+=req->size;
                segment_read_size+=req->size;
                webcache->admit(*req);
            }
        } else {
            update_metric_req(byte_miss, obj_miss, size);
            update_metric_req(rt_byte_miss, rt_obj_miss, size)
        }

        ++seq;
    }

    delete req;
    //for the residue segment of trace
    update_real_time_stats();
    update_stats();
    infile.close();

    return simulation_results();
}


bsoncxx::builder::basic::document FrameWork::simulation_results() {
    bsoncxx::builder::basic::document value_builder{};
    value_builder.append(kvp("read_size",read_size));
    double total_time_min = static_cast<double>(total_get_time)/ 1000.0;//ms
    total_time_min /= 1000.0;//s
    total_time_min /= 60.0;//min 
    value_builder.append(kvp("total_get_time",total_time_min));
    double total_time_min_no_threshold = static_cast<double>(total_get_time_no_threshold)/ 1000.0;//ms
    total_time_min_no_threshold /= 1000.0;//s
    total_time_min_no_threshold /= 60.0;//min 
    value_builder.append(kvp("total_get_time_no_threshold",total_time_min_no_threshold));
    value_builder.append(kvp("no_warmup_byte_miss_ratio",  
                             accumulate<vector<int64_t>::const_iterator, double>(seg_byte_miss.begin(),
                                                                                 seg_byte_miss.end(), 0) /
                             accumulate<vector<int64_t>::const_iterator, double>(seg_byte_req.begin(),
                                                                                 seg_byte_req.end(), 0)
    ));
    value_builder.append(kvp("no_warmup_object_miss_ratio",
                            accumulate<vector<int64_t>::const_iterator, double>(seg_object_miss.begin(),
                                                                                seg_object_miss.end(), 0) /
                            accumulate<vector<int64_t>::const_iterator, double>(seg_object_req.begin(),
                                                                                seg_object_req.end(), 0)
    ));
//    value_builder.append(kvp("byte_miss_cache", byte_miss_cache));
//    value_builder.append(kvp("byte_miss_filter", byte_miss_filter));
    value_builder.append(kvp("segment_byte_miss", [this](sub_array child) {
        for (const auto &element : seg_byte_miss)
            child.append(element);
    }));
    value_builder.append(kvp("segment_byte_req", [this](sub_array child) {
        for (const auto &element : seg_byte_req)
            child.append(element);
    }));
    value_builder.append(kvp("segment_object_miss", [this](sub_array child) {
        for (const auto &element : seg_object_miss)
            child.append(element);
    }));
    value_builder.append(kvp("segment_object_req", [this](sub_array child) {
        for (const auto &element : seg_object_req)
            child.append(element);
    }));
    value_builder.append(kvp("segment_rss", [this](sub_array child) {
        for (const auto &element : seg_rss)
            child.append(element);
    }));
    value_builder.append(kvp("segment_byte_in_cache", [this](sub_array child) {
        for (const auto &element : seg_byte_in_cache)
            child.append(element);
    }));
    webcache->update_stat(value_builder);
    return value_builder;
}

std::vector<std::string> split(const std::string& str, const std::string& splitStr) {
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



bsoncxx::builder::basic::document _simulation(string trace_file, string cache_type, uint64_t cache_size,
                                              map<string, string> params, uint64_t& req_num, uint64_t& max_data_size) {
    FrameWork frame_work(trace_file, cache_type, cache_size, params);
    frame_work.webcache->setReqNum(req_num, max_data_size);
    auto res = frame_work.simulate();
    return res;
}

bsoncxx::builder::basic::document _simulation_hro(string trace_file, string cache_type, uint64_t cache_size,
                                              map<string, string> params) {
    std::ifstream infile_hro;
    string original_trace_file = trace_file;
    if(cache_type=="HROCost"){
        trace_file=trace_file+".blc";
    }

    infile_hro.open(trace_file);
    cerr<<"hro_trace_file:"<<trace_file<<endl;
    if (!infile_hro) {
        cerr << "Exception opening/reading file " << trace_file << endl;
        exit(-1);
    }
    infile_hro.clear();
    infile_hro.seekg(0, ios::beg);
    uint64_t t_hro;
    uint64_t id_hro;
    uint64_t cost_hro;
    uint64_t size_hro;
    uint64_t extra_param;
    unordered_map<uint64_t,vector<uint64_t>> _map_time;
    unordered_map<uint64_t,uint64_t> _map_cost;
    unordered_map<uint64_t,uint64_t> _map_size;
    unordered_map<uint64_t,vector<uint64_t>> _map_inter;
    unordered_map<uint64_t,uint64_t> _map_counter;
    unordered_map<uint64_t, double> _map_avg; 
    uint64_t time_hro = 0;
    uint64_t max_req_size = 0;
    
    int extra_params = stoi(params["n_extra_fields"]);
    cerr<<"extra_params: "<< extra_params<<"  cache_type:"<<cache_type <<endl;
    if(cache_type == "HROCost"){
        cerr<<"test HROCost"<<endl;
        while (true) {
            if (!(infile_hro >>cost_hro>> t_hro >> id_hro >> size_hro)) 
                break;
            time_hro++;
            if(size_hro > max_req_size){
                max_req_size = size_hro;
            }
        
            for(int i=0;i<extra_params;i++){
                infile_hro >> extra_param;
            }
            _map_time[id_hro].push_back(time_hro);  
            _map_cost[id_hro]=cost_hro;
            auto iter = _map_size.find(id_hro);
            if(iter != _map_size.end()){
                if(iter->second != size_hro){
                    cerr << " initial key size not equal: " << id_hro <<" time:" << time_hro << endl;
                    cerr << iter->second << endl;
                    cerr << size_hro << endl;
                    exit(-1);
                }
            }else{
                _map_size.insert({id_hro, size_hro});
            }
        }
    }else{
        while (true) {
            if (!(infile_hro >> t_hro >> id_hro >> size_hro))  
                break;
            time_hro++;
            if(size_hro > max_req_size){
                max_req_size = size_hro;
            }
        
            for(int i=0;i<extra_params;i++){
                infile_hro >> extra_param;
            }
            _map_time[id_hro].push_back(time_hro);  
            auto iter = _map_size.find(id_hro);
            if(iter != _map_size.end()){
                if(iter->second != size_hro){
                    cerr << " initial key size not equal: " << id_hro <<" time:" << time_hro << endl;
                    cerr << iter->second << endl;
                    cerr << size_hro << endl;
                    exit(-1);
                }
            }else{
                _map_size.insert({id_hro, size_hro});
            }
        }
    }
    
    infile_hro.close();
    uint64_t total_dist;
    uint64_t key_dist_size;
    uint64_t max_scaled = time_hro * max_req_size;
    for(auto it=_map_time.begin(); it!=_map_time.end(); it++){
        _map_counter[it->first] = 0;
        _map_inter[it->first].push_back(max_scaled);

        key_dist_size = (it->second).size();
        total_dist = max_scaled;
        for(int i=0; i< key_dist_size - 1; i++){
            uint64_t tmp_dist = (it->second)[i+1] - (it->second)[i];
            _map_inter[it->first].push_back(tmp_dist);
            total_dist += tmp_dist;
        }

        _map_avg[it->first] = total_dist / (double)key_dist_size;
    }
    cerr<<"original_trace_file:"<<original_trace_file<<endl;
    FrameWork frame_work(original_trace_file, cache_type, cache_size, params);
    frame_work.webcache->setReqNum(time_hro, max_req_size);
    cerr<<"test id=0:"<<_map_size[0]<<endl;
    frame_work.webcache->setInters(_map_inter, _map_counter, _map_size, _map_avg, _map_cost);
    auto res = frame_work.simulate();
    return res;
}



bsoncxx::builder::basic::document simulation(string trace_file, string cache_type,
                                             uint64_t cache_size, map<string, string> params) {
    int n_extra_fields = get_n_fields(trace_file) - 3;
    params["n_extra_fields"] = to_string(n_extra_fields);

    bool enable_trace_format_check = true;
    if (params.find("enable_trace_format_check") != params.end()) {
        enable_trace_format_check = stoi(params.find("enable_trace_format_check")->second);
    }

    bool is_convert_MSR_to_tr = false;
    if(params.find("is_convert_MSR_to_tr") != params.end()){
        uint uint_is_convert_MSR_to_tr = stoul(params.find("is_convert_MSR_to_tr")->second);
        if(uint_is_convert_MSR_to_tr != 0){
            is_convert_MSR_to_tr = true;
        }else{
            is_convert_MSR_to_tr = false;
        }
        cerr << "is_convert_MSR_to_tr: " << is_convert_MSR_to_tr <<endl;
    }

    if(is_convert_MSR_to_tr){
        cout<<"trace file: "<<trace_file<<endl;
        string excepted_trace = trace_file+".tr";
        ifstream readFile(trace_file);
        if (!readFile) {
            cerr << "Exception opening/reading file " << trace_file << endl;
            exit(-1);
        }

        ofstream convertFile(excepted_trace);
        if (!convertFile) {
            cerr << "Exception opening/reading tmp file: " << excepted_trace << endl;
            exit(-1);
        }

        string online;
        uint64_t timeIndex = 0;
        std::hash<std::string> strHash;
        uint64_t total_size = 0;
        unordered_map<string, uint32_t> key_size_maps;
        while(readFile>> online) {
            vector<string> splits = split(online, ",");
            uint32_t size = stoul(splits[5].c_str());
            total_size += size;
            auto itt = key_size_maps.find(splits[4]);
            if(itt!=key_size_maps.end()){
                if(size>itt->second){
                    itt->second = size;
                }
            }else{
                key_size_maps.insert({splits[4],size});
            }

            timeIndex++;
            if(timeIndex %1000000 ==0){
                cerr<<"process "<<timeIndex<<" items"<<endl;
            }
        }
        readFile.close();
        readFile.open(trace_file);
        if (!readFile) {
            cerr << "Exception opening/reading file " << trace_file << endl;
            exit(-1);
        }
        cerr<<"unique request:"<<key_size_maps.size()<<endl;
        cerr<<"original total size:"<<total_size/1073741824<<"  GB"<<endl;
        
        total_size=0;
        timeIndex=0;
        while(readFile>> online) {
            vector<string> splits = split(online, ",");
            if(splits[3]=="Read"){
                int64_t id = atoll(splits[4].c_str());
                if(id <0){
                    cerr<<"id must more than 0   id:"<<id<<"  original_id:"<<splits[4]<<endl;
                    exit(-1);
                }

                convertFile<<timeIndex<<" "<<splits[4]<<" "<<key_size_maps[splits[4]]<<" "<<0<<endl;
            }else{
                convertFile<<timeIndex<<" "<<splits[4]<<" "<<key_size_maps[splits[4]]<<" "<<1<<endl;
            }
            total_size+=key_size_maps[splits[4]];
            timeIndex++;
            if(timeIndex %1000000 ==0){
                cerr<<"process "<<timeIndex<<" items"<<endl;
            }
        }

        cerr<<"after convert total size:"<<total_size/1073741824<<"  GB"<<endl;
        readFile.close();

        convertFile.close();
        cerr<<"convert done"<<endl;
        exit(0);
    }


    uint64_t req_num;
    uint64_t max_data_size;
    if (true == enable_trace_format_check) {
        auto if_pass = trace_sanity_check(trace_file, params, req_num, max_data_size);
        if (true == if_pass) {
            cerr << "pass sanity check" << endl;
        } else {
            throw std::runtime_error("fail sanity check");
        }
    }

    if (cache_type == "Adaptive-TinyLFU")
        return _simulation_tinylfu(trace_file, cache_type, cache_size, params);
    else if(cache_type == "HRO" || cache_type == "HROCost")
        return _simulation_hro(trace_file, cache_type, cache_size, params);
    else
        return _simulation(trace_file, cache_type, cache_size, params, req_num, max_data_size);
}
