#include "annotate.h"
#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <random>
#include <assert.h>

const uint64_t max_next_seq = 0xffffffff;
using namespace std;

 
void annotate_cost(const string &trace_file, const int &n_extra_fields, bool is_offline, int offline_num,
    int64_t delta_ratio, int64_t fixed_byte, int64_t min_ratio){
    auto expect_file = trace_file+".blc_d"+
        to_string(delta_ratio)+"_f"+to_string(fixed_byte)+"_m"+to_string(min_ratio);
    ifstream cachefile(expect_file);
    if (cachefile.good()) {
        cerr<<"cost file has been annotated, so skip annotation"<<endl;
        return;
    }
    cerr<<"annotate_cost generate file:"<<trace_file<<endl;
    string generate_cost_file =trace_file.substr(0,trace_file.rfind("/")+1)+"generate.cost_d"+
        to_string(delta_ratio)+"_f"+to_string(fixed_byte)+"_m"+to_string(min_ratio);
    cerr<<"generated :annotate_cost generate file:"<<generate_cost_file<<endl;
    uint64_t t, id, size, write_cost, read_cost, next_seq;
    vector<uint64_t> extra_features(n_extra_fields, 0);
    uint64_t i=0;
    default_random_engine _generator = default_random_engine();
    uniform_int_distribution<std::size_t> _distribution = uniform_int_distribution<std::size_t>();
    int gdsf_cost, gdsf_time, gdsf_id, gdsf_size;

    unordered_map <uint64_t, uint64_t> _map_cost;

    ifstream generateCostFile(generate_cost_file);
    if (generateCostFile.good()) {
        cerr<<"generateCostFile:"+generate_cost_file+" has generated"<<endl;
    }else{
        ofstream outGenCostfile(generate_cost_file);
        if (!outGenCostfile) {
            cerr << "Exception opening/reading tmp file: " << generate_cost_file << endl;
            exit(-1);
        }
        ifstream infile(trace_file);
        if(!infile){
            cerr << "Exception opening/reading file: " << trace_file << endl;
            exit(-1);
        }
        while(infile>> t >> id >> size) {
            for (int j = 0; j < n_extra_fields; ++j)
                infile >> extra_features[j];

            int tmp_cost=-1;
            auto iter = _map_cost.find(size);
            if(iter != _map_cost.end()){
                tmp_cost = iter->second;
            }else{
                int tmp_delta = _distribution(_generator) % delta_ratio;
                tmp_cost = (tmp_delta * fixed_byte + min_ratio * size)/1000;
                _map_cost.insert({size, tmp_cost});
            }
            outGenCostfile << tmp_cost <<endl;
            if (!(i % 1000000))
                cerr<<"cost file writing: "<<i<<endl;
            i++;
        }
        infile.close();
        outGenCostfile.close();
    }
    generateCostFile.close();



    auto timenow = chrono::system_clock::to_time_t(chrono::system_clock::now());
    auto tmp_file = trace_file + ".blc.tmp." + to_string(timenow);
    cerr<<"writing the annotated trace "<<tmp_file<<endl;
    uint64_t tmp_cost=0;

    ofstream outTracefile(tmp_file);
    if (!outTracefile) {
        cerr << "Exception opening/reading tmp file " << tmp_file << endl;
        exit(-1);
    }
    ifstream genCostFile(generate_cost_file);
    if(!genCostFile){
        cerr << "cost file open error: "<<generate_cost_file<<endl;
        exit(-1);
    }
    
    ifstream inTracefile(trace_file);
    if(!inTracefile){
        cerr << "Exception opening/reading file: " << trace_file << endl;
        exit(-1);
    }

    i=0;
    vector<int64_t> offline_nums(offline_num,0);
    if(is_offline){
        while(true) {
            for(int i=0;i<offline_num;i++){
                if(!(inTracefile>>offline_nums[i])){
                    break;
                }
            }
            if(!(inTracefile >> t >> id >> size)){
                break;
            }
            for (int j = 0; j < n_extra_fields; ++j)
                inTracefile >> extra_features[j];

            if(!(genCostFile>>tmp_cost)){
                break;
            }

            outTracefile << tmp_cost;
            for(int i=0;i<offline_num;i++){
                outTracefile<<" "<<offline_nums[i];
            }
            outTracefile <<" " << t << " " << id << " " << size;

            for (int j = 0; j < n_extra_fields; ++j)
                outTracefile << " " << extra_features[j];
            outTracefile <<endl;
            if (!(i % 1000000))
                cerr<<"writing: "<<i<<endl;
            i++;
        }
    }else{
        while(true) {
            if(!(inTracefile>> t >> id >> size)){
                break;
            }
            for (int j = 0; j < n_extra_fields; ++j)
                inTracefile >> extra_features[j];
            if(!(genCostFile>>tmp_cost)){
                break;
            }

            outTracefile << tmp_cost << " " << t << " " << id << " " << size;
            for (int j = 0; j < n_extra_fields; ++j)
                outTracefile << " " << extra_features[j];
            outTracefile <<endl;
            if (!(i % 1000000))
                cerr<<"writing: "<<i<<endl;
            i++;
        }
    }

    genCostFile.close();
    outTracefile.close();
    inTracefile.close();

    if (rename(tmp_file.c_str(), expect_file.c_str())) {
        cerr << "Exception in renaming file from " << tmp_file << " to " << expect_file << " code: " << strerror(errno)
             << endl;
        return;
    }
}


void annotate(const string &trace_file, const int &n_extra_fields, int offline_num, int offline_threshold) {
    /*
     * assume trace is less than max(uint32_t)
     * */
    auto expect_file = trace_file+"_"+to_string(offline_num)+".ant";
    ifstream cachefile(expect_file);
    if (cachefile.good()) {
        cerr<<"file has been annotated, so skip annotation"<<endl;
        return;
    }
    

    // in the first path, hold ids; in the reverse path, hold next_seq
    vector<vector<int64_t> > id_and_next_seq;
    id_and_next_seq.resize(offline_num);
    vector<uint64_t> id_seq;

    uint64_t id;
    int64_t i = 0;
    //not actually need t and size
    uint64_t t, size;
    vector<uint64_t> extra_features(n_extra_fields, 0);

    ifstream infile(trace_file);
    if (!infile) {
        cerr << "Exception opening/reading annotate original file " << trace_file << endl;
        exit(-1);
    }

    while(infile>> t >> id >> size) {
        for (int j = 0; j < n_extra_fields; ++j)
            infile >> extra_features[j];
        if (!(++i%1000000))
            cerr<<"reading origin trace: "<<i<<endl;
        id_seq.emplace_back(id);
    }

    uint64_t n_req = id_seq.size();
    std::cerr << "scanned trace n=" << n_req << std::endl;
    if (n_req > max_next_seq) {
        cerr<<"Error: don't support more trace length than "<<max_next_seq<<endl;
        abort();
    }

    for(int i=0;i<offline_num;i++){
        id_and_next_seq[i].resize(n_req);
    }
    unordered_map<uint64_t, uint32_t> last_seen;
    for (i = n_req - 1; i >= 0; --i) {
        uint64_t current_id = id_seq[i];
        auto lit = last_seen.find(current_id);
        if (lit != last_seen.end())
            id_and_next_seq[0][i] = lit->second;
        else
            id_and_next_seq[0][i] = max_next_seq;
        last_seen[current_id] = i;
        if (!(i % 1000000))
            cerr<<"computing next t: "<<i<<endl;
    }
   for(int i=0;i<n_req;i++){
        for(int j=1;j<offline_num;j++){
            if(id_and_next_seq[0][i]>n_req){
                id_and_next_seq[j][i]=-1;
            }else{
                int nex_offline_num_seq = i;
                for(int m=0;m<=j;m++){
                    nex_offline_num_seq = id_and_next_seq[0][nex_offline_num_seq]; 
                    if(nex_offline_num_seq > n_req){
                        break;
                    }
                }   
                id_and_next_seq[j][i] =nex_offline_num_seq;
                if(nex_offline_num_seq > offline_threshold +i){
                    id_and_next_seq[j][i]=-1;
                }
            }
        }
        if (!(i % 1000000))
            cerr<<"computing next offline_num t: "<<i<<endl;
   }

    string now;
    auto timenow = chrono::system_clock::to_time_t(chrono::system_clock::now());
    auto tmp_file = trace_file + ".ant.tmp." + to_string(timenow);
    cerr<<"writing the annotated trace "<<tmp_file<<endl;

    ofstream outfile(tmp_file);
    if (!outfile) {
        cerr << "Exception opening/reading tmp file " << tmp_file << endl;
        exit(-1);
    }

    infile.clear();
    infile.seekg(0, ios::beg);
    for (i = 0; i < (uint32_t) n_req; ++i) {
        infile >> t >> id >> size;
        for (int j = 0; j < n_extra_fields; ++j)
            infile >> extra_features[j];
        for(int j=0; j<offline_num;j++)
            outfile << id_and_next_seq[j][i]<<" ";
        outfile << t << " " << id << " " << size;
        for (int j = 0; j < n_extra_fields; ++j)
            outfile << " " << extra_features[j];
        outfile <<endl;
        if (!(i % 1000000))
            cerr<<"writing: "<<i<<endl;
    }

    outfile.close();

    if (rename(tmp_file.c_str(), expect_file.c_str())) {
        cerr << "Exception in renaming file from " << tmp_file << " to " << expect_file << " code: " << strerror(errno)
             << endl;
        return;
    }

}
