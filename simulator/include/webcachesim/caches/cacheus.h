#ifndef WEBCACHESIM_CACHEUS_H
#define WEBCACHESIM_CACHEUS_H

#include "cache.h"
#include <unordered_map>
#include <unordered_set>
#include "sparsepp/sparsepp/spp.h"
#include <vector>
#include <random>
#include <cmath>
#include <LightGBM/c_api.h>
#include <assert.h>
#include <fstream>
#include <list>
#include "mongocxx/client.hpp"
#include "mongocxx/uri.hpp"
#include <bsoncxx/builder/basic/document.hpp>
#include "bsoncxx/json.hpp"
#include <chrono>
#include <math.h>
#include <boost/bimap.hpp>
#include <list>
#include <queue>

#define  DOUBLE_MIN 1e-15

using namespace chrono;

using namespace webcachesim;
using namespace std;
using spp::sparse_hash_map;
using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::sub_array;

namespace cacheus { 

    class Cacheus_Entry {
public:
    Cacheus_Entry(
        const uint64_t& _id,
        const uint64_t& _size,
        const uint64_t& _freq,
        const uint64_t& _time,
        bool _is_new
    ) {
        id = _id;
        freq = _freq;
        time = _time;
        evicted_time = -1;
        is_demoted = false;
        is_new = _is_new;
        size = _size;
    }
    Cacheus_Entry() {
        id = -1;
        freq = -1;
        time = -1;
        evicted_time = -1;
        is_demoted = false;
        is_new = false;
        size = -1;
    }

    Cacheus_Entry(const Cacheus_Entry& other) {
        id = other.id;
        freq = other.freq;
        time = other.time;
        size = other.size;
        evicted_time = other.evicted_time;
        is_demoted = other.is_demoted;
        size = other.size;
    }

    Cacheus_Entry& operator=(const Cacheus_Entry& other) {
        id = other.id;
        freq = other.freq;
        time = other.time;
        size = other.size;
        evicted_time = other.evicted_time;
        is_demoted = other.is_demoted;
        size = other.size;
        return *this;
    }
    uint64_t id;
    uint64_t freq;
    uint64_t time;
    uint64_t size;
    uint64_t evicted_time;
    bool is_demoted;
    bool is_new;
};

class HeapDict {
public:
    HeapDict() {
        data_size = 0;
    }
    
    uint64_t size() {
        return data_size;
    }

    void insert(const uint64_t& _id, Cacheus_Entry cache_entry) {
        data_size += cache_entry.size;
        if (_map.find(_id) != _map.end()) {
            cerr << "HeapDict insert id" << _id << "error" << endl;
            exit(-1);
        }
        _freq_key[cache_entry.freq].push_front(_id);
        auto iter = _freq_key[cache_entry.freq].begin();
        MPair mpair = make_pair(iter, cache_entry);
        _map.insert({ _id, mpair });

    }

    Cacheus_Entry getMin() {
        auto iter = _freq_key.begin();
        uint64_t key = (iter->second).back();
        auto it_map = _map.find(key);
        if (it_map == _map.end()) {
            cerr << "HeapDict getMin error" << endl;
            exit(-1);
        }
        Cacheus_Entry cache_entry = (it_map->second).second;
        return cache_entry;
    }

    bool isin(int _id) {
        auto it = _map.find(_id);
        if (it != _map.end()) {
            return true;
        }
        else {
            return false;
        }
    }

    void erase(const uint64_t& _id) {
        auto iter = _map.find(_id);
        if (iter == _map.end()) {
            cerr << "DequeDict erase error" << endl;
            exit(-1);
        }
        data_size -= (iter->second).second.size;
        int freq = (iter->second).second.freq;
        auto it_list = _freq_key.find(freq);
        if (it_list == _freq_key.end()) {
            cerr << "DequeDict erase error" << endl;
            exit(-1);
        }
        (it_list->second).erase((iter->second).first);
        if ((it_list->second).size() == 0) {
            _freq_key.erase(it_list);
        }
        _map.erase(iter);
    }
    typedef pair<list<int>::iterator, Cacheus_Entry> MPair;
    unordered_map<int, MPair> _map;
    map<int,list<int>> _freq_key;
    uint64_t data_size;
    
};

class DequeuDict {
public:
    DequeuDict() {
        data_size = 0;
        time=0;
    }
    bool isin(int _id) {
        assert(_map.size() == list_key.size());
        if (_map.find(_id) != _map.end()) {
            return true;
        }
        else {
            return false;
        }
    }

    uint64_t size() {
        return data_size;
    }

    Cacheus_Entry get(int _id) {
        auto it = _map.find(_id);
        assert(_map.size() == list_key.size());
        if(it == _map.end()){
            cerr<<"DequeuDict get id:"<<_id<<" error"<<endl;
            exit(-1);
        }
        Cacheus_Entry cache_entry = (it->second).second;
        return cache_entry;
    }

    void insert(int _id, Cacheus_Entry cache_entry) { 
        assert(_map.size() == list_key.size());
        if(_map.find(_id) != _map.end()){
            cerr<<"DequeuDict insert id:"<<_id<<" error"<<endl;
            exit(-1);
        }

        data_size += cache_entry.size;
        list_key.push_front(_id);
        auto iter = list_key.begin();
        MPair mpair = make_pair(iter, cache_entry);
        _map.insert({_id, mpair});
    }

    Cacheus_Entry popOldest() {
        assert(_map.size() == list_key.size());
        int _id = list_key.back();
        auto it = _map.find(_id);
        Cacheus_Entry cache_entry = (it->second).second;
        list_key.pop_back();
        _map.erase(it);
        data_size -= cache_entry.size;
        return cache_entry;
    }

    Cacheus_Entry getOldest() {
        assert(_map.size() == list_key.size());
        int _id = list_key.back();
        auto it = _map.find(_id);
        Cacheus_Entry cache_entry = (it->second).second;
        return cache_entry;
    }

    void erase(int _id) {
        assert(_map.size() == list_key.size());
        auto iter = _map.find(_id);
        if (iter == _map.end()){
            cerr<<"DequeDict erase id"<<_id<<"error"<<endl;
            exit(-1);
        }

        data_size -= (iter->second).second.size;
        auto iterList = (iter->second).first;
        if(*iterList != _id){
            cerr<<"iterList error"<<endl;
            exit(-1);
        }
        _map.erase(iter);
        list_key.erase(iterList);

    }

    uint64_t time;

    typedef pair<list<int>::iterator, Cacheus_Entry> MPair;
    unordered_map<int, MPair> _map;
    list<int> list_key;
    uint64_t data_size;
};


    class Cacheus_Learning_Rate{
    public:
        Cacheus_Learning_Rate(
            const uint64_t& period_length
        ){
            learning_rate = sqrt((2.0*log(2))/period_length);

            learning_rate_reset = min(max(learning_rate,0.001),1.0);
            learning_rate_curr = learning_rate;
            learning_rate_prev = 0.0;

            period_len = period_length;

            byte_hitrate=0;
            hitrate_prev=0.0;
            hitrate_diff_prev=0.0;
            hitrate_zero_count=0;
            hitrate_nega_count=0;
            sgement_size = 0;

        }

        void update(const uint64_t& value_size){
            sgement_size += value_size;
            if(sgement_size >= period_len){
                sgement_size = 0;
                double hitrate_curr = byte_hitrate/(double)period_len;
                double hitrate_diff = hitrate_curr - hitrate_prev;

                double delta_LR = learning_rate_curr - learning_rate_prev;
                int delta = 0;
                int delta_HR = 0;
                updateInDeltaDirection(delta_LR, hitrate_diff, delta, delta_HR);

                if(delta>0){
                    learning_rate = min(
                        learning_rate+
                        fabs(learning_rate*delta_LR),1.0);
                    hitrate_nega_count = 0;
                    hitrate_zero_count = 0;
                }else if(delta<0){
                    learning_rate = max(
                        learning_rate-
                        fabs(learning_rate*delta_LR),0.001);
                    hitrate_nega_count = 0;
                    hitrate_zero_count = 0;
                }else if(delta==0 && (fabs(hitrate_diff)< DOUBLE_MIN || hitrate_diff < 0)){//hitrate_diff<=0
                    if(fabs(hitrate_diff)<DOUBLE_MIN && (fabs(hitrate_curr)<DOUBLE_MIN || hitrate_curr <0)){
                        hitrate_zero_count+=1;
                    }
                    if(hitrate_diff<0){
                        hitrate_nega_count+=1;
                        hitrate_zero_count+=1;
                    }
                    if(hitrate_zero_count>=10){
                        learning_rate = learning_rate_reset;
                        hitrate_zero_count=0;
                    }else if(hitrate_diff<0){
                        if(hitrate_nega_count>=10){
                            learning_rate = learning_rate_reset;
                            hitrate_nega_count=0;
                        }else{
                            updateInRandomDirection();
                        }
                    }
                }

                learning_rate_prev = learning_rate_curr;
                learning_rate_curr = learning_rate;
                hitrate_prev = hitrate_curr;
                hitrate_diff_prev = hitrate_diff;
                byte_hitrate = 0;
                
            }
        }

        void updateInRandomDirection()
        {
            int random = rand()%10;
            if(learning_rate>=1){
                learning_rate=0.9;
            }else if(learning_rate<=0.001){
                learning_rate = 0.005;
            }else if(random<=4){
                learning_rate = min(learning_rate*1.25,1.0);
            }else{
                learning_rate = max(learning_rate*0.75,0.001);
            }
        }

        void updateInDeltaDirection(
            const double& _learning_rate_diff,
            const double& _hitrate_diff,
            int& delta,
            int& delta_HR
        )
        {   
            double d_delta = _learning_rate_diff * _hitrate_diff;
            if(fabs(d_delta) < DOUBLE_MIN){
                delta = 0;
            }else if(d_delta > 0){
                delta = 1; 
            }else{
                delta = -1;
            }

            if(delta==0 && fabs(_learning_rate_diff) > DOUBLE_MIN){ 
                delta_HR = 0;
            }else{
                delta_HR = 1;
            }
        }

        double learning_rate;
        double learning_rate_reset;
        double learning_rate_curr;
        double learning_rate_prev;
        uint64_t period_len;

        uint64_t byte_hitrate;
        double hitrate_prev;
        double hitrate_diff_prev;
        uint64_t hitrate_zero_count;
        uint64_t hitrate_nega_count;
        uint64_t sgement_size;
    };


    class CACHEUSCache : public Cache{
    public:
        bool lookup(const SimpleRequest &req) override;

        void admit(const SimpleRequest &req) override;

        void evict();
        void miss(const uint64_t& _id, const uint64_t& size);
 
        void update_stat_periodic() override;

        void update_stat(bsoncxx::v_noabi::builder::basic::document &doc) override {
    
    
        }

        void init_with_params(const map<string, string> &params) override {
            time=0;
            mean_size = 100;

            evict_num=0;
            hitinLRUHist_num=0;
            hitinLFUHist_num=0;
            miss_num=0;

            for (auto &it: params) {
                if (it.first == "mean_size") {
                    mean_size = stoul(it.second);
                    cerr << "mean_size: " << mean_size <<endl;
                }
            }
            
            history_size = _cacheSize/2;
            initial_weight = 0.5;

            learning_rate = new Cacheus_Learning_Rate(_cacheSize);

            W.push_back(initial_weight);
            W.push_back(1-initial_weight);

            hirsRatio = 0.1;
            q_limit = (uint64_t)max(1.0, hirsRatio*_cacheSize+0.5);
            s_limit = _cacheSize - q_limit;
            q_size = 0;
            s_size = 0;
            dem_count = 0;
            nor_count = 0;
        }

        void hitinS(const uint64_t& _id){
            Cacheus_Entry x = s.get(_id);
            s.erase(_id);
            x.time = time;
            
            x.freq += 1;
            s.insert(_id, x);
            if(lfu.isin(_id)){
                lfu.erase(_id);
            }
            lfu.insert(_id, x);
        }

        void hitinQ(const uint64_t& _id){
            Cacheus_Entry x = q.get(_id);
            x.time = time;
            x.freq += 1;
            if(lfu.isin(_id)){
                lfu.erase(_id); 
            }
            lfu.insert(_id, x);
            if(x.is_demoted){
                adjustSize(true);
                x.is_demoted = false;
                dem_count-=1;
            }
            q.erase(x.id);
            q_size -= x.size;

            if(s_size>=s_limit){
                Cacheus_Entry y = s.popOldest();
                y.is_demoted = true;
                dem_count += 1;
                s_size -= y.size;
                q.insert(y.id, y);
                q_size += y.size;
            }
            s.insert(x.id, x);
            s_size += x.size;
        }

        int getChoice(){
            double _rand = rand()%100/(double)101;
            if(_rand < W[0]){
                return 0;
            }else{
                return 1;
            }
        }

        void addToS(const uint64_t& _id, const uint64_t& freq, bool isNew, const uint64_t& size){
            Cacheus_Entry x(_id, size, freq, time, isNew);
            s.insert(_id, x);
            if(lfu.isin(_id)){
                lfu.erase(_id); 
            }
            lfu.insert(_id, x);
            s_size += x.size;
        }

        void addToQ(const uint64_t& _id, const uint64_t& freq, bool isNew, const uint64_t& size){
            Cacheus_Entry x(_id, size, freq, time, isNew);
            q.insert(_id, x);
            if(lfu.isin(_id)){
                lfu.erase(_id); 
            }
            lfu.insert(_id, x);
            q_size += x.size;
        }


        void addToHistory(const Cacheus_Entry& x, int policy){
            if(policy == 0){
                if(x.is_new == true){
                    nor_count+=1;
                }
                if(lru_hist.size()>=history_size){
                    Cacheus_Entry evicted = lru_hist.getOldest();
                    lru_hist.erase(evicted.id);
                    if(evicted.is_new){
                        nor_count-=1;
                    }
                }
                lru_hist.insert(x.id,x);
            }else if(policy == 1){
                if(lfu_hist.size()>=history_size){
                    Cacheus_Entry evicted = lfu_hist.getOldest();
                    lfu_hist.erase(evicted.id);
                }
                lfu_hist.insert(x.id,x);
            }else if(policy == -1){
                return;
            }
        }


        void adjustWeights(double rewardLRU, double rewardLFU){
            W[0] = W[0] * exp(learning_rate->learning_rate*rewardLRU);
            W[1] = W[1] * exp(learning_rate->learning_rate*rewardLFU);
            W[0] = W[0] / (W[0] + W[1]);
            W[1] = W[1] / (W[0] + W[1]);

            if(W[0]>=0.99){
                W[0]=0.99;
                W[1]=0.01;
            }else if(W[1]>=0.99){
                W[0]=0.01;
                W[1]=0.99;
            }
        }

        void hitinLRUHist(const uint64_t& _id, const uint64_t& size){
            hitinLRUHist_num++;
            Cacheus_Entry entry = lru_hist.get(_id);
            entry.freq += 1;
            lru_hist.erase(_id);
            if(entry.is_new){
                nor_count-=1;
                entry.is_new=false;
                adjustSize(false);
            }
            adjustWeights(-1,0);

            addToS(entry.id,entry.freq,false,size);
            limitStack();

            while(s_size+q_size>=_cacheSize){
                evict();
            }
        }

        void hitinLFUHist(const uint64_t& _id, const uint64_t& size){
            hitinLFUHist_num++;
            Cacheus_Entry entry = lfu_hist.get(_id);
            entry.freq+=1;
            lfu_hist.erase(_id);

            adjustWeights(0,-1);

            addToS(entry.id, entry.freq, false, size);
            limitStack();

            while(s_size+q_size>=_cacheSize){
                evict();
            }
        }

        void limitStack(){
            while(s_size>=s_limit){
                Cacheus_Entry demoted = s.popOldest();
                s_size -= demoted.size;
                demoted.is_demoted = true;
                dem_count += 1;
                q.insert(demoted.id, demoted);
                q_size += demoted.size;
            }
        }



        void adjustSize(bool hit_in_Q){
            if(hit_in_Q){
                if(dem_count<1){
                    dem_count=1;
                }
                s_limit = min(
                    _cacheSize-mean_size, s_limit+max(1,(int)(nor_count/dem_count+0.5))*mean_size
                );
                q_limit = _cacheSize-s_limit;
            }else{
                if(nor_count<1){
                    nor_count=1;
                }
                q_limit=min(
                    _cacheSize-mean_size, q_limit+max(1,(int)(dem_count/nor_count+0.5))*mean_size
                );
                s_limit = _cacheSize-q_limit;
            }
        }


        ~CACHEUSCache(){
            delete learning_rate;
        }

        uint64_t time;
        uint64_t mean_size;
        DequeuDict s;
        DequeuDict q;

        HeapDict lfu;

        DequeuDict lru_hist;
        DequeuDict lfu_hist;

        uint64_t history_size;

        double initial_weight;
        Cacheus_Learning_Rate* learning_rate;
        vector<double> W;
        
        double hirsRatio;
        uint64_t q_limit;
        uint64_t s_limit;
        uint64_t q_size;
        uint64_t s_size;
        uint64_t dem_count;
        uint64_t nor_count;
        uint64_t evict_num;
        uint64_t hitinLRUHist_num;
        uint64_t hitinLFUHist_num;
        uint64_t miss_num;

};

static Factory<CACHEUSCache> factoryCACHEUS("CACHEUS");
}

#endif //WEBCACHESIM_CACHEUS_H