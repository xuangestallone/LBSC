#include "lbsc.h"
#include <algorithm>
#include "utils.h"
#include <chrono>
#include <exception>  
using namespace chrono;
using namespace std;
using namespace lbsc;


void LBSCCache::train(){
    auto timeBegin = chrono::system_clock::now();

    if(is_use_kl){
        bool is_need_train = need_train2();
        key_nums.clear();
        if(!is_need_train){
            double tmp_training_time = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now() - timeBegin).count();
            training_time  += tmp_training_time;
            total_training_time += tmp_training_time;
            return;
        }
    }

    ++n_retrain;// training time

    outlier_num_size.push_back(training_data->outlier_num);
    caches_data_size.push_back(in_cache_metas.size());


    if(booster) booster=nullptr;
    // create training dataset
    DatasetHandle trainData;
    LGBM_DatasetCreateFromCSR(
            static_cast<void *>(training_data->indptr.data()),
            C_API_DTYPE_INT32,
            training_data->indices.data(),
            static_cast<void *>(training_data->data.data()),
            C_API_DTYPE_FLOAT64,
            training_data->indptr.size(),
            training_data->data.size(),
            n_feature,  //remove future t
            training_params,
            nullptr,
            &trainData);

    LGBM_DatasetSetField(trainData,
                         "label",
                         static_cast<void *>(training_data->labels.data()),
                         training_data->labels.size(),
                         C_API_DTYPE_FLOAT32);

    LGBM_BoosterCreate(trainData, training_params, &booster);
    // train
    for (int i = 0; i < stoi(training_params["num_iterations"]); i++) {
        int isFinished;
        LGBM_BoosterUpdateOneIter(booster, &isFinished);
        if (isFinished) {
            break;
        }
    }

    int64_t len;
    vector<double> result(training_data->indptr.size() - 1);
    LGBM_BoosterPredictForCSR(booster,
                              static_cast<void *>(training_data->indptr.data()),
                              C_API_DTYPE_INT32,
                              training_data->indices.data(),
                              static_cast<void *>(training_data->data.data()),
                              C_API_DTYPE_FLOAT64,
                              training_data->indptr.size(),
                              training_data->data.size(),
                              n_feature,  //remove future t
                              C_API_PREDICT_NORMAL,
                              0,
                              training_params,
                              &len,
                              result.data());


    double se = 0;
    for (int i = 0; i < result.size(); ++i) {
        auto diff = result[i] - training_data->labels[i];
        se += diff * diff;
    }
    training_losses.push_back(se/batch_size);

    LGBM_DatasetFree(trainData);

    if(is_use_kl){
        boosters.push_back(booster);
        vector<double> tmpCurrentPars(currentPars.size());
        for(int j=0;j<currentPars.size();j++){
            tmpCurrentPars[j]=currentPars[j];
        }
        klparas.push_back(tmpCurrentPars);
    }
    have_trained=true;

    double tmp_training_time = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now() - timeBegin).count();
    training_time  += tmp_training_time;
    total_training_time += tmp_training_time;

}

double LBSCCache::calKL(int index){
    double totalKL1 = 0.0;
    double totalKL2 = 0.0;
    for(int i=1;i<=11;i++){
        double tmpLog = log1p(i);
        totalKL1 += (
            exp(currentA*tmpLog+currentB)*
                ((currentA-zipfinPara[index].first)*tmpLog+(currentB-zipfinPara[index].second))
        );
    }

    for(int i=1;i<=11;i++){
        double tmpLog = log1p(i);
        totalKL2 += (
            exp(zipfinPara[index].first*tmpLog+zipfinPara[index].second)*
                ((zipfinPara[index].first-currentA)*tmpLog+(zipfinPara[index].second-currentB))
        );
    }

    return max(totalKL1, totalKL2);
}

double LBSCCache::calJS(int index){
    double totalKL = 0.0;
    for(int i=1;i<=11;i++){
        double tmpLog = log1p(i);
        double p1 = exp(zipfinPara[index].first*tmpLog+zipfinPara[index].second);
        double p2 = exp(currentA*tmpLog+currentB);
        totalKL += 0.5*(
            p1*log1p(2*p1/(p1+p2))+
            p2*log1p(2*p2/(p1+p2))
        );
    }
    if(min_js < 0 || min_js > totalKL){
        min_js = totalKL;
    }
    if(max_js < 0 || max_js < totalKL){
        max_js = totalKL;
    }
    return totalKL;
}

double LBSCCache::calJS2(int index){

    double totalKL = 0.0;
    for(int i=0;i<kl_sample_num;i++){
        totalKL += 0.5*(
            klparas[index][i]*log1p(2*klparas[index][i]/(klparas[index][i]+currentPars[i]))+
            currentPars[i]*log1p(2*currentPars[i]/(klparas[index][i]+currentPars[i]))
        );
    }
    totalKL*=10;
    if(min_js < 0 || min_js > totalKL){
        min_js = totalKL;
    }
    if(max_js < 0 || max_js < totalKL){
        max_js = totalKL;
    }
    return totalKL;
}

bool LBSCCache::need_train3(){
    if(key_nums.size()==0){
        return true;
    }
    vector<uint32_t> to_sort_data(key_nums.size());
    uint index=0;
    uint32_t total_num=0;
    uint32_t num1s=0;
    uint32_t num1_num=0;
    for(auto it = key_nums.begin(); it!=key_nums.end();it++){
        to_sort_data[index++]=it->second;
        //for test
        if(it->second < 0){
            exit(-1);
        }
        total_num+=it->second;

        if(it->second==1){
            num1_num++;
        }
    }
    sort(to_sort_data.begin(),to_sort_data.end(),
        [&](const uint32_t &a, const uint32_t &b){
            return (a > b);
        }
    );
    for(int i=0;i<kl_sample_num;i++){
        currentPars[i]=to_sort_data[i*5]/(double)total_num;
    }
    if(boosters.size()==0 || booster==nullptr){
        return true;
    }
    if(min_vec_kl_size == 0 || min_vec_kl_size > boosters.size()){
        min_vec_kl_size = boosters.size();
    }
    if(max_vec_kl_size == 0 || max_vec_kl_size < boosters.size()){
        max_vec_kl_size = boosters.size();
    }

    double minKL = kl_threshold+100;
    index=0;
    for(int i=0;i<boosters.size();i++){
        double tmpJS = calJS2(i);
        if(minKL>tmpJS){
            minKL=tmpJS;
            index=i;
        }
    }
    if(min_kl_threshold<0 || min_kl_threshold>minKL){
        min_kl_threshold=minKL;
    }
    if(max_kl_threshold<0 || max_kl_threshold<minKL){
        max_kl_threshold=minKL;
    }

    if(minKL <= kl_threshold){
        booster = boosters[index];
        return false;
    }else{
        return true;
    }

}


bool LBSCCache::need_train2(){
    if(key_nums.size()==0){
        return true;
    }
    vector<uint32_t> to_sort_data(key_nums.size());
    uint index=0;
    uint32_t total_num=0;
    uint32_t num1s=0;
    uint32_t num1_num=0;
    for(auto it = key_nums.begin(); it!=key_nums.end();it++){
        to_sort_data[index++]=it->second;
        //for test
        if(it->second < 0){
            exit(-1);
        }
        total_num+=it->second;

        if(it->second==1){
            num1_num++;
        }
    }
    sort(to_sort_data.begin(),to_sort_data.end(),
        [&](const uint32_t &a, const uint32_t &b){
            return (a > b);
        }
    );
    for(int i=0;i<kl_sample_num;i++){
        currentPars[i]=to_sort_data[i]/(double)total_num;
    }
    if(boosters.size()==0 || booster==nullptr){
        return true;
    }
    if(min_vec_kl_size == 0 || min_vec_kl_size > boosters.size()){
        min_vec_kl_size = boosters.size();
    }
    if(max_vec_kl_size == 0 || max_vec_kl_size < boosters.size()){
        max_vec_kl_size = boosters.size();
    }

    double minKL = kl_threshold+100;
    index=0;
    for(int i=0;i<boosters.size();i++){
        double tmpJS = calJS2(i);
        if(minKL>tmpJS){
            minKL=tmpJS;
            index=i;
        }
    }
    if(min_kl_threshold<0 || min_kl_threshold>minKL){
        min_kl_threshold=minKL;
    }
    if(max_kl_threshold<0 || max_kl_threshold<minKL){
        max_kl_threshold=minKL;
    }

    if(minKL <= kl_threshold){
        booster = boosters[index];
        return false;
    }else{
        return true;
    }

}



bool LBSCCache::need_train(){
    if(key_nums.size()==0){
        return true;
    }
    const size_t key_num_size = key_nums.size();
    Eigen::MatrixXf A(key_num_size,2);
    Eigen::VectorXf b(key_num_size);
    vector<uint32_t> to_sort_data(key_nums.size());
    uint index=0;
    uint32_t total_num=0;
    uint32_t num1s=0;
    uint32_t num1_num=0;
    for(auto it = key_nums.begin(); it!=key_nums.end();it++){
        to_sort_data[index++]=it->second;
        if(it->second < 0){
            cout<<"key<0: "+it->first<<"  value:"<<it->second<<endl;
            exit(-1);
        }
        total_num+=it->second;

        if(it->second==1){
            num1_num++;
        }
    }
    sort(to_sort_data.begin(),to_sort_data.end(),
        [&](const uint32_t &a, const uint32_t &b){
            return (a > b);
        }
    );
    for(int i=1;i<=key_num_size;i++){
        A(i-1,0)=log1p(i);
        A(i-1,1)=1;
        b(i-1)=log1p(to_sort_data[i-1]/(double)total_num);
    }
    Eigen::VectorXf x= A.colPivHouseholderQr().solve(b);
    currentA = x[0];
    currentB = x[1];
    if(boosters.size()==0 || booster==nullptr){
        return true;
    }
    if(min_vec_kl_size == 0 || min_vec_kl_size > boosters.size()){
        min_vec_kl_size = boosters.size();
    }
    if(max_vec_kl_size == 0 || max_vec_kl_size < boosters.size()){
        max_vec_kl_size = boosters.size();
    }

    double minKL = kl_threshold+100;
    index=0;
    for(int i=0;i<boosters.size();i++){
        if(!is_use_js){
            double tmpKL = calKL(i);
            if(minKL>tmpKL){
                minKL = tmpKL;
                index=i;
            }
        }else{
            double tmpJS = calJS(i);
            if(minKL>tmpJS){
                minKL=tmpJS;
                index=i;
            }
        }
    }
    if(min_kl_threshold<0 || min_kl_threshold>minKL){
        min_kl_threshold=minKL;
    }
    if(max_kl_threshold<0 || max_kl_threshold<minKL){
        max_kl_threshold=minKL;
    }

    if(minKL <= kl_threshold){
        booster = boosters[index];
        return false;
    }else{
        return true;
    }
}


void LBSCCache::admit(const SimpleRequest &req){

    if (req.size > _cacheSize) {
        LOG("L", _cacheSize, req.id, req.size);
        return;
    }
    auto it = key_pos.find(req.id);

    if(it != key_pos.end()){ 
        auto keyList = it->second;
        assert(keyList.list_idx==1);
        auto _meta = out_cache_metas[keyList.list_pos];
        assert(_meta->_key == req.id);
        if(is_bypss_cold_data && (current_seq - _meta->_past_timestamp >= memory_window)){
            bypass_num++;
            _meta->update(current_seq, _reqsMap[req.id]);
            return;
        }else{
            if(start_collect_train_data){
                uint32_t _distance = current_seq - _meta->_past_timestamp;
                if(_distance > memory_window){
                    _distance = 2 *memory_window;
                }
                training_data->emplace_back(_meta, _distance, _meta->_key);
                // if(is_use_kl){
                    auto knIter = key_nums.find(_meta->_key);
                    if(knIter!=key_nums.end()){
                        knIter->second = _meta->metaSize();
                    }else{
                        key_nums.insert({_meta->_key, _meta->metaSize()});
                    }
                // }

                if (training_data->labels.size() >= batch_size) {
                    train();
                    training_data->clear();
                }
            }
            _meta->update(current_seq, _reqsMap[req.id]);
            _currentSize += req.size;

            uint32_t tail0_pos = in_cache_metas.size();
            in_cache_metas.emplace_back(_meta);
            uint32_t tail1_pos = out_cache_metas.size() - 1;
            if (keyList.list_pos != tail1_pos) {
                //swap tail
                out_cache_metas[keyList.list_pos] = out_cache_metas[tail1_pos];
                key_pos.find(out_cache_metas[tail1_pos]->_key)->second.list_pos = keyList.list_pos;
            }
            out_cache_metas.pop_back();
            it->second = {0, tail0_pos};

            double rate_ = req.cost/(double)req.size;
            if(max_size_cost_rate<0 || max_size_cost_rate<rate_){
                max_size_cost_rate = rate_;
            }
            if(min_size_cost_rate<0 || min_size_cost_rate>rate_){
                min_size_cost_rate = rate_;
            }

            assert(_meta->_key == req.id);

            if(rate_ >cost_size_threshol){
                key_pos_thresholds.insert({req.id, {1,(unsigned int)in_cache_metas_threshold_up.size()}});
                in_cache_metas_threshold_up.emplace_back(_meta);
            }else{
                key_pos_thresholds.insert({req.id, {0,(unsigned int)in_cache_metas_threshold_low.size()}});
                in_cache_metas_threshold_low.emplace_back(_meta);
            }
        }
        _sizemap[req.id] = req.size;
        long double ageVal = ageValue(req);
        _cacheMap[req.id] = _valueMap.emplace(ageVal, pair<uint64_t, shared_ptr<Meta>>(req.id,_meta));

    }else{
        auto _meta = make_shared<Meta>(req.id, req.size, req.cost, current_seq, req.extra_features);
        if(is_bypss_cold_data && current_seq >= memory_window){
            bypass_num++;
            key_pos.insert({req.id, {1, (uint32_t) out_cache_metas.size()}});
            out_cache_metas.emplace_back(_meta);

            return;
        }else{
            if(start_collect_train_data){
                uint32_t _distance = 2 * memory_window;
                training_data->emplace_back(_meta, _distance, _meta->_key);
                // if(is_use_kl){
                    auto knIter = key_nums.find(_meta->_key);
                    if(knIter!=key_nums.end()){
                        knIter->second = _meta->metaSize();
                    }else{
                        key_nums.insert({_meta->_key, _meta->metaSize()});
                    }
                // }
                if (training_data->labels.size() >= batch_size) {
                    train();
                    training_data->clear();
                }
            }

            _currentSize += req.size;
            key_pos.insert({req.id, {0, (uint32_t) in_cache_metas.size()}});
            in_cache_metas.emplace_back(_meta);

            double rate_ = req.cost/(double)req.size;
            if(max_size_cost_rate<0 || max_size_cost_rate<rate_){
                max_size_cost_rate = rate_;
            }
            if(min_size_cost_rate<0 || min_size_cost_rate>rate_){
                min_size_cost_rate = rate_;
            }

            if(rate_ >cost_size_threshol){
                key_pos_thresholds.insert({req.id, {1,(unsigned int)in_cache_metas_threshold_up.size()}});
                in_cache_metas_threshold_up.emplace_back(_meta);
            }else{
                key_pos_thresholds.insert({req.id, {0,(unsigned int)in_cache_metas_threshold_low.size()}});
                in_cache_metas_threshold_low.emplace_back(_meta);
            }
        }

        _sizemap[req.id] = req.size;
        long double ageVal = ageValue(req);
        long double ageValRank = ageValueRank(req);
        _cacheMap[req.id] = _valueMap.emplace(ageVal, pair<uint64_t, shared_ptr<Meta>>(req.id,_meta));
    }
    

    if (_currentSize > _cacheSize){
        eviction_num++;
        tmp_evict_num++;
        evict(req);
        if(is_use_decay){
            key_sets.clear();  
        }

        if(is_dynamic_cost_size_threshold){
            if(tmp_start_sample_num>start_sample_threshold){
                start_sample=false;
            }
            if(is_change_threshold){
                cost_size_threshol+=0.01;
                move_to_low();
            }

            is_change_threshold=false;
            if(tmp_evict_num>evict_num_threshold){
                tmp_evict_num=0;
            }
        }
    }

    if(_currentSize > _cacheSize){
        cerr<<"cache is still full error"<<endl;
        exit(-1);
    }

}






void LBSCCache::evict(const SimpleRequest &req){
   if(have_trained){

        if(is_cost_size_sample){ 
            double cost_size_ratio =  in_cache_metas_threshold_low.size() /(double)in_cache_metas.size();
            if(min_cost_size_ratio<0 || min_cost_size_ratio>cost_size_ratio){
                min_cost_size_ratio = cost_size_ratio;
            }
            if(max_cost_size_ratio<0 || max_cost_size_ratio< cost_size_ratio){
                max_cost_size_ratio = cost_size_ratio;
            }
        }

        while(_currentSize > _cacheSize){
            pair<uint64_t, KeyMapEntryT> epair;
            epair = rank();
            uint64_t &key = epair.first;
            KeyMapEntryT &old_pos = epair.second;

            shared_ptr<Meta> meta;
            if(is_cost_size_sample){
                if(old_pos.list_idx==0){
                    if(is_dynamic_cost_size_threshold && start_sample){
                        tmp_start_sample_num++;
                    }
                    meta = in_cache_metas_threshold_low[old_pos.list_pos];
                }else{
                    if(is_dynamic_cost_size_threshold && start_sample && tmp_start_sample_num>0){
                        tmp_start_sample_num=0;
                    }
                    meta = in_cache_metas_threshold_up[old_pos.list_pos];
                    double rate_ = meta->_cost/(double)meta->_size;
                    if(is_dynamic_cost_size_threshold && (rate_>cost_size_threshol)){
                        is_change_threshold=true;
                    }
                }
                
            }else{
                meta = in_cache_metas[old_pos.list_pos];
            } 
            assert(meta->_key == key);

            double evict_rate_  = meta->_cost /(double)meta->_size;
            if(evict_max_size_cost_rate < 0 || evict_max_size_cost_rate < evict_rate_){
                evict_max_size_cost_rate = evict_rate_;
            }
            if(evict_min_size_cost_rate < 0 || evict_min_size_cost_rate > evict_rate_){
                evict_min_size_cost_rate = evict_rate_;
            }

            uint32_t last_distance = current_seq - meta->_past_timestamp;
            if(evict_max_last_distance < last_distance){
                evict_max_last_distance = last_distance;
            }
            if(evict_min_last_distance > last_distance){
                evict_min_last_distance = last_distance;
            }

            if(is_cost_size_sample){

                auto itKeyList = key_pos.find(key);
                assert(itKeyList != key_pos.end());
                uint32_t old_key_pos = itKeyList->second.list_pos;
                assert(itKeyList->second.list_idx == 0);


                if (memory_window > last_distance) {
                    uint32_t new_pos = out_cache_metas.size();
                    out_cache_metas.emplace_back(meta);
                    uint32_t activate_tail_idx = in_cache_metas.size() - 1;
                    if (old_key_pos != activate_tail_idx) {
                        in_cache_metas[old_key_pos] = in_cache_metas[activate_tail_idx];
                        key_pos.find(in_cache_metas[activate_tail_idx]->_key)->second.list_pos = old_key_pos;
                    }
                    in_cache_metas.pop_back();
                    key_pos.find(key)->second = {1, new_pos};
                }else{
                    key_pos.erase(key);

                    uint32_t activate_tail_idx = in_cache_metas.size() - 1;
                    if (old_key_pos != activate_tail_idx) {
                        in_cache_metas[old_key_pos] = in_cache_metas[activate_tail_idx];
                        key_pos.find(in_cache_metas[activate_tail_idx]->_key)->second.list_pos = old_key_pos;
                    }
                    in_cache_metas.pop_back();

                }

                if(old_pos.list_idx==0){
                    uint32_t a_tail_idx = in_cache_metas_threshold_low.size() - 1;
                    assert(in_cache_metas_threshold_low[old_pos.list_pos]->_key == key);
                    if(old_pos.list_pos != a_tail_idx){
                        in_cache_metas_threshold_low[old_pos.list_pos] = in_cache_metas_threshold_low[a_tail_idx];
                        auto itLow = key_pos_thresholds.find(in_cache_metas_threshold_low[a_tail_idx]->_key);
                        assert(itLow != key_pos_thresholds.end());
                        itLow->second.list_pos = old_pos.list_pos;
                    }
                    in_cache_metas_threshold_low.pop_back();
                }else{
                    uint32_t a_tail_idx = in_cache_metas_threshold_up.size() - 1;
                    assert(in_cache_metas_threshold_up[old_pos.list_pos]->_key == key);
                    if(old_pos.list_pos != a_tail_idx){
                        in_cache_metas_threshold_up[old_pos.list_pos] = in_cache_metas_threshold_up[a_tail_idx];
                        auto itLow = key_pos_thresholds.find(in_cache_metas_threshold_up[a_tail_idx]->_key);
                        assert(itLow != key_pos_thresholds.end());
                        itLow->second.list_pos = old_pos.list_pos;
                    }
                    in_cache_metas_threshold_up.pop_back();
                }
                key_pos_thresholds.erase(key);
            }else{
                if (memory_window > last_distance) { 
                    uint32_t new_pos = out_cache_metas.size();
                    out_cache_metas.emplace_back(meta);
                    uint32_t activate_tail_idx = in_cache_metas.size() - 1;
                    if (old_pos.list_pos != activate_tail_idx) {
                        in_cache_metas[old_pos.list_pos] = in_cache_metas[activate_tail_idx];
                        key_pos.find(in_cache_metas[activate_tail_idx]->_key)->second.list_pos = old_pos.list_pos;
                    }
                    in_cache_metas.pop_back();
                    key_pos.find(key)->second = {1, new_pos};
                }else{
                    key_pos.erase(key);

                    uint32_t activate_tail_idx = in_cache_metas.size() - 1;
                    if (old_pos.list_pos != activate_tail_idx) {
                        in_cache_metas[old_pos.list_pos] = in_cache_metas[activate_tail_idx];
                        key_pos.find(in_cache_metas[activate_tail_idx]->_key)->second.list_pos = old_pos.list_pos;
                    }
                    in_cache_metas.pop_back();

                }
                auto itKeyList = key_pos_thresholds.find(key);
                assert(itKeyList != key_pos_thresholds.end());
                uint32_t old_threshold_pos = itKeyList->second.list_pos;
                if(itKeyList->second.list_idx == 1){
                    uint32_t a_tail_idx = in_cache_metas_threshold_up.size() - 1;
                    if(old_threshold_pos != a_tail_idx){
                        in_cache_metas_threshold_up[old_threshold_pos] = in_cache_metas_threshold_up[a_tail_idx];
                        auto itUp = key_pos_thresholds.find(in_cache_metas_threshold_up[a_tail_idx]->_key);
                        assert(itUp != key_pos_thresholds.end());
                        itUp->second.list_pos = old_threshold_pos;
                    }
                    in_cache_metas_threshold_up.pop_back();
                }else{
                    uint32_t a_tail_idx = in_cache_metas_threshold_low.size() - 1;
                    if(old_threshold_pos != a_tail_idx){
                        in_cache_metas_threshold_low[old_threshold_pos] = in_cache_metas_threshold_low[a_tail_idx];
                        auto itLow = key_pos_thresholds.find(in_cache_metas_threshold_low[a_tail_idx]->_key);
                        assert(itLow != key_pos_thresholds.end());
                        itLow->second.list_pos = old_threshold_pos;
                    }
                    in_cache_metas_threshold_low.pop_back();
                }
                key_pos_thresholds.erase(key);

            }

            _currentSize-=meta->_size;
            assert(meta->_size == _sizemap[key]);

            auto itDel = _cacheMap.find(key);
            assert(itDel != _cacheMap.end());

            ValueMapIteratorType lit = itDel->second;
            _currentL = lit->first;
            _valueMap.erase(lit);
            _cacheMap.erase(key);

            _sizemap.erase(key);
        }
   }

    while(_currentSize > _cacheSize){
        assert(_valueMap.size() == in_cache_metas.size());
        if (_valueMap.size() > 0) {
            ValueMapIteratorType lit  = _valueMap.begin();
            assert(lit != _valueMap.end());

            uint64_t toDelObj = lit->second.first;

            auto size = _sizemap[toDelObj];
            _currentSize -= size;
            _cacheMap.erase(toDelObj);
            _sizemap.erase(toDelObj);
            _currentL = lit->first;
            _valueMap.erase(lit);

            auto delit = key_pos.find(toDelObj);
            assert(delit != key_pos.end());
            assert(delit->second.list_idx == 0);// in_cache
            uint32_t old_pos = delit->second.list_pos;
            auto meta = in_cache_metas[old_pos];
            if (memory_window > (current_seq - meta->_past_timestamp)) { 
                uint32_t new_pos = out_cache_metas.size();
                out_cache_metas.emplace_back(in_cache_metas[old_pos]);
                uint32_t activate_tail_idx = in_cache_metas.size() - 1;
                if (old_pos != activate_tail_idx) {
                    //move tail
                    in_cache_metas[old_pos] = in_cache_metas[activate_tail_idx];
                    key_pos.find(in_cache_metas[activate_tail_idx]->_key)->second.list_pos = old_pos;
                }
                in_cache_metas.pop_back();
                key_pos.find(toDelObj)->second = {1, new_pos};
            }else{
                key_pos.erase(toDelObj);

                uint32_t activate_tail_idx = in_cache_metas.size() - 1;
                if (old_pos != activate_tail_idx) {
                    //move tail
                    in_cache_metas[old_pos] = in_cache_metas[activate_tail_idx];
                    key_pos.find(in_cache_metas[activate_tail_idx]->_key)->second.list_pos = old_pos;
                }
                in_cache_metas.pop_back();

            }
            auto itKeyList = key_pos_thresholds.find(toDelObj);
            assert(itKeyList != key_pos_thresholds.end());
            uint32_t old_threshold_pos = itKeyList->second.list_pos;
            if(itKeyList->second.list_idx == 1){
                uint32_t a_tail_idx = in_cache_metas_threshold_up.size() - 1;
                if(old_threshold_pos != a_tail_idx){
                    in_cache_metas_threshold_up[old_threshold_pos] = in_cache_metas_threshold_up[a_tail_idx];
                    auto itUp = key_pos_thresholds.find(in_cache_metas_threshold_up[a_tail_idx]->_key);
                    assert(itUp != key_pos_thresholds.end());
                    itUp->second.list_pos = old_threshold_pos;
                }
                in_cache_metas_threshold_up.pop_back();
            }else{
                uint32_t a_tail_idx = in_cache_metas_threshold_low.size() - 1;
                if(old_threshold_pos != a_tail_idx){
                    in_cache_metas_threshold_low[old_threshold_pos] = in_cache_metas_threshold_low[a_tail_idx];
                    auto itLow = key_pos_thresholds.find(in_cache_metas_threshold_low[a_tail_idx]->_key);
                    assert(itLow != key_pos_thresholds.end());
                    itLow->second.list_pos = old_threshold_pos;
                }
                in_cache_metas_threshold_low.pop_back();
            }
            key_pos_thresholds.erase(toDelObj);
        }
    }
}

void LBSCCache::sample_move_to_low(){
    unordered_set<uint64_t> key_set;
    unsigned int idx_row = 0;
    uint32_t pos=0;

    while (idx_row != sample_rate_to_low) {
        pos = _distribution(_generator) % in_cache_metas_threshold_up.size();
        shared_ptr<Meta> meta = in_cache_metas_threshold_up[pos]; 
        if (key_set.find(meta->_key) != key_set.end()) {
            continue;
        } else {
            key_set.insert(meta->_key);
        }

        double rate_ = meta->_cost/(double)meta->_size;
        if(rate_<cost_size_threshol){
            //should_move_to_low.insert(meta->_key);
            uint32_t a_tail_idx = in_cache_metas_threshold_up.size() - 1;
            uint64_t move_key = meta->_key;

            in_cache_metas_threshold_low.emplace_back(meta);
            if(pos != a_tail_idx){
                in_cache_metas_threshold_up[pos] = in_cache_metas_threshold_up[a_tail_idx];
                auto itLow = key_pos_thresholds.find(in_cache_metas_threshold_up[a_tail_idx]->_key);
                assert(itLow != key_pos_thresholds.end());
                itLow->second.list_pos = pos;
            }
            in_cache_metas_threshold_up.pop_back();
            key_pos_thresholds.find(move_key)->second={0,(unsigned int)in_cache_metas_threshold_low.size()-1};

        }
        idx_row++;
    }

}


void LBSCCache::move_to_low(){
    unsigned int idx_row = 0;

    while (idx_row < in_cache_metas_threshold_up.size()) {
        shared_ptr<Meta> meta = in_cache_metas_threshold_up[idx_row]; 
        double rate_ = meta->_cost/(double)meta->_size;
        if(rate_<cost_size_threshol){
            uint32_t a_tail_idx = in_cache_metas_threshold_up.size() - 1;
            uint64_t move_key = meta->_key;

            in_cache_metas_threshold_low.emplace_back(meta);
            if(idx_row != a_tail_idx){
                in_cache_metas_threshold_up[idx_row] = in_cache_metas_threshold_up[a_tail_idx];
                auto itLow = key_pos_thresholds.find(in_cache_metas_threshold_up[a_tail_idx]->_key);
                assert(itLow != key_pos_thresholds.end());
                itLow->second.list_pos = idx_row;
            }
            in_cache_metas_threshold_up.pop_back();
            key_pos_thresholds.find(move_key)->second={0,(unsigned int)in_cache_metas_threshold_low.size()-1};

        }else{
            idx_row++;
        }
    }

}


pair<uint64_t, KeyMapEntryT> LBSCCache::rank(){
    system_clock::time_point addFeatureTimeBegin = chrono::system_clock::now();
    uint32_t tmp_sample_rate = sample_rate;
    uint32_t tmp_sample_rate_low=0;
    if(is_cost_size_sample){
        if(is_dynamic_cost_size_threshold && (start_sample || tmp_evict_num > evict_num_threshold)){
            tmp_sample_rate_low = in_cache_metas_threshold_low.size()*sample_rate/in_cache_metas.size() + 1;
            if(in_cache_metas_threshold_low.size()==0){
                 tmp_sample_rate_low=0;
            }
        }else{
            if(in_cache_metas_threshold_low.size()==0){
                cerr<<"error: in_cache_metas_threshold_low has no data"<<endl;
                exit(-1);
            }
            tmp_sample_rate = in_cache_metas_threshold_low.size()*sample_rate/in_cache_metas.size() + 1;
        }
    }

    if(min_sample_rate==sample_rate || min_sample_rate > tmp_sample_rate){
        min_sample_rate = tmp_sample_rate;
    }
    if(max_sample_rate==0 || max_sample_rate < tmp_sample_rate){
        max_sample_rate = tmp_sample_rate;
    }

    int32_t indptr[tmp_sample_rate + 1];
    indptr[0] = 0;
    int32_t indices[tmp_sample_rate * n_feature];
    double data[tmp_sample_rate * n_feature];
    int32_t past_timestamps[tmp_sample_rate];
    uint32_t sizes[tmp_sample_rate];
    uint32_t costs[tmp_sample_rate];
    KeyMapEntryT poses[tmp_sample_rate];

    unordered_set<uint64_t> key_set;
    uint64_t keys[tmp_sample_rate];

    unsigned int idx_feature = 0;
    unsigned int idx_row = 0;

    while (idx_row != tmp_sample_rate) {
        sample_num++;
        KeyMapEntryT pos;
        shared_ptr<Meta> meta;

        if(is_cost_size_sample){
            if(is_dynamic_cost_size_threshold){
                if(start_sample || tmp_evict_num > evict_num_threshold){
                    if(tmp_sample_rate_low==0){
                        uint32_t posTmp = _distribution(_generator) % in_cache_metas_threshold_up.size();
                        pos.list_idx=1;
                        pos.list_pos = posTmp;
                        meta = in_cache_metas_threshold_up[posTmp]; 

                        double rate_ = meta->_cost/(double)meta->_size;
                        auto keyMap = key_pos_thresholds.find(meta->_key)->second;
                        if(keyMap.list_idx==0){
                            cerr<<"error   aaa"<<endl;
                            exit(-1);
                        }

                    }else{
                        uint32_t posTmp = _distribution(_generator) % in_cache_metas_threshold_low.size();
                        pos.list_idx=0;
                        pos.list_pos = posTmp;
                        meta = in_cache_metas_threshold_low[posTmp];
                        tmp_sample_rate_low--;
                    }
                }else{
                    uint32_t posTmp = _distribution(_generator) % in_cache_metas_threshold_low.size();
                    pos.list_idx=0;
                    pos.list_pos = posTmp;
                    meta = in_cache_metas_threshold_low[posTmp];
                }

            }else{
                uint32_t posTmp = _distribution(_generator) % in_cache_metas_threshold_low.size();
                pos.list_idx=0;
                pos.list_pos = posTmp;
                meta = in_cache_metas_threshold_low[posTmp];
            }

        }else{
            uint32_t posTmp = _distribution(_generator) % in_cache_metas.size();
            pos.list_idx=0;
            pos.list_pos = posTmp;
            meta = in_cache_metas[posTmp];
        }

        if(is_use_decay){
            if (key_sets.find(meta->_key) != key_sets.end()) {
                continue;
            } else {
                key_sets.insert({meta->_key,0});
            }

        }else{
            if (key_set.find(meta->_key) != key_set.end()) {
                continue;
            } else {
                key_set.insert(meta->_key);
            }
        }
    
        
        keys[idx_row] = meta->_key;
        poses[idx_row] = pos;
        indices[idx_feature] = 0;
        data[idx_feature++] = current_seq - meta->_past_timestamp;
        past_timestamps[idx_row] = meta->_past_timestamp;


        uint8_t j = 0;
        uint32_t this_past_distance = 0;
        uint8_t n_within = 0;
        if (meta->_extra) {
            for (j = 0; j < meta->_extra->_past_distance_idx && j < max_n_past_distances; ++j) {
                uint8_t past_distance_idx = (meta->_extra->_past_distance_idx - 1 - j) % max_n_past_distances;
                uint32_t &past_distance = meta->_extra->_past_distances[past_distance_idx];
                this_past_distance += past_distance;
                indices[idx_feature] = j + 1;
                data[idx_feature++] = past_distance;
                if (this_past_distance < memory_window) {
                    ++n_within;
                }
            }
        }

        indices[idx_feature] = max_n_past_timestamps;
        data[idx_feature++] = meta->_size;
        sizes[idx_row] = meta->_size;
        costs[idx_row] = meta->_cost;

        for (uint k = 0; k < n_extra_fields; ++k) {
            indices[idx_feature] = max_n_past_timestamps + k + 1;
            data[idx_feature++] = meta->_extra_features[k];
        }

        indices[idx_feature] = max_n_past_timestamps + n_extra_fields + 1;
        data[idx_feature++] = n_within;

        for (uint8_t k = 0; k < n_edc_feature; ++k) {
            indices[idx_feature] = max_n_past_timestamps + n_extra_fields + 2 + k;
            uint32_t _distance_idx = min(uint32_t(current_seq - meta->_past_timestamp) / edc_windows[k],
                                         max_hash_edc_idx);
            if (meta->_extra)
                data[idx_feature++] = meta->_extra->_edc[k] * hash_edc[_distance_idx];
            else
                data[idx_feature++] = hash_edc[_distance_idx];
        }

        if(add_new_features){
            indices[idx_feature] = max_n_past_distances + n_extra_fields+n_edc_feature + 2;
            data[idx_feature++] = static_cast<double>(meta->_cost)/static_cast<double>(meta->_size);

            indices[idx_feature] = max_n_past_distances + n_extra_fields + n_edc_feature+ 3;
            data[idx_feature++] = static_cast<double>(meta->_freq);
        }
        indptr[++idx_row] = idx_feature;
    }

    int64_t len;
    double scores[tmp_sample_rate];
    system_clock::time_point timeBegin = chrono::system_clock::now();


    LGBM_BoosterPredictForCSR(booster,
                              static_cast<void *>(indptr),
                              C_API_DTYPE_INT32,
                              indices,
                              static_cast<void *>(data),
                              C_API_DTYPE_FLOAT64,
                              idx_row + 1,
                              idx_feature,
                              n_feature,  //remove future t
                              C_API_PREDICT_NORMAL,
                              0,
                              inference_params,
                              &len,
                              scores);
    n_inference++;

    double tmp_inference_time = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now() - timeBegin).count();
    double tmp_add_featrue_inference_time = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now() - addFeatureTimeBegin).count();
    total_inference_time += tmp_inference_time;
    inference_time += tmp_inference_time;
    inference_add_feature_time  += tmp_add_featrue_inference_time;
    total_inference_add_feature_time += tmp_add_featrue_inference_time;

    system_clock::time_point timeSortBegin = chrono::system_clock::now();

    for (uint32_t i=0; i<tmp_sample_rate; ++i){
        scores[i] = scores[i] + log1p(sizes[i]) - log1p(costs[i]);
        if(is_use_decay){
            key_sets[keys[i]]=scores[i];
        }
    }

    if(is_use_decay){
        vector<pair<double, uint64_t>> vec_score_key(key_sets.size());
        int index=0;
        for(auto it=key_sets.begin(); it != key_sets.end(); it++){
            vec_score_key[index++] = make_pair(it->second, it->first); 
        }
        sort(vec_score_key.begin(),vec_score_key.end(),
            [&](const pair<double, uint64_t> &a, const pair<double, uint64_t> &b){
                return (a.first > b.first);
            }
        );
        KeyMapEntryT ret_pos;
        if(is_cost_size_sample){
            ret_pos = key_pos_thresholds[vec_score_key[0].second];
        }else{
            ret_pos = key_pos[vec_score_key[0].second];
        }
        return {vec_score_key[0].second, ret_pos};

    }else{
        vector<int> index(tmp_sample_rate, 0);
        for (int i = 0; i < index.size(); ++i) {
            index[i] = i;
        }
        sort(index.begin(), index.end(),
            [&](const int &a, const int &b) {
                return (scores[a] > scores[b]);
            }
        );
        double tmp_inference_sort_time = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now()-timeSortBegin).count();
        inferenct_sort_time += tmp_inference_sort_time;
        total_inference_sort_time+=tmp_inference_sort_time;

        return {keys[index[0]], poses[index[0]]};
    }

}


bool LBSCCache::_lookup(const SimpleRequest &req)
{
    auto & obj = req.id;
    auto it = _cacheMap.find(obj);
    if (it != _cacheMap.end()) {
        // log hit
        LOG("h", 0, obj.id, obj.size);
        hit(req);
        return true;
    }
    return false;
}

void LBSCCache::hit(const SimpleRequest& req)
{
    auto & obj = req.id;
    auto it = _cacheMap.find(obj);
    assert(it != _cacheMap.end());
    uint64_t cachedObj = it->first;
    ValueMapIteratorType si = it->second;
    _valueMap.erase(si);
    long double hval = ageValue(req);
    auto meta = si->second.second;
    it->second = _valueMap.emplace(hval, pair<uint64_t,shared_ptr<Meta>>(cachedObj,meta));
}

long double LBSCCache::ageValue(const SimpleRequest& req)
{
    auto & obj = req.id;
    uint64_t & size = _sizemap[obj];//size
    assert(_sizemap.find(obj) != _sizemap.end());
    return _currentL + static_cast<double>(_reqsMap[obj]*req.cost) / static_cast<double>(size);
}

long double LBSCCache::ageValueRank(const SimpleRequest& req)
{
    auto & obj = req.id;
    uint64_t & size = _sizemap[obj];//size
    assert(_sizemap.find(obj) != _sizemap.end());
    return static_cast<double>(req.cost) / static_cast<double>(size);
}


bool LBSCCache::lookup(const SimpleRequest &req) {

    const uint64_t &size = req.size;
    const uint64_t &cost = req.cost;
    auto & obj = req.id;
    ++current_seq;

    if(current_seq > memory_window){ 
        start_collect_train_data = true;
    }
    bool hit = _lookup(req);
    if (!hit) {
        _reqsMap[obj] = 1;
    } else {
        _reqsMap[obj]++;
    }


    if(hit){
        auto itkey = key_pos.find(req.id);
        assert(itkey != key_pos.end());
        auto keyList = itkey->second;
        assert(keyList.list_idx==0);
        auto meta = in_cache_metas[keyList.list_pos];

        if(start_collect_train_data){
            uint32_t _distance = current_seq - meta->_past_timestamp;
            if(_distance > memory_window){
                _distance = 2 *memory_window;
            }
            training_data->emplace_back(meta, _distance, meta->_key);
            // if(is_use_kl){
                auto knIter = key_nums.find(meta->_key);
                if(knIter!=key_nums.end()){
                    knIter->second = meta->metaSize();
                }else{
                    key_nums.insert({meta->_key, meta->metaSize()});
                }
            // }

            if (training_data->labels.size() >= batch_size) {
                //
                train();
                training_data->clear();
            }
        }
        meta->update(current_seq, _reqsMap[obj]);
    }

    return hit;
}


void LBSCCache::update_stat_periodic(){
    segment_n_retrain.emplace_back(n_retrain);
    segment_n_out.emplace_back(out_cache_metas.size());

    double interval_time = chrono::duration_cast<chrono::milliseconds>(chrono::system_clock::now() - interval_timeBegin).count();

    cerr
            << " / " << out_cache_metas.size() << endl
            << "memory_window: " << memory_window << endl
            << "n_training: " << training_data->labels.size() << endl
            << "training_time: " << training_time/1000 << " ms" << endl
            << "inference_time: " << inference_time/1000 << " ms" << endl
            << "inference_add_feature_time: " << inference_add_feature_time/1000 << " ms" << endl
            << "inference_sort_time: "<<inferenct_sort_time/1000 << " ms"<<endl
            << "max_size_cost_rate: "<<max_size_cost_rate<<endl
            << "min_size_cost_rate: "<<min_size_cost_rate<<endl
            << "evict_max_size_cost_rate: "<<evict_max_size_cost_rate<<endl
            << "evict_min_size_cost_rate: "<<evict_min_size_cost_rate<<endl 
            << "evict_max_last_distance: "<<evict_max_last_distance<<endl
            << "evict_min_last_distance: "<<evict_min_last_distance<<endl
            << "min_cost_size_ratio: "<<min_cost_size_ratio<<endl
            << "max_cost_size_ratio: "<<max_cost_size_ratio<<endl
            << "_gdsfInferenceValueMapMaxSize: "<<_gdsfInferenceValueMapMaxSize<<endl
            << "eviction_num: "<<eviction_num<<endl
            << "bypass_num: "<< bypass_num<<endl
            << "sample_num: "<<sample_num<<endl
            << "n_inference:" << n_inference << endl
            << "n_retrain: " << n_retrain << endl
            << "interval time: "<<interval_time<<" ms"<<endl
            << "min_sample_rate: "<<min_sample_rate << "  max_sample_rate: "<<max_sample_rate<<endl
            << "min_kl_threshold:"<<min_kl_threshold<<"    max_kl_threshold:"<<max_kl_threshold<<endl 
            << "min_booster_size:"<<min_vec_kl_size<<"    max_boosters_size:"<<max_vec_kl_size<<endl
            << "current cost_size_threshol: "<<cost_size_threshol<<endl
            << "current low/high ratio:  "<<in_cache_metas_threshold_low.size()/(double)in_cache_metas.size()<<endl
            << "start_sample:   "<<start_sample<<endl;
    cerr <<"cache items:"<<in_cache_metas.size()<<endl;
    cerr << "training_loss are: ";
    for(int i=0;i<training_losses.size();i++){
        cerr<<training_losses[i]<<" ";
    }
    cerr <<endl << "training data size: ";
    for(int i=0;i<training_data_size.size();i++){
        cerr<<training_data_size[i]<<" ";
    }
    cerr<<endl << "caches data size: ";
    for(int i=0;i<caches_data_size.size();i++){
        cerr<<caches_data_size[i]<<" ";
    }
    cerr<<endl << "outlier num size: ";
    for(int i=0;i<outlier_num_size.size();i++){
        cerr<<outlier_num_size[i]<<" ";
    }
    cerr<<endl;
    caches_data_size.clear();
    training_data_size.clear();
    training_losses.clear();
    outlier_num_size.clear();
    bypass_num = 0;
    eviction_num = 0;
    _gdsfInferenceValueMapMaxSize = 0;
    sample_num = 0;
    // opt_inference_num = 0;
    min_kl_threshold=-1.0;
    max_kl_threshold=-1.0;
    min_vec_kl_size=0;
    max_vec_kl_size=0;

    min_sample_rate = sample_rate;
    max_sample_rate = 0;

    max_size_cost_rate = -1.0;
    min_size_cost_rate = -1.0;

    evict_max_size_cost_rate = -1.0;
    evict_min_size_cost_rate = -1.0;

    min_cost_size_ratio = -1.0;
    max_cost_size_ratio = -1.0;

    evict_max_last_distance = 0;
    evict_min_last_distance = memory_window;

    n_retrain = 0;
    training_time  = 0;
    inference_time = 0;
    inference_add_feature_time = 0;
    inferenct_sort_time = 0;
    n_inference = 0;

    cerr<<"feature importance:"<<endl;
    int res;
    auto importances = vector<double>(n_feature, 0);

    if (booster) {
        res = LGBM_BoosterFeatureImportance(booster,0,1,importances.data());
        if (res == -1) {
            cerr << "error: get model importance fail" << endl;
            abort();
        }
    }
    for(int i=0;i<importances.size();i++){
        cerr<<importances[i]<<" ";
    }
    cerr<<endl;


    interval_timeBegin = chrono::system_clock::now();
}