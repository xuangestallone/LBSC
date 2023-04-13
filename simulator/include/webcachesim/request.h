#ifndef REQUEST_H
#define REQUEST_H

#include <cstdint>
#include <iostream>
#include <vector>

using namespace std;

// Request information
class SimpleRequest {
public:
    SimpleRequest() = default;

    virtual ~SimpleRequest() = default;

    SimpleRequest(
            const int64_t &_id,
            const int64_t &_size) {
        reinit(0, _id, _size);
    }

    SimpleRequest(const int64_t &_seq, const int64_t &_id, const int64_t &_size, const vector<uint16_t> *_extra_features = nullptr) {
        reinit(_seq, _id, _size, _extra_features);
    }

    SimpleRequest(const int64_t &_seq, const int64_t &_id, const int64_t &_size, const int64_t &_cost, const vector<uint16_t> *_extra_features = nullptr) {
        reinit(_seq, _id, _size, _cost,  _extra_features);
    }

    void reinit(const int64_t &_seq, const int64_t &_id, const int64_t &_size, const vector<uint16_t> *_extra_features = nullptr) {
        seq = _seq;
        id = _id;
        size = _size;
        if (_extra_features) {
            extra_features = *_extra_features;
        }
        cost=0;
    }

    void reinit(const int64_t &_seq, const int64_t &_id, const int64_t &_size, const int64_t &_cost, const vector<uint16_t> *_extra_features = nullptr) {
        seq = _seq;
        id = _id;
        size = _size;
        if (_extra_features) {
            extra_features = *_extra_features;
        }
        cost=_cost;
    }

    uint64_t seq{};
    int64_t id{}; // request object id
    int64_t size{}; // request size in bytes
    int64_t cost{};
    //category feature. unsigned int. ideally not exceed 2k
    vector<uint16_t > extra_features;
};


class AnnotatedRequest : public SimpleRequest {
public:
    AnnotatedRequest() = default;

    // Create request
    AnnotatedRequest(const int64_t &_seq, const int64_t &_id, const int64_t &_size, const int64_t &_next_seq,
                     const vector<uint16_t> *_extra_features = nullptr)
            : SimpleRequest(_seq, _id, _size, _extra_features),
              next_seq(_next_seq){}
    
    AnnotatedRequest(const int64_t &_seq, const int64_t &_id, const int64_t &_size, const int64_t &_next_seq, const int64_t &_cost,
                     const vector<uint16_t> *_extra_features = nullptr)
            : SimpleRequest(_seq, _id, _size, _cost, _extra_features),
              next_seq(_next_seq){}

    void reinit(const int64_t &_seq, const int64_t &_id, const int64_t &_size, const int64_t &_next_seq,
                       const vector<uint16_t> *_extra_features = nullptr) {
        SimpleRequest::reinit(_seq, _id, _size, _extra_features);
        next_seq = _next_seq;
        //cost=0;
    }

    void reinit(const int64_t &_seq, const int64_t &_id, const int64_t &_size, const int64_t &_next_seq, const int64_t &_cost,
                       const vector<uint16_t> *_extra_features = nullptr) {
        SimpleRequest::reinit(_seq, _id, _size, _cost, _extra_features);
        next_seq = _next_seq;
        //cost=_cost;
    }

    int64_t next_seq{};
    //int64_t cost{};
};


class AnnotatedCostRequest : public SimpleRequest {
public:
    AnnotatedCostRequest() = default;

    // Create request
    AnnotatedCostRequest(const int64_t &_seq, const int64_t &_id, const int64_t &_size, const vector<int64_t> &_next_seq,
                     const vector<uint16_t> *_extra_features = nullptr)
            : SimpleRequest(_seq, _id, _size, _extra_features){
                next_seq.resize(_next_seq.size());
                for(int i=0;i<_next_seq.size();i++){
                    next_seq[i] = _next_seq[i];
                }
            }
              //next_seq(_next_seq){}
    
    AnnotatedCostRequest(const int64_t &_seq, const int64_t &_id, const int64_t &_size, const vector<int64_t> &_next_seq, const int64_t &_cost,
                     const vector<uint16_t> *_extra_features = nullptr)
            : SimpleRequest(_seq, _id, _size, _cost, _extra_features){
                next_seq.resize(_next_seq.size());
                for(int i=0;i<_next_seq.size();i++){
                    next_seq[i] = _next_seq[i];
                }
            }
             // next_seq(_next_seq){}

    void reinit(const int64_t &_seq, const int64_t &_id, const int64_t &_size, const vector<int64_t> &_next_seq,
                       const vector<uint16_t> *_extra_features = nullptr) {
        SimpleRequest::reinit(_seq, _id, _size, _extra_features);
        next_seq.resize(_next_seq.size());
        for(int i=0;i<_next_seq.size();i++){
            next_seq[i] = _next_seq[i];
        }
        //next_seq = _next_seq;
        //cost=0;
    }

    void reinit(const int64_t &_seq, const int64_t &_id, const int64_t &_size, const vector<int64_t> &_next_seq, const int64_t &_cost,
                       const vector<uint16_t> *_extra_features = nullptr) {
        SimpleRequest::reinit(_seq, _id, _size, _cost, _extra_features);
        next_seq.resize(_next_seq.size());
        for(int i=0;i<_next_seq.size();i++){
            next_seq[i] = _next_seq[i];
        }
        //next_seq = _next_seq;
        //cost=_cost;
    }

    vector<int64_t> next_seq;
    //int64_t cost{};
};

#endif /* REQUEST_H */



