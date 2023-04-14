# LBSC

## Introduction
LBSC a cost-aware approach for caching that uses machine learning with low computation overheads. To accomplish this work, we propose an oracle algorithm for LBSC to mimic, which is optimal under some assumptions with provable theoretical guarantees. To make the ML model more lightweight, we put forward some optimizations, such as efficiently sampling cached data for model inference, discarding outlier after training data generation, and efficiently re-training.

Our evaluation is two-fold. 
- In the first part, we evaluate the performance of LBSC on synthetic datasets based on real-world workloads, this part is implemented based on the cache framework webcachesim~(https://github.com/sunnyszy/lrb). The main code is in folder simulator.
- In the second part, we demonstrate that it is workable in real cloud analytical databases~(i.e., FPDB). This part is implemented based on the cloud databases prototype FlexPushdownDB~(https://github.com/cloud-olap/FlexPushdownDB). The main code is in folder FPDB.


## Dependencies and Build
```
See simulator/README.md  and FPDB/README.md.
```

## Dataset
```
The two CDN datasets are shown in simulator/README.md.
The datasets used in FPDB is the same as the original paper, which can be found in FPDB/README.md.
```

## Quick Start
### LBSC in synthetic dataset: 
- Generating cost for the real-world dataset. caching algorithm(LRU) and cache size(4294967296) are arbitrary. The last three parameters are used to adjust whether transfer cost dominates or computation cost dominates.
```
webcachesim_cli xxx LRU 4294967296 --delta_ratio=xx --fixed_byte=xx --min_ratio=xx 
```

- Using optimization of efficient sampling.
```
webcachesim_cli xxx LBSC 4294967296 --is_cost_size_sample=1 --cost_size_threshol=xx   #static splitting method
webcachesim_cli xxx LBSC 4294967296 --is_cost_size_sample=1 --cost_size_threshol=xx --is_dynamic_cost_size_threshold=1  #dynamic splitting method
```

- Using optimization of outlier detection.
```
webcachesim_cli xxx LBSC 4294967296 --is_optimize_train=1
```

- Using optimization of efficient re-training.
```
webcachesim_cli xxx LBSC 4294967296 --kl_threshold=xx --is_use_kl=1 --kl_sample_num=xx
```

### LBSC in FPDB: 
We integrate LBSC with FPDB to evaluate the performance in cloud databases. The basic run command is same as FPDB/README.md. We take an example as following.
ssb-sf10-sortlineorder/csv/ is the metadata folder of the original data.

```
./normal-ssb-experiment -d ssb-sf10-sortlineorder/csv/ <cache_size> <mode> <caching_policy> <execution_batch_size> 0 <weight_method> 0 0 1 2
```

- caching_policy: cache replacement policy used in the segment cache. add two new algorithms:
```
5 - BeladySizeCost
6 - LBSC
```
- execution_batch_size: we can define the number of execution queries(default number of warm-up queries is 50).
- weight_method:
```
1 - Cumulative cost.
2 - Decayed cost.
3 - Average cost.
```

