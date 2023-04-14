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
The two CDN datasets are download by http://lrb.cs.princeton.edu/wiki2019.tr.tar.gz  and  http://lrb.cs.princeton.edu/wiki2018.tr.tar.gz.
```

## Quick Start
### LBSC in synthetic dataset: 
- Generating cost for the 
```
python multi-party-real/feature_selection_passive_selectall.py
python multi-party-real/feature_selection_active_selectall.py
```

### LBSC in FPDB: 
In this scenario, The users can simulate the multi-party feature selection process by modifying the profile.
Taking mimic dataset as an example, the users can run FEAST with the following command:
```
python multi-party-simulation/mimic/mimic_FEAST.py
```

