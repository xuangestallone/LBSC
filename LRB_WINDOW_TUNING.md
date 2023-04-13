# LRB Window Tuning

LRB memory window is tuned by using the first 20% of the trace. 
The tuning code runs parallel simulations at the target cache size, and potentially at smaller cache sizes in order to do a linear fit.
For more detail, please check Section 4.1 in [our paper](https://www.usenix.org/system/files/nsdi20-paper-song.pdf).

Below are the instructions to tune LRB memory window on wiki 2018 trace with 64/128/256/512/1024 GB cache sizes.

* Follow the [instructions](INSTALL.md) to install LRB, set up a mongodb instance, and install pywebcachesim. The LRB c++ simulator is used to simulate each memory window value. Mongodb is used to store tuning results. The pywebcachesim is used to control the high level tuning process.
* To get help of the pywebcachesim tuning script, run `python  -m pywebcachesim.lrb_window_search --help`.
* Set up the trace and machine to run. See [job_dev.yaml](config/job_dev.yaml) as an example.
* Set up the cache sizes to run. See [trace_params.yaml](config/trace_params.yaml) as an example
* Run the tuning script. Below shows the sample output and comments.
```shell script
$python3 -m pywebcachesim.lrb_window_search ${YOUR JOB CONFIG FILE} ${YOUR ALGORITHM PARAM FILE} ${YOUR TRACE PARAM FILE} ${MONGODB URI}

# Since the goal is to simulate different memory windows, the window value in the config files will be ignored
... | INFO     | ... wiki2018.tr LRB ignore config: memory_window=10000000
# Window candidates are selected evenly in log space 
... | INFO     | ... wiki2018.tr/size=68719476736/LRB, memory windows to validate: [1950358, 2675819, 3671123, 5036643, 6910086, 9480379, 13006724, 17844737, 24482310, 33588811, 46082588, 63223581, 86740381, 119004547, 163269772, 224000000]
...
# Simulations start
parallel -v --eta --shuf --sshdelay 0.1 ...
...
# Output selected windows
... | INFO     | ... find best memory window for wiki2018.tr cache size 68719476736: ...
```

