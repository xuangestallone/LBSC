* Recommend OS: Ubuntu 18
* Install webcachesim library
```bash
git clone --recurse-submodules https://github.com/sunnyszy/lrb webcachesim
cd webcachesim
./scripts/install.sh
# test
./build/webcachesim_cli
#output: webcachesim_cli traceFile cacheType cacheSize [--param=value]
```
* Set environment variables. Edit `~/.bashrc`:
```bash
# add binary and trace dir to path
export PATH=$PATH:${YOUR webcachesim DIR}/build/bin
export WEBCACHESIM_TRACE_DIR=${YOUR TRACE DIR}
export WEBCACHESIM_ROOT=${YOUR webcachesim DIR}
```
* [Optional] Set up a mongodb instance. LRB uses this to store tuning results.
* [Optional] pywebcachesim is a python wrapper to run multiple simulation in parallel on multiple nodes.
```shell script
cd python-package
# Install pywebcachesim package
pip3 install -e .
python3 pywebcachesim/simulate.py ${YOUR JOB CONFIG FILE} ${YOUR ALGORITHM PARAM FILE} ${YOUR TRACE PARAM FILE} ${MONGODB URI}
cd ..
```
