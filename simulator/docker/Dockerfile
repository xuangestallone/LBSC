# cd docker
# docker build -t webcachesim .
# docker images
# docker run -it -v ${YOUR TRACE DIRECTORY}:/trace webcachesim wiki2018.tr LRU 1099511627776
# docker tag webcachesim sunnyszy/webcachesim:latest
# docker login
# docker push sunnyszy/webcachesim
FROM ubuntu:18.04

ENV WEBCACHESIM_ROOT=/opt/webcachesim
WORKDIR ${WEBCACHESIM_ROOT}

RUN apt-get update && apt-get install -y git sudo && \
					  git clone --recurse-submodules https://github.com/sunnyszy/lrb . && \
                      bash ./scripts/install.sh

ENV WEBCACHESIM_TRACE_DIR=/trace
ENTRYPOINT ["/opt/webcachesim/build/bin/webcachesim_cli"]
