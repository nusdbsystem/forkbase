FROM ubuntu:16.04

# install dependencies
RUN apt-get update && apt-get install -y --no-install-recommends psmisc

# import ustore executable
COPY . /ustore

# set environment variables
ENV USTORE_HOME=/ustore/build
ENV PATH=$USTORE_HOME/bin:$PATH
ENV LD_LIBRARY_PATH=$USTORE_HOME/lib:$LD_LIBRARY_PATH
