#!/bin/bash
export CPATH=$CPATH:$JAVA_HOME/include:/home/rdma_match/java_rdma/src/infinity/src/

cd /home/rdma_match/java_rdma/src


gcc -o rdmaclientlib.so -fPIC -shared -I$JAVA_HOME/include \
-I/home/rdma_match/java_rdma/src/infinity/src/ -I$JAVA_HOME/include/linux \
./infinity/java-wrapper/java_wrapper_client.cpp -std=c++0x

 gcc -o rdmaserverlib.so -fPIC -shared -I$JAVA_HOME/include \
-I/home/rdma_match/java_rdma/src/infinity/src/ -I$JAVA_HOME/include/linux \
./infinity/java-wrapper/java_wrapper_server.cpp -std=c++0x


