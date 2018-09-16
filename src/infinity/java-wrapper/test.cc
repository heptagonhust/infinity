#include "RdmaImpl.hpp"
#include <iostream>
#include <string>
#include <unistd.h>
#include "org_apache_hadoop_hbase_ipc_RdmaNative.h"

using namespace std;
int main(int argc, char **argv) {
    bool isServer = true;
    int serverPort = 25543;
    string serverName;
    if(argc == 1) {
        cout << "server mode" << endl;
        Java_org_apache_hadoop_hbase_ipc_RdmaNative_rdmaInitGlobal(NULL, NULL);
        Java_org_apache_hadoop_hbase_ipc_RdmaNative_rdmaBind(NULL, NULL, serverPort);
        CRdmaServerConnectionInfo conn;
        void *dataPtr;
        uint64_t size;

        conn.waitAndAccept();
        while(!conn.isQueryReadable());
        conn.readQuery(dataPtr, size);
        cout << "query:" << (char *)dataPtr << endl;
        string responseData = "fuck";
        conn.writeResponse(responseData.data(), responseData.size());
        cout << "Sleeping 5 seconds to wait for the client reading response..." << endl;
        sleep(5); // the client is still reading thr response!

        cout << "---- Test the second round! ----" << endl;

        conn.waitAndAccept();
        while(!conn.isQueryReadable());
        conn.readQuery(dataPtr, size);
        cout << "query:" << (char *)dataPtr << endl;
        string responseData = "FFFFFFFFFFFFFFucking!!!";
        conn.writeResponse(responseData.data(), responseData.size());
        cout << "Sleeping 5 seconds to wait for the client reading response..." << endl;
 
        Java_org_apache_hadoop_hbase_ipc_RdmaNative_rdmaDestroyGlobal(NULL, NULL);
    }
    else {
        cout << "client mode" << endl;
        serverName = argv[1];
        Java_org_apache_hadoop_hbase_ipc_RdmaNative_rdmaInitGlobal(NULL, NULL);
        CRdmaClientConnectionInfo conn;
        conn.connectToRemote(serverName.c_str(), serverPort);
        string queryData = "hello";
        conn.writeQuery((void *)queryData.data(), queryData.size());
        while(!conn.isResponseReady());
        infinity::memory::Buffer *bufPtr;
        conn.readResponse(bufPtr);
        cout << "response:" << (char *)bufPtr->getData() << endl;

        cout << "---- Test the second round! ----" << endl;

        
        Java_org_apache_hadoop_hbase_ipc_RdmaNative_rdmaDestroyGlobal(NULL, NULL);
    }
}
