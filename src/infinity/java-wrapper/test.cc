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
    Java_org_apache_hadoop_hbase_ipc_RdmaNative_rdmaInitGlobal(NULL, NULL);
    if(argc == 1) {
        cout << "server mode" << endl;
        Java_org_apache_hadoop_hbase_ipc_RdmaNative_rdmaBind(NULL, NULL, serverPort);
        CRdmaServerConnectionInfo conn;
        void *dataPtr;
        uint64_t size;
        string responseData;
        conn.waitAndAccept();

        while(!conn.isQueryReadable());
        conn.readQuery(dataPtr, size);
        cout << "query:" << (char *)dataPtr << endl;
        responseData = "fuckFirstPkg";
        conn.writeResponse(responseData.data(), responseData.size());
        cout << "Sleeping 5 seconds to wait for the client reading response..." << endl;
        sleep(5); // the client is still reading thr response!

        cout << "---- Test the second round! ----" << endl;

        while(!conn.isQueryReadable());
        conn.readQuery(dataPtr, size);
        cout << "query:" << (char *)dataPtr << endl;
        responseData = "FFFFFFFFFFFFFFucking!!!";
        conn.writeResponse(responseData.data(), responseData.size());
        cout << "Sleeping 5 seconds to wait for the client reading response..." << endl;
        sleep(5); // the client is still reading thr response!
 
    }
    else {
        cout << "client mode" << endl;
        serverName = argv[1];
        CRdmaClientConnectionInfo conn;
        string queryData;
        infinity::memory::Buffer *bufPtr;
        while(true) {
            try {
                conn.connectToRemote(serverName.c_str(), serverPort);
            }
            catch(...) {sleep(1);}
        }
        cout << "connected" << endl;
        queryData = "hello";
        conn.writeQuery((void *)queryData.data(), queryData.size());
        while(!conn.isResponseReady());
        conn.readResponse(bufPtr);
        cout << "response:" << (char *)bufPtr->getData() << endl;

        cout << "---- Test the second round! ----" << endl;

        queryData = "hello again";
        conn.writeQuery((void *)queryData.data(), queryData.size());
        while(!conn.isResponseReady()) sleep(1);
        conn.readResponse(bufPtr);
        cout << "response:" << (char *)bufPtr->getData() << endl;
    }
    Java_org_apache_hadoop_hbase_ipc_RdmaNative_rdmaDestroyGlobal(NULL, NULL);
}
