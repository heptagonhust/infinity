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
        cout << "query:" << (char *)dataPtr << ", with length " << size << endl;
        responseData = "fuckFirstPkg";
        conn.writeResponse(responseData.data(), responseData.size());
        cout << "Sleeping 5 seconds to wait for the client reading response..." << endl;
        sleep(5); // the client is still reading thr response!

        cout << "---- Test the second round! ----" << endl;

        while(!conn.isQueryReadable());
        conn.readQuery(dataPtr, size);
        cout << "query:" << (char *)dataPtr << ", with length " << size << endl;
        responseData = "FFFFFFFFFFFFFFucking!!!";
        conn.writeResponse(responseData.data(), responseData.size());
        cout << "Sleeping 5 seconds to wait for the client reading response..." << endl;
        sleep(5); // the client is still reading thr response!

        cout << "---- Test the third round! ----" << endl;
        CRdmaServerConnectionInfo anotherConn;
        anotherConn.waitAndAccept();

        while(!anotherConn.isQueryReadable());
        anotherConn.readQuery(dataPtr, size);
        cout << "query:" << (char *)dataPtr << ", with length " << size << endl;
        responseData = "Ml with you the 3rd time.";
        anotherConn.writeResponse(responseData.data(), responseData.size());
        cout << "Sleeping 5 seconds to wait for the client reading response..." << endl;
        sleep(5); // the client is still reading thr response!
    }
    else {
        cout << "client mode" << endl;
        serverName = argv[1];
        CRdmaClientConnectionInfo conn;
        string queryData;
        infinity::memory::Buffer *bufPtr;

        _again:
        try {
            conn.connectToRemote(serverName.c_str(), serverPort);
        }
        catch(std::exception &e) {sleep(1);goto _again;}

        queryData = "hello";
        conn.writeQuery((void *)queryData.data(), queryData.size());
        while(!conn.isResponseReady());
        conn.readResponse(bufPtr);
        cout << "response:" << (char *)bufPtr->getData() << ", with length " << bufPtr->getSizeInBytes() << endl;

        cout << "---- Test the second round! ----" << endl;

        queryData = "hello again";
        conn.writeQuery((void *)queryData.data(), queryData.size());
        while(!conn.isResponseReady()) sleep(1);
        conn.readResponse(bufPtr);
        cout << "response:" << (char *)bufPtr->getData() << ", with length " << bufPtr->getSizeInBytes() << endl;

        cout << "---- Test the third round! ----" << endl;
        CRdmaClientConnectionInfo anotherConn;
        _again2:
        try {
            anotherConn.connectToRemote(serverName.c_str(), serverPort);
        }
        catch(std::exception &e) {sleep(1);goto _again2;}

        queryData = "hello third ~";
        anotherConn.writeQuery((void *)queryData.data(), queryData.size());
        while(!anotherConn.isResponseReady()) sleep(1);
        anotherConn.readResponse(bufPtr);
        cout << "response:" << (char *)bufPtr->getData() << ", with length " << bufPtr->getSizeInBytes() << endl;
    }
    Java_org_apache_hadoop_hbase_ipc_RdmaNative_rdmaDestroyGlobal(NULL, NULL);
}
