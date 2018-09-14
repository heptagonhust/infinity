#include "org_apache_hadoop_hbase_ipc_RdmaNative.h"
#include "org_apache_hadoop_hbase_ipc_RdmaNative_RdmaConnection.h"

#include <infinity/core/Context.h>
#include <infinity/queues/QueuePairFactory.h>
#include <infinity/queues/QueuePair.h>
#include <infinity/memory/Buffer.h>
#include <infinity/memory/RegionToken.h>
#include <infinity/requests/RequestToken.h>
using namespace infinity;

#include <iostream>

core::Context *context;
queues::QueuePairFactory *qpFactory = new infinity::queues::QueuePairFactory(context);

#ifdef RDEBUG
#define rdma_debug std::cerr << "RdmaNative Debug: "
#else
#define rdma_debug if(false) std::cerr
#endif
#define rdma_error std::cerr << "RdmaNative: "

class CRdmaServerConnectionInfo {
private:
    //               < msgSize, DyBufTokenBufToken >
    typedef std::pair<uint32_t, memory::RegionToken> magicAndTokenType;
    queues::QueuePair *pQP = nullptr; // must delete

    memory::Buffer *pDynamicBufferTokenBuffer = nullptr; // must delete
    memory::RegionToken *pDynamicBufferTokenBufferToken = nullptr; // must delete
    magicAndTokenType magicAndToken;

    memory::Buffer *pDynamicBuffer = nullptr; // must delete
    memory::RegionToken *pDynamicBufferToken = nullptr; // must delete
    long currentSize = 4096; // Default 4K

    void initFixedLocalBuffer() {
        pDynamicBuffer = new memory::Buffer(context, currentSize);
        pDynamicBufferToken = pDynamicBuffer->createRegionToken();
        pDynamicBufferTokenBuffer = new memory::Buffer(context, sizeof(memory::RegionToken));
        pDynamicBufferTokenBufferToken = pDynamicBufferTokenBuffer->createRegionToken();
        magicAndToken.first = 0x00000000;
        magicAndToken.second = *pDynamicBufferTokenBufferToken;
    }
public:
//    CRdmaServerConnectionInfo() : pDynamicBuffer(nullptr), pDynamicBufferToken(nullptr),
//        pDynamicBufferTokenBuffer(nullptr), pDynamicBufferTokenBufferToken(nullptr),
//        currentSize(4096), pQP(nullptr) {
//
//        }
    ~CRdmaConnectionInfo() {
        if(pQP) delete pQP;
        if(pDynamicBuffer) delete pDynamicBuffer;
        if(pDynamicBufferToken) delete pDynamicBufferToken;
        if(pDynamicBufferTokenBuffer) delete pDynamicBufferTokenBuffer;
        if(pDynamicBufferTokenBufferToken) delete pDynamicBufferTokenBufferToken;
    }

    void writeResponse(const void *dataPtr, long dataSize) {
        if(magicAndToken.first != 0xaaaaaaaa)
            throw std::runtime_error(std::string("write response: wrong magic. Want 0xaaaaaaaa, got ") + std::to_string(magicAndToken.first));
        pDynamicBuffer->resize(dataSize);
        // TODO: here's an extra copy. use dataPtr directly and jni global reference to avoid it!
        std::memcpy(pDynamicBuffer->getData(), dataPtr, dataSize); 
        if(0xaaaaaaaa != std::exchange(magicAndToken.first, 0x55555555)) // not atomic
            throw std::runtime_error("write response: magic is changed while copying memory data.");
    }

    void connectToRemote(const char *serverAddr, int serverPort) {
        // Factory method
        pQP = qpFactory->connectToRemoteHost(serverAddr, serverPort, &magicAndToken, sizeof(magicAndToken));
        magicAndTokenType qpRemoteUserData (*reinterpret_cast<magicAndTokenType *>(pQP->getUserData()));
        pRemoteRegionTokenBufferToken = &qpRemoteUserData.first;
        pRemoteDynamicRegionSizeBufferToken = &qpRemoteUserData.second;
        rdma_debug << "remote token1 is " << pRemoteRegionTokenBufferToken->getAddress() << ",local token1 is " << pLocalRegionTokenBufferToken->getAddress() << std::endl;

        // Create and register first local buffer.
        // Create another local buffer to indicate local buffer size, sothat remote know how many bytes to read.
    }

    void waitAndAccept() {
        pQP = qpFactory->acceptIncomingConnection(&magicAndToken, sizeof(magicAndToken));
        magicAndTokenType qpRemoteUserData (*reinterpret_cast<magicAndTokenType *>(pQP->getUserData()));
        pRemoteRegionTokenBufferToken = &qpRemoteUserData.first;
        pRemoteDynamicRegionSizeBufferToken = &qpRemoteUserData.second;
        rdma_debug << "remote token1 is " << pRemoteRegionTokenBufferToken->getAddress() << ",local token1 is " << pLocalRegionTokenBufferToken->getAddress() << std::endl;
        
    }
};

/*
 * Class:     org_apache_hadoop_hbase_ipc_RdmaNative
 * Method:    rdmaInitGlobal
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_hbase_ipc_RdmaNative_rdmaInitGlobal(JNIEnv *, jobject) {
    try {
        context = new core::Context();
        qpFactory = new queues::QueuePairFactory(context);
    }
    catch (std::exception &ex) {
        rdma_error << "Exception: " << ex.what() << std::endl;
        return 1;
    }
    return 0;
}

/*
 * Class:     org_apache_hadoop_hbase_ipc_RdmaNative
 * Method:    rdmaDestroyGlobal
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_hbase_ipc_RdmaNative_rdmaDestroyGlobal(JNIEnv *, jobject) {
    delete context;
    delete qpFactory;
    return 0;
}

/*
 * Class:     org_apache_hadoop_hbase_ipc_RdmaNative
 * Method:    rdmaConnect
 * Signature: (Ljava/lang/String;I)Lorg/apache/hadoop/hbase/ipc/RdmaNative/RdmaConnection;
 */
JNIEXPORT jobject JNICALL Java_org_apache_hadoop_hbase_ipc_RdmaNative_rdmaConnect(JNIEnv *env, jobject, jstring jServerAddr, jint jServerPort) {

#define REPORT_FATAL(msg) do { \
    std::cerr << "RdmaNative FATAL: " << msg << "Unable to pass error to Java. Have to abort..." << std::endl; \
    abort(); \
} while(0)

    // Deal with java...
    static const jclass jConnCls = env->FindClass("org/apache/hadoop/hbase/ipc/RdmaNative/RdmaConnection");
    if(jConnCls == NULL) REPORT_FATAL("Unable to find class org/apache/hadoop/hbase/ipc/RdmaNative/RdmaConnection.");
    jmethodID jConnClsInit = env->GetMethodID(jConnCls, "<init>", "()V"); // -> problem!
    if (jConnClsInit == NULL) REPORT_FATAL("Unable to find constructor org/apache/hadoop/hbase/ipc/RdmaNative/RdmaConnection::<init> -> ()V.");
    jobject jConn = env->NewObject(jConnCls, jConnClsInit);
    if(jConn == NULL) REPORT_FATAL("Unable to create RdmaConnection object.");
    jfieldID jErrCode = env->GetFieldID(jConnCls, "errorCode", "I");
    if(jErrCode == NULL) REPORT_FATAL("Unable to getFieldId `errorCode` of class RdmaConnection.");

#define REPORT_ERROR(code, msg) do { \
    env->SetIntField(jConn, jErrCode, code); \
    std::cerr << "RdmaNative ERROR: " << msg << "Returning error code to java..." << std::endl; \
} while(0)

    // Deal with rdma logic...
    jboolean isCopy;
    const char *serverAddr = env->GetStringUTFChars(jServerAddr, &isCopy);
    if(serverAddr == NULL) REPORT_ERROR(3, "GetStringUTFChars from jServerAddr error.");

    // do connect
    try {
        CRdmaConnectionInfo *conn = new CRdmaConnectionInfo();
        conn.connectToRemote(serverAddr, jServerPort);

    }



    // cleanup
    if (isCopy == JNI_TRUE) {
        env->ReleaseStringUTFChars(jServerAddr, serverAddr);
    }


}



